package com.devpilot.gateway.service;

import com.devpilot.gateway.config.GatewayProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 内核 HTTP 客户端 —— 网关与内核之间**唯一**的通道。
 *
 * <h2>为什么要把这一层单独抽出来</h2>
 *
 * 因为它是"边界"。边界两侧的东西不该互相知道：
 * <ul>
 *   <li>内核不该知道网关的任务表长什么样；</li>
 *   <li>网关的业务代码不该知道内核的 JSON 字段叫什么。</li>
 * </ul>
 * 把 WebClient 散在 TaskDispatcher、Reaper、Controller 里各写一遍，
 * 后果是"内核改一个字段名要改三处"，而漏改的那处会在运行时报 NPE ——
 * 那种错误不会出现在编译期，也没人会专门为它写测试。
 *
 * <p>所以：**所有对内核的调用都在这个类里，所有内核返回的 JSON 都在这里被
 * 翻译成 record。** 外面拿到的是类型，不是 Map。
 *
 * <h2>为什么用 WebClient 而不是 RestClient</h2>
 *
 * 因为其中 {@link #events} 是个**长连接流**：一次交付要跑几分钟，
 * 期间事件是持续推过来的。用同步阻塞客户端接流，就得自己管线程和缓冲；
 * WebClient 的 {@code Flux} 天然就是这个形态，且等待期间不占线程。
 */
@Component
public class KernelClient {

    private static final Logger log = LoggerFactory.getLogger(KernelClient.class);

    /** 内核的内网凭证头。名字必须与内核 server.py 里的 Header 参数一致。 */
    public static final String TOKEN_HEADER = "X-Internal-Token";

    private final GatewayProperties props;
    private final ObjectMapper json = new ObjectMapper();
    private final WebClient http;

    public KernelClient(GatewayProperties props) {
        this.props = props;
        this.http = WebClient.builder()
                .baseUrl(props.getKernel().getBaseUrl())
                .defaultHeader(TOKEN_HEADER, props.getKernel().getToken())
                // 默认缓冲上限会卡住长响应；SSE 本身是流式的，
                // 这里放宽到 1MB，防止某条事件带了大 payload 时整条流被掐断。
                .codecs(c -> c.defaultCodecs().maxInMemorySize(1024 * 1024))
                .build();
    }

    // ============================================================
    // 返回类型：把 JSON 翻译成 record，外部只看到类型
    // ============================================================

    /**
     * 内核受理任务后的回执。
     *
     * <p>{@code deduplicated} 很重要：内核自己也做幂等。
     * 如果网关重试了一次提交（比如网络超时后重发），内核可能返回**上一次创建的任务**。
     * 不知道这一点的话，网关会把 attempts 记成 2，而实际上只执行了一次 ——
     * 重派上限就会被虚耗掉。
     */
    public record KernelTask(String taskId, String status, boolean deduplicated) {}

    /** 一条内核事件。{@code data} 保持 JsonNode，因为不同事件类型的 payload 结构不同。 */
    public record KernelEvent(long seq, String type, JsonNode data, long ts) {}

    // ============================================================
    // 普通请求
    // ============================================================

    /** 提交任务。超时单独放宽 —— 内核可能正在冷启动，30 秒会把"还在启动"误判成"提交失败"。 */
    public Mono<KernelTask> submit(String requirement, Map<String, Object> options, String idemKey) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("requirement", requirement);
        body.put("options", options == null ? Map.of() : options);
        if (idemKey != null && !idemKey.isBlank()) {
            body.put("idempotency_key", idemKey);
        }
        return http.post().uri("/v1/tasks")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(body)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .timeout(Duration.ofSeconds(props.getKernel().getSubmitTimeoutSeconds()))
                .map(n -> new KernelTask(
                        n.path("task_id").asText(),
                        n.path("status").asText("pending"),
                        n.path("deduplicated").asBoolean(false)))
                .doOnError(e -> log.warn("提交内核失败：{}", e.toString()));
    }

    /** 查内核侧任务详情（检查点、沙箱文件、审批挂起标记都在这）。 */
    public Mono<JsonNode> getTask(String kernelTaskId) {
        return get("/v1/tasks/" + kernelTaskId);
    }

    public Mono<JsonNode> health() {
        return get("/health");
    }

    public Mono<JsonNode> pipeline() {
        return get("/v1/pipeline");
    }

    public Mono<JsonNode> artifacts(String kernelTaskId) {
        return get("/v1/tasks/" + kernelTaskId + "/artifacts");
    }

    public Mono<String> artifactRaw(String kernelTaskId, String path) {
        return http.get()
                .uri(b -> b.path("/v1/tasks/" + kernelTaskId + "/artifacts/raw")
                        .queryParam("path", path).build())
                .retrieve()
                .bodyToMono(String.class)
                .timeout(Duration.ofSeconds(props.getKernel().getTimeoutSeconds()));
    }

    public Mono<JsonNode> approve(String kernelTaskId, boolean approved, String comment) {
        return post("/v1/tasks/" + kernelTaskId + "/approve",
                Map.of("approved", approved, "comment", comment == null ? "" : comment));
    }

    public Mono<JsonNode> cancel(String kernelTaskId) {
        return post("/v1/tasks/" + kernelTaskId + "/cancel", Map.of());
    }

    public Mono<JsonNode> resume(String kernelTaskId, boolean autoApprove) {
        return post("/v1/tasks/" + kernelTaskId + "/resume",
                Map.of("auto_approve", autoApprove));
    }

    private Mono<JsonNode> get(String uri) {
        return http.get().uri(uri)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .timeout(Duration.ofSeconds(props.getKernel().getTimeoutSeconds()));
    }

    private Mono<JsonNode> post(String uri, Object body) {
        return http.post().uri(uri)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(body)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .timeout(Duration.ofSeconds(props.getKernel().getTimeoutSeconds()));
    }

    // ============================================================
    // 事件流
    // ============================================================

    /**
     * 订阅内核某个任务的事件流。
     *
     * <p>这里**只做协议解析**：把内核推来的事件变成 {@link KernelEvent}。
     * 至于"这条事件意味着任务变成了什么状态"，是业务判断，
     * 放在 {@link TaskDispatcher} 里 —— 客户端不该有业务观点。
     *
     * <h2>踩过的坑：SSE 的帧已经被 WebFlux 拆过一遍了</h2>
     *
     * 直觉上 {@code bodyToFlux(String.class)} 拿到的是**原始文本**，
     * 于是想当然地用"找 data: 行、拼接、再 JSON.parse"去解 ——
     * 结果是一条事件都收不到，而且不报错。
     *
     * <p>原因是 Spring 为 {@code text/event-stream} 注册了
     * {@code ServerSentEventHttpMessageReader}，而它对 {@code String} 目标类型
     * 也生效：它会**把 event: / data: 外壳剥掉**，只把 data 段交给你。
     * 也就是说，我们收到的是 {@code {"seq":1,...}}，而不是
     * {@code data: {"seq":1,...}} —— 再去按行找 "data:" 前缀，自然一条都匹配不上。
     *
     * <p>这种 bug 的可怕之处是**它不抛异常**：流的订阅成功、连接 200、
     * 只是永远没有数据。界面上表现为"任务一直在运行"，看起来完全合理。
     *
     * <p>修法是从宽到严地试：先当成"已经是 JSON"直接解；不行再退回去做
     * 手动的行解析（应对将来换成裸字节读取、或对端真的推原始帧的情况）。
     * 两条路都覆盖，就不会因为换个客户端实现而静默失效。
     */
    public Flux<KernelEvent> events(String kernelTaskId, long since) {
        return withReconnect(cursor -> rawEvents(kernelTaskId, cursor), since);
    }

    /**
     * 给任意一个"按下标订阅"的事件源套上断线重连 + 游标续传。
     *
     * <p>抽成独立方法有两个原因，都不是为了好看：
     * <ol>
     *   <li><b>可测</b>：重连是这条链路上最容易静默失效的一环，
     *       而 {@code KernelClient} 的其它部分都依赖真实 HTTP，
     *       很难在单测里制造"连上、收到两条、再断掉"这种时序。
     *       把重连逻辑与 HTTP 解耦，测试就能用一个完全可控的 Flux 去驱动它。</li>
     *   <li><b>边界清晰</b>：这个方法只关心"怎么重连、从哪续传"，
     *       不关心事件从哪来、长什么样。</li>
     * </ol>
     */
    Flux<KernelEvent> withReconnect(java.util.function.Function<Long, Flux<KernelEvent>> subscribe,
                                    long since) {
        // 游标：记住已经收到的最大 seq，重连时从这里续传。
        // 用 AtomicLong 而不是闭包变量，因为它要在多次重订阅之间共享。
        AtomicLong cursor = new AtomicLong(since);

        return Flux.defer(() -> subscribe.apply(cursor.get()))
                .doOnNext(ev -> cursor.accumulateAndGet(ev.seq(), Math::max))
                .retryWhen(reconnectPolicy())
                .doOnError(e -> log.warn("内核事件流彻底放弃：{}", e.toString()));
    }

    /**
     * 断线重连策略。
     *
     * <h2>为什么要重连，而不是"断了就算了"</h2>
     *
     * 这条流代表一次持续几分钟的交付过程，而中间会经历好几次
     * **几十秒完全没有事件**的静默期（模型在思考、沙箱在跑 pytest）。
     * 这些静默期正是各种中间设施最容易掐连接的时刻 ——
     * 负载均衡的空闲超时、操作系统的 TCP 保活、乃至对端进程的一次 GC 停顿。
     *
     * <p>不重连的后果是**静默死亡**：订阅进入终止态，回调被调用一次，
     * 然后这个任务的事件就再也进不来了。任务在网关这边的状态永远停在
     * 最后收到的那条事件上（比如一直显示"运行中"），
     * 而内核那边其实早就跑到了审批点、甚至已经完成。
     *
     * <h2>更隐蔽的一种写法</h2>
     *
     * 曾经在这里写过 {@code .timeout(30s, Flux.never())}，本意是"别无限等"。
     * 后果比不重连更糟：30 秒没事件就切到 {@code Flux.never()} ——
     * **既不报错也不完成，只是永远不再出声**。日志里干干净净，
     * 界面上任务一直"运行中"，而实际早就卡住了。
     * 这类"用一个看似无害的兜底把故障抹平"的写法，是流式系统里最贵的 bug。
     *
     * <h2>重连为什么不会重复处理事件</h2>
     *
     * 因为续传用的是游标：重订阅时带 {@code ?since=<已收到的最大 seq>}，
     * 内核从那个位置往后推。这是"至少一次 + 幂等消费"的标准做法 ——
     * 宁可多取一次，也不能漏。漏的代价（状态永远不动）远大于多取一次。
     *
     * <h2>为什么敢无限重试</h2>
     *
     * 因为重试次数不是由这里兜底的，而是由**租约 + Reaper** 兜底：
     * 内核真的死了的话，租约到期会把任务判为失联并重派，
     * 重派时会 {@code dispose} 掉这条订阅，重连自然就停了。
     * 两条机制职责不同、互不替代：这里保证"网络抖一下不会丢事件"，
     * 租约保证"对端真的死了会被发现"。
     */
    private Retry reconnectPolicy() {
        return Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(
                        props.getKernel().getReconnectBaseMillis()))
                .maxBackoff(Duration.ofSeconds(30))
                .jitter(0.5)
                // 主动取消（dispose）不是故障，不该触发重连 ——
                // 否则取消任务之后，后台还会不停地重连，白耗资源。
                .filter(e -> !(e instanceof CancellationException))
                .doBeforeRetry(sig -> log.warn("内核事件流断开，准备第 {} 次重连：{}",
                        sig.totalRetries() + 1, sig.failure().toString()));
    }

    /** 一次原始的订阅（不带重连）。抽出来是为了让 {@link #withReconnect} 能 defer 地反复调用它。 */
    private Flux<KernelEvent> rawEvents(String kernelTaskId, long since) {
        return http.get()
                .uri(b -> b.path("/v1/tasks/" + kernelTaskId + "/events")
                        .queryParam("since", since).build())
                .accept(MediaType.TEXT_EVENT_STREAM)
                .retrieve()
                .bodyToFlux(String.class)
                .flatMap(frame -> parseFrame(kernelTaskId, frame));
    }

    /**
     * 把一帧解析成事件。
     *
     * <p>返回 {@code Flux} 而不是单个对象：一帧里可能带多行 {@code data:}
     * （协议允许），也可能只有心跳/注释行（不以 data 开头），
     * 后者必须解析成"空"，否则会往流里塞一个 null 事件。
     */
    private Flux<KernelEvent> parseFrame(String kernelTaskId, String frame) {
        if (frame == null || frame.isBlank()) {
            return Flux.empty();
        }
        // 路 1：这一帧已经是纯 JSON（WebFlux 的 SSE 解码器剥过壳了）。
        KernelEvent direct = tryJson(frame);
        if (direct != null) {
            return Flux.just(direct);
        }
        // 路 2：裸帧，自己做行解析。
        StringBuilder data = new StringBuilder();
        for (String line : frame.split("\n")) {
            if (line.startsWith("data:")) {
                data.append(line.substring(5).trim());
            }
            // "event:" 行忽略：JSON 里已经带 type，用它作唯一真相，
            // 避免两处解析结果不一致（这正是"同一个事实只有一份"的实践）。
        }
        if (data.isEmpty()) {
            return Flux.empty();
        }
        KernelEvent parsed = tryJson(data.toString());
        if (parsed == null) {
            log.warn("内核事件解析失败（任务 {}）：原文={}", kernelTaskId,
                    data.length() > 300 ? data.substring(0, 300) + "…" : data);
            return Flux.empty();
        }
        return Flux.just(parsed);
    }

    private KernelEvent tryJson(String s) {
        try {
            JsonNode n = json.readTree(s);
            if (n == null || !n.isObject() || n.path("type").isMissingNode()) {
                return null;
            }
            return new KernelEvent(
                    n.path("seq").asLong(),
                    n.path("type").asText(""),
                    n.path("data"),
                    n.path("ts").asLong());
        } catch (Exception e) {
            return null;
        }
    }
}
