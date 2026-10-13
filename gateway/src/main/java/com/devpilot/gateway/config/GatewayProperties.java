package com.devpilot.gateway.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 网关配置。
 *
 * <p>所有会变的东西都放在这里，而不是散落在代码里：
 * 内核地址、超时、租约时长、重派上限。这样"调参"和"改逻辑"是两件事 ——
 * 前者不需要重新理解代码，后者不需要重新测参数。
 */
@Component
@ConfigurationProperties(prefix = "gateway")
public class GatewayProperties {

    private final Kernel kernel = new Kernel();
    private final Lease lease = new Lease();
    private final Approval approval = new Approval();
    private final Audit audit = new Audit();

    public Kernel getKernel() { return kernel; }
    public Lease getLease() { return lease; }
    public Approval getApproval() { return approval; }
    public Audit getAudit() { return audit; }

    /** 内核连接配置。 */
    public static class Kernel {
        private String baseUrl = "http://127.0.0.1:8078";
        /** 内核只认这个内网凭证 —— 它面向用户的所有鉴权都在网关做。 */
        private String token = "dev-local-token-change-me";
        /** 普通请求超时（秒）。 */
        private int timeoutSeconds = 30;
        /**
         * 提交任务的超时。
         *
         * <p>为什么和普通请求分开？因为提交要等内核受理，而内核在忙的时候
         * 受理会慢；更常见的是第一次调用要冷启动（建库、加载配置）。
         * 用 30 秒会把"内核还在启动"误判成"提交失败"。
         */
        private int submitTimeoutSeconds = 60;
        /**
         * 事件流断线后的重连起始退避（毫秒）。
         *
         * <p>为什么是 1 秒而不是 0：内核如果真的挂了，立刻重连会变成忙等，
         * 一秒几十次请求全是"连接拒绝"。给一个退避，让它有机会恢复。
         * 之后按指数退避到 30 秒封顶 —— 再长就不合适了，
         * 因为一次交付的总时长也就是几分钟，退避太久等于错过整段过程。
         *
         * <p>做成可配置主要是为了测试：单测里把它设成 1 毫秒，
         * 就能在几十毫秒内验证"断了会重连、且从游标续传"。
         */
        private long reconnectBaseMillis = 1000;

        public String getBaseUrl() { return baseUrl; }
        public void setBaseUrl(String v) { this.baseUrl = v; }
        public String getToken() { return token; }
        public void setToken(String v) { this.token = v; }
        public int getTimeoutSeconds() { return timeoutSeconds; }
        public void setTimeoutSeconds(int v) { this.timeoutSeconds = v; }
        public int getSubmitTimeoutSeconds() { return submitTimeoutSeconds; }
        public void setSubmitTimeoutSeconds(int v) { this.submitTimeoutSeconds = v; }
        public long getReconnectBaseMillis() { return reconnectBaseMillis; }
        public void setReconnectBaseMillis(long v) { this.reconnectBaseMillis = v; }
    }

    /**
     * 租约与重派。
     *
     * <p>这两个参数是一对权衡：
     * <ul>
     *   <li>租约太短 → 正常的慢任务被误判失联，重复执行（浪费钱）</li>
     *   <li>租约太长 → 真的失联后要等很久才重派（任务卡住）</li>
     * </ul>
     * 默认 10 分钟 + 最多 2 次，是按"一次交付约 3~5 分钟"估的：
     * 留了约一倍余量，同时失联后最坏 20 分钟会被接住。
     *
     * <p>注意这里用**固定租约**而不是"最后一次心跳 + 超时"。
     * 固定租约更简单，但要求执行者主动续租（见 renewLease），
     * 否则一个跑 30 分钟的任务会在第 10 分钟被判失联。
     */
    public static class Lease {
        private long durationSeconds = 600;
        private int maxAttempts = 2;
        /** Reaper 扫描间隔（秒）。 */
        private int reapIntervalSeconds = 30;

        public long getDurationSeconds() { return durationSeconds; }
        public void setDurationSeconds(long v) { this.durationSeconds = v; }
        public long durationMillis() { return durationSeconds * 1000L; }
        public int getMaxAttempts() { return maxAttempts; }
        public void setMaxAttempts(int v) { this.maxAttempts = v; }
        public int getReapIntervalSeconds() { return reapIntervalSeconds; }
        public void setReapIntervalSeconds(int v) { this.reapIntervalSeconds = v; }
    }

    /**
     * 人工审批（HITL）的等待上限。
     *
     * <p>这个值存在的理由不是"给人足够时间点按钮"，而是**让等审批的任务也能被超时机制接住**。
     *
     * <p>一个任务挂在审批点上时，它没有执行中的副本，看起来"不该被当成失联"。
     * 但如果就这么把租约清空、让它彻底离开重派候选，就等于承认：
     * **内核一旦在等人期间挂掉，这个任务永远没人管**。用户看到的是
     * "等待审批"四个字，点下去却报错（内核那边根本没人在等）。
     *
     * <p>正确做法是给一个**有界**的等待：把租约设成内核自己的审批超时
     * （{@code DEVPILOT_APPROVAL_TIMEOUT}，默认 1800 秒）加一点余量。
     * 于是：
     * <ul>
     *   <li>内核活着：它自己会在 1800 秒时放弃并回报失败，轮不到网关出手；</li>
     *   <li>内核死了：租约在 1800+余量 秒后到期，Reaper 把任务重派出去。</li>
     * </ul>
     * 租约时长在这里不是"给用户多少时间"，而是"多久之后我们认定对端已经没了"。
     */
    public static class Approval {
        /** 必须与内核的 DEVPILOT_APPROVAL_TIMEOUT 一致，否则两侧的判断会打架。 */
        private long waitTimeoutSeconds = 1800;
        /** 余量：留给内核把"审批超时失败"这个终态事件发出来的时间。 */
        private long marginSeconds = 120;

        public long getWaitTimeoutSeconds() { return waitTimeoutSeconds; }
        public void setWaitTimeoutSeconds(long v) { this.waitTimeoutSeconds = v; }
        public long getMarginSeconds() { return marginSeconds; }
        public void setMarginSeconds(long v) { this.marginSeconds = v; }

        /** 等审批期间用的租约长度。 */
        public long leaseMillis() {
            return (waitTimeoutSeconds + marginSeconds) * 1000L;
        }
    }

    /** 审计。落 JSONL，一个文件一行，方便直接 grep 与导入。 */
    public static class Audit {        private boolean enabled = true;
        private String path = "var/audit.jsonl";

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean v) { this.enabled = v; }
        public String getPath() { return path; }
        public void setPath(String v) { this.path = v; }
    }
}
