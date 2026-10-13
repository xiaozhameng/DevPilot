package com.devpilot.gateway.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * 统一异常 → HTTP 状态码的映射。
 *
 * <h2>为什么必须集中做，而不是每个 controller 自己 try-catch</h2>
 *
 * 因为**状态码是协议的一部分**，它是客户端唯一能稳定依赖的信息。
 * 如果每个接口各自决定"任务不存在该返 404 还是 500"，
 * 客户端就没法写出通用的错误处理，只能对每个接口硬编码。
 *
 * <h2>三类错误，三种含义</h2>
 *
 * <ul>
 *   <li><b>404 Not Found</b>：你要的东西不存在。客户端不该重试。</li>
 *   <li><b>400 Bad Request</b>：参数不对。客户端改了参数再试有意义。</li>
 *   <li><b>409 Conflict</b>：**参数没问题，但当前状态不允许这个操作**。
 *       比如给一个已经成功的任务点审批。这是最关键的一类 ——
 *       它和 400 的区别是：400 是"你发的东西有问题"，409 是"你发的没问题，
 *       但时机不对"。客户端对 409 的正确反应是**刷新状态**，而不是改参数重试。</li>
 * </ul>
 *
 * <p>把 409 单独分出来，是这套接口能被称为"状态机驱动"的标志：
 * 状态冲突是一等公民，而不是被降级成一个笼统的 400。
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(NoSuchElementException.class)
    public ResponseEntity<Map<String, Object>> notFound(NoSuchElementException e) {
        return body(HttpStatus.NOT_FOUND, e.getMessage());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> badRequest(IllegalArgumentException e) {
        return body(HttpStatus.BAD_REQUEST, e.getMessage());
    }

    /**
     * 状态冲突：状态机拒绝了这次操作，或者"还没派发就要求审批/恢复"。
     */
    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<Map<String, Object>> conflict(IllegalStateException e) {
        return body(HttpStatus.CONFLICT, e.getMessage());
    }

    /**
     * 内核返回的错误。
     *
     * <p>**原样透传状态码**：内核用 409 表达"不在等待审批"、
     * 用 404 表达"任务不存在"，这些语义在网关这一层是**完全一样**的。
     * 如果统一压成 500，客户端就丢掉了"该刷新状态"还是"该报错了"的判断依据。
     */
    @ExceptionHandler(WebClientResponseException.class)
    public ResponseEntity<Map<String, Object>> fromKernel(WebClientResponseException e) {
        log.warn("内核返回 {}：{}", e.getStatusCode(), e.getResponseBodyAsString());
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("ok", false);
        m.put("error", "kernel_error");
        m.put("message", e.getResponseBodyAsString());
        return ResponseEntity.status(e.getStatusCode()).body(m);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> unexpected(Exception e) {
        // 未预期的异常要打全栈：它是 bug 的信号，不是用户输入的问题。
        log.error("网关内部错误", e);
        return body(HttpStatus.INTERNAL_SERVER_ERROR,
                e.getClass().getSimpleName() + ": " + e.getMessage());
    }

    private ResponseEntity<Map<String, Object>> body(HttpStatus status, String message) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("ok", false);
        m.put("status", status.value());
        m.put("message", message);
        return ResponseEntity.status(status).body(m);
    }
}
