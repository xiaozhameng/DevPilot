#!/usr/bin/env node
/**
 * 用 Chrome DevTools Protocol 给演示页截图。
 *
 * 为什么不用 `--screenshot` 那个一次性的参数（踩过）：
 * 演示页会订阅一条 SSE 事件流（`/api/tasks/{id}/events`），这是一条**永不结束**的
 * 长连接。配合 `--virtual-time-budget` 时 Chrome 会一直等页面"网络空闲"，
 * 于是永远等不到，进程被外部超时杀掉（表现为 `Trace/BPT trap: 5` 或 SIGTERM），
 * 一张图都拿不到 —— 而报错信息完全不提 SSE。
 *
 * CDP 的路子反而简单：真的打开页面 → 真的等 N 秒 → 让浏览器拍一张。
 * 长连接在不在都无所谓。
 *
 * 用法： node scripts/screenshot.mjs <url> <out.png> [width] [height]
 */
import { spawn } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const [url, out, w = "1440", h = "900", waitMs = "6000"] = process.argv.slice(2);
if (!url || !out) {
  console.error("用法: node scripts/screenshot.mjs <url> <out.png> [w] [h] [waitMs]");
  process.exit(2);
}

const CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";
const PORT = 9333 + Math.floor(Math.random() * 500);
const profile = mkdtempSync(join(tmpdir(), "devpilot-shot-"));

const chrome = spawn(CHROME, [
  "--headless=new",
  "--no-sandbox",
  "--disable-gpu",
  "--disable-dev-shm-usage",
  "--no-first-run",
  "--hide-scrollbars",
  // 2 倍缩放：截图要进 README，1x 在视网膜屏上看是糊的。
  "--force-device-scale-factor=2",
  `--window-size=${w},${h}`,
  `--user-data-dir=${profile}`,
  `--remote-debugging-port=${PORT}`,
  "about:blank",
], { stdio: "ignore" });

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function cdpTarget() {
  for (let i = 0; i < 60; i++) {
    try {
      const r = await fetch(`http://127.0.0.1:${PORT}/json/list`);
      const tabs = await r.json();
      const page = tabs.find((t) => t.type === "page");
      if (page?.webSocketDebuggerUrl) return page.webSocketDebuggerUrl;
    } catch { /* 还没起来 */ }
    await sleep(250);
  }
  throw new Error("等不到 Chrome 的调试端口");
}

let id = 0;
function makeSend(ws) {
  const pending = new Map();
  ws.addEventListener("message", (e) => {
    const msg = JSON.parse(e.data);
    if (msg.id && pending.has(msg.id)) {
      pending.get(msg.id)(msg.result);
      pending.delete(msg.id);
    }
  });
  return (method, params = {}) =>
    new Promise((resolve) => {
      const mid = ++id;
      pending.set(mid, resolve);
      ws.send(JSON.stringify({ id: mid, method, params }));
    });
}

try {
  const wsUrl = await cdpTarget();
  const ws = new WebSocket(wsUrl);
  await new Promise((res, rej) => {
    ws.addEventListener("open", res);
    ws.addEventListener("error", rej);
  });
  const send = makeSend(ws);

  await send("Page.enable");
  await send("Emulation.setDeviceMetricsOverride", {
    width: Number(w), height: Number(h), deviceScaleFactor: 2, mobile: false,
  });
  await send("Page.navigate", { url });

  // 真等一会儿：让异步接口回来、列表渲染完、字体加载完。
  // 默认 6 秒够静态页用；问答类页面要传更长的 waitMs（一次完整回答 7 秒以上）。
  await sleep(Number(waitMs));

  const { data } = await send("Page.captureScreenshot", {
    format: "png",
    captureBeyondViewport: true,   // 内容比视口长时也整页拍下来
  });
  writeFileSync(out, Buffer.from(data, "base64"));
  console.log(`  ✓ ${out.split("/").pop()}  ${Buffer.byteLength(data, "base64")} B`);
} catch (err) {
  console.error(`  ✗ 截图失败：${err.message}`);
  process.exitCode = 1;
} finally {
  chrome.kill("SIGKILL");
  // 删临时 profile 有可能失败：Chrome 刚被 SIGKILL，它的子进程可能还在写这个目录
  // （表现为 ENOTEMPTY: rmdir）。这不影响截图结果，不该让脚本以异常退出 ——
  // 否则 CI 里会把一次成功截图判成失败。
  try { rmSync(profile, { recursive: true, force: true }); } catch { /* 交给系统清 tmp */ }
}
