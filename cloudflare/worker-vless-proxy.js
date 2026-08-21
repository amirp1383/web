// ═══════════════════════════════════════════════════════════════════════════
// 🇩🇪 X4G — Cloudflare Worker VLESS-WS Proxy
// ═══════════════════════════════════════════════════════════════════════════
//
// این Worker به‌عنوان یه پروکسی بین مشتری و سرور Railway کار می‌کنه:
//
//   📱 مشتری در ایران
//         ↓ WebSocket
//   🇩🇪 Cloudflare Worker (این فایل - نزدیک‌ترین دیتاسنتر Cloudflare)
//         ↓ WebSocket
//   🏢 سرور Railway (پنل X4G)
//         ↓
//   🌐 اینترنت آزاد
//
// فایده:
//   • IP اروپایی (Cloudflare معمولاً برای IP ایران دیتاسنتر آلمان/هلند/ترکیه انتخاب می‌کنه)
//   • پینگ کمتر (نزدیک‌تر از Railway)
//   • IP اصلی Railway مخفی می‌مونه
//   • در ایران بازتره
//   • حفاظت DDoS و کش خودکار Cloudflare
//
// محدودیت:
//   • فقط پروتکل vless-ws (WebSocket) — نه XHTTP
//   • رایگان: تا 100,000 درخواست/روز (~30-50 کاربر همزمان)
//   • پلن Paid ($5/ماه): 10 میلیون درخواست
//
// نصب:
//   1. برو https://dash.cloudflare.com → Workers & Pages → Create Worker
//   2. اسم بذار (مثلاً: sub-proxy)
//   3. کد رو پیست کن → Deploy
//   4. تنظیم متغیر Environment:
//      • BACKEND_HOST = آدرس Railway بدون https:// (مثلاً hub-production-742e.up.railway.app)
//   5. آدرس Worker (مثلاً sub-proxy.yourname.workers.dev) رو تو پنل X4G تنظیم کن:
//      داشبورد → کارت Cloudflare Worker Proxy → ذخیره تنظیمات
// ═══════════════════════════════════════════════════════════════════════════

export default {
  async fetch(request, env, ctx) {
    const upgradeHeader = request.headers.get("Upgrade");

    // ─── مسیر 1: WebSocket برای VLESS-WS ─────────────────────────────────
    if (upgradeHeader && upgradeHeader.toLowerCase() === "websocket") {
      return handleWebSocket(request, env);
    }

    // ─── مسیر 2: HTTP معمولی (Subscription) ──────────────────────────────
    // این باعث می‌شه لینک /p/xxx و /sub/xxx هم از طریق worker سرو بشه
    return handleHTTP(request, env);
  },
};

// ─────────────────────────────────────────────────────────────────────────
// WebSocket relay: مشتری ↔ Worker ↔ Railway
// ─────────────────────────────────────────────────────────────────────────
async function handleWebSocket(request, env) {
  const backend = env.BACKEND_HOST;
  if (!backend) {
    return new Response("BACKEND_HOST env var not set", { status: 500 });
  }

  const url = new URL(request.url);
  const backendUrl = `wss://${backend}${url.pathname}${url.search}`;

  // اتصال به backend
  const backendSocket = new WebSocket(backendUrl, "vless"); // subprotocol اختیاری
  // بعضی نسخه‌های Workers از سازنده‌ی WebSocket direct پشتیبانی نمی‌کنن؛
  // در این صورت از fetch + Upgrade استفاده می‌کنیم:
  let backendResponse;
  try {
    backendResponse = await fetch(backendUrl.replace(/^wss:/, "https:"), {
      headers: {
        Upgrade: "websocket",
        Connection: "Upgrade",
        "Sec-WebSocket-Key": request.headers.get("Sec-WebSocket-Key") || "",
        "Sec-WebSocket-Version": request.headers.get("Sec-WebSocket-Version") || "13",
        "Sec-WebSocket-Protocol": request.headers.get("Sec-WebSocket-Protocol") || "",
      },
    });
  } catch (e) {
    return new Response(`Backend WS fetch failed: ${e.message}`, { status: 502 });
  }

  const backendWS = backendResponse.webSocket;
  if (!backendWS) {
    return new Response("Backend didn't upgrade to WebSocket", {
      status: backendResponse.status || 502,
    });
  }
  backendWS.accept();

  // WebSocket به سمت مشتری
  const [clientSocket, serverSocket] = Object.values(new WebSocketPair());
  serverSocket.accept();

  // ─── دو طرفه رله: مشتری ↔ Backend ─────────────────────────────
  serverSocket.addEventListener("message", (event) => {
    try { backendWS.send(event.data); } catch (e) {}
  });
  backendWS.addEventListener("message", (event) => {
    try { serverSocket.send(event.data); } catch (e) {}
  });

  const closeBoth = () => {
    try { serverSocket.close(); } catch (e) {}
    try { backendWS.close(); } catch (e) {}
  };
  serverSocket.addEventListener("close", closeBoth);
  serverSocket.addEventListener("error", closeBoth);
  backendWS.addEventListener("close", closeBoth);
  backendWS.addEventListener("error", closeBoth);

  return new Response(null, {
    status: 101,
    webSocket: clientSocket,
  });
}

// ─────────────────────────────────────────────────────────────────────────
// HTTP proxy: برای مسیرهای subscription (مثل /p/xxx و /sub/xxx)
// ─────────────────────────────────────────────────────────────────────────
async function handleHTTP(request, env) {
  const backend = env.BACKEND_HOST;
  if (!backend) {
    return new Response(
      "X4G Worker Proxy is up! ✅\n\n" +
      "Set BACKEND_HOST env var to your Railway domain, e.g.:\n" +
      "  hub-production-742e.up.railway.app",
      { headers: { "Content-Type": "text/plain; charset=utf-8" } }
    );
  }

  const url = new URL(request.url);
  const backendUrl = `https://${backend}${url.pathname}${url.search}`;

  // کپی هدرها و جسم درخواست
  const newHeaders = new Headers(request.headers);
  newHeaders.set("X-Forwarded-Host", backend);
  newHeaders.set("Host", backend);

  const backendReq = new Request(backendUrl, {
    method: request.method,
    headers: newHeaders,
    body: request.method !== "GET" && request.method !== "HEAD" ? request.body : undefined,
    redirect: "manual",
  });

  try {
    const resp = await fetch(backendReq);
    // پاسخ رو بدون تغییر برگردون
    return new Response(resp.body, {
      status: resp.status,
      statusText: resp.statusText,
      headers: resp.headers,
    });
  } catch (e) {
    return new Response(`Backend fetch failed: ${e.message}`, { status: 502 });
  }
}
