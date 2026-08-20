# relay_vless.py
# بخش VLESS Relay — جدا شده از main.py (منطق اصلی دست‌نخورده)
# تغییر: ثبت IP واقعی کلاینت (با احتساب هدر x-forwarded-for پشت پراکسی) در connections

import asyncio
import secrets
from datetime import datetime

from fastapi import WebSocket, WebSocketDisconnect

from main import (
    LINKS,
    LINKS_LOCK,
    stats,
    hourly_traffic,
    connections,
    error_logs,
    logger,
    is_link_allowed,
    is_ip_allowed,
    save_state,
    log_activity,
    now_ir,
)
from speed_limit import throttle

# ══════════════════════════════════════════════════════════════════════════════
# VLESS Relay — بهینه‌شده برای حداکثر throughput
# ══════════════════════════════════════════════════════════════════════════════

# بافر relay از 256KB به 1MB افزایش یافت.
# با اینترنت پرسرعت (100Mbps+) و کاربران هم‌زمان زیاد، بافر بزرگ‌تر یعنی:
#   - syscall های read/write کمتر
#   - کاهش overhead جابجایی داده بین کرنل و userspace
#   - throughput پایدارتر روی دانلود‌های سنگین
RELAY_BUF = 1024 * 1024   # 1 MB buffer (قبلاً 256 KB)

# تنظیمات بهینه‌سازی سوکت‌های TCP هدف (upstream)
SOCK_BUF_SIZE = 2 * 1024 * 1024   # SO_SNDBUF / SO_RCVBUF = 2MB

# چک کوتا/محدودیت — به‌جای هر پیام، هر N بایت یک بار قفل رو می‌گیریم
# (باعث می‌شه lock contention کم بشه، مخصوصاً روی دانلود سنگین)
QUOTA_BATCH_BYTES = 128 * 1024   # 128 KB


def _tune_upstream_socket(writer: asyncio.StreamWriter):
    """TCP_NODELAY + بافرهای بزرگ سوکت روی اتصال upstream.
    این باعث می‌شه سرعت واقعی به CPU/سیستم‌عامل محدود نشه و از پهنای‌باند
    شبکه‌ی سرور Railway کامل استفاده بشه."""
    sock = writer.transport.get_extra_info("socket")
    if not sock:
        return
    try:
        import socket as _s
        sock.setsockopt(_s.IPPROTO_TCP, _s.TCP_NODELAY, 1)
        sock.setsockopt(_s.SOL_SOCKET, _s.SO_SNDBUF, SOCK_BUF_SIZE)
        sock.setsockopt(_s.SOL_SOCKET, _s.SO_RCVBUF, SOCK_BUF_SIZE)
    except OSError:
        pass

def _ws_client_ip(ws: WebSocket) -> str:
    fwd = ws.headers.get("x-forwarded-for")
    if fwd:
        return fwd.split(",")[0].strip()
    real_ip = ws.headers.get("x-real-ip")
    if real_ip:
        return real_ip.strip()
    return ws.client.host if ws.client else "نامشخص"

async def parse_vless_header(chunk: bytes):
    if len(chunk) < 24:
        raise ValueError("chunk too small")
    pos = 1
    pos += 16
    addon_len = chunk[pos]; pos += 1 + addon_len
    command = chunk[pos]; pos += 1
    port = int.from_bytes(chunk[pos:pos+2], "big"); pos += 2
    addr_type = chunk[pos]; pos += 1
    if addr_type == 1:
        address = ".".join(str(b) for b in chunk[pos:pos+4]); pos += 4
    elif addr_type == 2:
        dlen = chunk[pos]; pos += 1
        address = chunk[pos:pos+dlen].decode("utf-8", errors="ignore"); pos += dlen
    elif addr_type == 3:
        ab = chunk[pos:pos+16]; pos += 16
        address = ":".join(f"{ab[i]:02x}{ab[i+1]:02x}" for i in range(0, 16, 2))
    else:
        raise ValueError(f"unknown addr type: {addr_type}")
    return command, address, port, chunk[pos:]

async def check_and_use(uid: str, n: int) -> bool:
    async with LINKS_LOCK:
        link = LINKS.get(uid)
        if link is None:
            return False
        if not is_link_allowed(link):
            return False
        link["used_bytes"] += n
        stats["total_bytes"] += n
        hourly_traffic[now_ir().strftime("%H:00")] += n
    return True

# ── Batched quota gate ────────────────────────────────────────────────────────
# به‌جای اینکه بابت هر پیام یک بار قفل بگیریم و آمار رو آپدیت کنیم،
# بایت‌ها رو تجمیع می‌کنیم و هر QUOTA_BATCH_BYTES یک بار async قفل رو می‌گیریم.
# این باعث می‌شه throughput به‌شدت بره بالا (کمتر lock, کمتر ctx switch).
_batch_buckets: dict = {}   # {conn_id: pending_bytes}

async def _batched_check_and_use(uid: str, n: int, conn_id: str) -> bool:
    pending = _batch_buckets.get(conn_id, 0) + n
    if pending < QUOTA_BATCH_BYTES:
        _batch_buckets[conn_id] = pending
        return True
    _batch_buckets[conn_id] = 0
    return await check_and_use(uid, pending)

def _flush_batch(conn_id: str, uid: str):
    """در پایان اتصال، بایت‌های باقی‌مانده رو یک‌بار ثبت می‌کنه."""
    pending = _batch_buckets.pop(conn_id, 0)
    if pending > 0:
        asyncio.create_task(check_and_use(uid, pending))

async def relay_ws_to_tcp(ws: WebSocket, writer: asyncio.StreamWriter, conn_id: str, uid: str):
    try:
        while True:
            msg = await ws.receive()
            if msg["type"] == "websocket.disconnect":
                break
            data = msg.get("bytes") or (msg.get("text") or "").encode()
            if not data:
                continue
            # چک کوتا به‌صورت batched انجام می‌شه (کمتر قفل → سریع‌تر)
            if not await _batched_check_and_use(uid, len(data), conn_id):
                await ws.close(code=1008, reason="quota/disabled/unknown")
                break
            await throttle(uid, len(data))
            stats["total_requests"] += 1
            connections[conn_id]["bytes"] += len(data)
            writer.write(data)
            # فقط زمانی drain کن که واقعاً backpressure باشه (بافر بزرگ‌تره)
            if writer.transport.get_write_buffer_size() > RELAY_BUF:
                await writer.drain()
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        _flush_batch(conn_id, uid)
        try:
            writer.write_eof()
        except Exception:
            pass

async def relay_tcp_to_ws(ws: WebSocket, reader: asyncio.StreamReader, conn_id: str, uid: str):
    first = True
    try:
        while True:
            data = await reader.read(RELAY_BUF)
            if not data:
                break
            if not await _batched_check_and_use(uid, len(data), conn_id):
                await ws.close(code=1008, reason="quota/disabled/unknown")
                break
            await throttle(uid, len(data))
            connections[conn_id]["bytes"] += len(data)
            payload = (b"\x00\x00" + data) if first else data
            first = False
            await ws.send_bytes(payload)
    except Exception:
        pass
    finally:
        _flush_batch(conn_id, uid)

async def websocket_tunnel(ws: WebSocket, uuid: str):
    await ws.accept()

    async with LINKS_LOCK:
        link = LINKS.get(uuid)

    if not is_link_allowed(link):
        logger.warning(f"🚫 WS rejected uuid={uuid[:8]}… (not allowed)")
        await ws.close(code=1008, reason="not authorized")
        return

    ip = _ws_client_ip(ws)

    if not is_ip_allowed(link, uuid, ip):
        logger.warning(f"🚫 WS rejected uuid={uuid[:8]}… ip={ip} (ip limit reached)")
        log_activity("connection", f"اتصال {ip} به کانفیگ «{link.get('label','?')}» رد شد (محدودیت تعداد آی‌پی)", "warn")
        await ws.close(code=1008, reason="ip limit reached")
        return

    conn_id = secrets.token_urlsafe(6)
    connections[conn_id] = {
        "uuid": uuid,
        "ip": ip,
        "transport": "vless-ws",
        "connected_at": datetime.now().isoformat(),
        "bytes": 0,
    }
    logger.info(f"✅ WS [{conn_id}] uuid={uuid[:8]}… ip={ip} total={len(connections)}")
    log_activity("connection", f"اتصال جدید از {ip} (کانفیگ {link.get('label','?')})", "info")
    writer = None

    try:
        first_msg = await asyncio.wait_for(ws.receive(), timeout=15.0)
        if first_msg["type"] == "websocket.disconnect":
            return
        first_chunk = first_msg.get("bytes") or (first_msg.get("text") or "").encode()
        if not first_chunk:
            return

        command, address, port, payload = await parse_vless_header(first_chunk)

        if not await check_and_use(uuid, len(first_chunk)):
            await ws.close(code=1008, reason="quota/disabled")
            return

        stats["total_requests"] += 1
        connections[conn_id]["bytes"] += len(first_chunk)
        logger.info(f"➡️  [{conn_id}] → {address}:{port}")

        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(address, port),
            timeout=10.0
        )
        # TCP_NODELAY + بافرهای بزرگ سوکت (2MB) روی upstream
        _tune_upstream_socket(writer)

        if payload:
            writer.write(payload)
            await writer.drain()

        done, pending = await asyncio.wait(
            {
                asyncio.create_task(relay_ws_to_tcp(ws, writer, conn_id, uuid)),
                asyncio.create_task(relay_tcp_to_ws(ws, reader, conn_id, uuid)),
            },
            return_when=asyncio.FIRST_COMPLETED,
        )
        for t in pending:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass

        asyncio.create_task(save_state())

    except WebSocketDisconnect:
        pass
    except asyncio.TimeoutError:
        stats["total_errors"] += 1
        error_logs.append({"error": "connection timeout", "time": datetime.now().isoformat()})
    except Exception as exc:
        stats["total_errors"] += 1
        error_logs.append({"error": str(exc), "time": datetime.now().isoformat()})
        logger.error(f"WS error [{conn_id}]: {exc}")
    finally:
        if writer:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
        connections.pop(conn_id, None)
        logger.info(f"🔌 WS closed [{conn_id}] total={len(connections)}")
