"""
Live Storage Sync & Reporting Module
====================================

این ماژول یک سیستم گزارش‌دهی/بکاپ لحظه‌ای به Cloudflare Worker است.

قابلیت‌ها:
    ۱. Push رویدادمحور (Event-Driven): هر عملیات ذخیره‌سازی (ساخت/ویرایش/حذف
       کاربر یا تغییر تنظیمات) بلافاصله به‌صورت POST به Worker ارسال می‌شود.
    ۲. همگام‌سازی زمان‌بندی‌شده (Cron): هر ۱۵ دقیقه یک‌بار بکاپ کامل ارسال
       می‌شود تا هیچ تغییری از قلم نیفتد.

متغیرهای محیطی:
    CF_WORKER_URL      — آدرس ورکر کلودفلر
                         (مثلاً https://sub-proxy.you.workers.dev/api/push-backup)
    CF_SYNC_SECRET     — پسورد اختصاصی (در هدر X-Sync-Secret ارسال می‌شود)

اختیاری:
    CF_SYNC_INTERVAL   — بازه‌ی cron به ثانیه (پیش‌فرض 900 = ۱۵ دقیقه)
    CF_SYNC_TIMEOUT    — تایم‌اوت درخواست به ثانیه (پیش‌فرض 15)
    CF_SYNC_RETRIES    — تعداد تلاش مجدد در صورت خطا (پیش‌فرض 3)
    CF_SYNC_DISABLED   — اگر مقدارش "1"/"true" باشد ماژول غیرفعال می‌شود
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime
from typing import Any, Awaitable, Callable, Optional

import httpx

logger = logging.getLogger("X4G.cf_sync")

# ── Config از متغیرهای محیطی ─────────────────────────────────────────────────
CF_WORKER_URL: str = (os.environ.get("CF_WORKER_URL") or "").strip()
CF_SYNC_SECRET: str = (os.environ.get("CF_SYNC_SECRET") or "").strip()

_INTERVAL: int = max(30, int(os.environ.get("CF_SYNC_INTERVAL") or 900))
_TIMEOUT: float = float(os.environ.get("CF_SYNC_TIMEOUT") or 15.0)
_RETRIES: int = max(1, int(os.environ.get("CF_SYNC_RETRIES") or 3))
_DISABLED: bool = str(os.environ.get("CF_SYNC_DISABLED") or "").lower() in ("1", "true", "yes")

# ── State ‌داخلی ─────────────────────────────────────────────────────────────
_client: Optional[httpx.AsyncClient] = None
_cron_task: Optional[asyncio.Task] = None
_snapshot_provider: Optional[Callable[[], dict[str, Any]]] = None
_stats = {
    "sent_ok": 0,
    "sent_fail": 0,
    "last_ok_at": None,      # type: Optional[str]
    "last_fail_at": None,    # type: Optional[str]
    "last_error": None,      # type: Optional[str]
    "last_event": None,      # type: Optional[str]
}


# ── Public API ───────────────────────────────────────────────────────────────
def enabled() -> bool:
    """آیا این ماژول به‌درستی پیکربندی شده است؟"""
    return bool(CF_WORKER_URL and CF_SYNC_SECRET) and not _DISABLED


def set_snapshot_provider(fn: Callable[[], dict[str, Any]]) -> None:
    """
    یک تابع (sync) که یک اسنپ‌شات کامل از دیتابیس (LINKS/SUBS/تنظیمات) را
    به‌صورت dict قابل‌سریالایز به JSON برمی‌گرداند. توسط main.py صدا زده می‌شود
    تا از circular-import جلوگیری شود.
    """
    global _snapshot_provider
    _snapshot_provider = fn


def get_stats() -> dict[str, Any]:
    return {
        "enabled": enabled(),
        "worker_url": CF_WORKER_URL if enabled() else None,
        "interval_sec": _INTERVAL,
        **_stats,
    }


async def start() -> None:
    """راه‌اندازی client + cron task. در startup سرور صدا زده شود."""
    global _client, _cron_task
    if not enabled():
        logger.info("cf_sync disabled (CF_WORKER_URL / CF_SYNC_SECRET not set).")
        return
    if _client is None:
        _client = httpx.AsyncClient(
            timeout=httpx.Timeout(_TIMEOUT, connect=min(_TIMEOUT, 8.0)),
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
            follow_redirects=True,
        )
    if _cron_task is None or _cron_task.done():
        _cron_task = asyncio.create_task(_periodic_loop(), name="cf_sync_cron")
    logger.info(
        f"cf_sync started → {CF_WORKER_URL} (cron every {_INTERVAL}s)"
    )


async def stop() -> None:
    """توقف تمیز: cron را کنسل و client را می‌بندد. در shutdown صدا زده شود."""
    global _client, _cron_task
    if _cron_task is not None:
        _cron_task.cancel()
        try:
            await _cron_task
        except (asyncio.CancelledError, Exception):
            pass
        _cron_task = None
    if _client is not None:
        try:
            await _client.aclose()
        except Exception:
            pass
        _client = None


def push_event(
    event_type: str,
    details: Optional[dict[str, Any]] = None,
) -> None:
    """
    یک رویداد ذخیره‌سازی را در پس‌زمینه به Worker ارسال می‌کند.
    این تابع non-blocking است — به هیچ عنوان جریان اصلی پنل را متوقف نمی‌کند.

    Args:
        event_type: مثل "USER_CREATED", "USER_UPDATED", "USER_DELETED",
                    "SUB_CREATED", "SUB_UPDATED", "SUB_DELETED",
                    "SETTINGS_UPDATED", "CRON_AUTO_SYNC".
        details:    اطلاعات موجودیت تغییریافته (uuid, label, حجم، انقضا و ...)
    """
    if not enabled():
        return
    try:
        asyncio.create_task(_send(event_type, details or {}))
    except RuntimeError:
        # اگر خارج از event-loop صدا زده شد (نامعمول)، بی‌سروصدا رد شو
        logger.debug("cf_sync.push_event called outside a running loop; skipped.")


# ── Internal ─────────────────────────────────────────────────────────────────
async def _send(event_type: str, details: dict[str, Any]) -> None:
    """ارسال واقعی درخواست POST همراه با retry/backoff."""
    global _client
    if _client is None:
        # لِیزی-init در صورتی که start() به هر دلیل صدا زده نشده باشد
        _client = httpx.AsyncClient(
            timeout=httpx.Timeout(_TIMEOUT, connect=min(_TIMEOUT, 8.0)),
        )

    payload = _build_payload(event_type, details)
    headers = {
        "Content-Type": "application/json",
        "X-Sync-Secret": CF_SYNC_SECRET,
        "X-Sync-Event": event_type,
        "User-Agent": "X4G-LiveSync/1.0",
    }

    delay = 1.0
    last_exc: Optional[Exception] = None
    for attempt in range(1, _RETRIES + 1):
        try:
            resp = await _client.post(CF_WORKER_URL, headers=headers, json=payload)
            if 200 <= resp.status_code < 300:
                _stats["sent_ok"] += 1
                _stats["last_ok_at"] = datetime.now().isoformat()
                _stats["last_event"] = event_type
                logger.debug(
                    f"cf_sync OK [{event_type}] status={resp.status_code} "
                    f"(users={payload['total_users']})"
                )
                return
            last_exc = RuntimeError(
                f"HTTP {resp.status_code}: {resp.text[:200]}"
            )
        except Exception as e:
            last_exc = e

        if attempt < _RETRIES:
            await asyncio.sleep(delay)
            delay = min(delay * 2, 10.0)

    _stats["sent_fail"] += 1
    _stats["last_fail_at"] = datetime.now().isoformat()
    _stats["last_error"] = str(last_exc) if last_exc else "unknown"
    logger.warning(
        f"cf_sync FAILED [{event_type}] after {_RETRIES} attempts: {last_exc}"
    )


def _build_payload(event_type: str, details: dict[str, Any]) -> dict[str, Any]:
    """ساخت payload نهایی JSON مطابق قرارداد."""
    backup: dict[str, Any] = {}
    total_users = 0
    if _snapshot_provider is not None:
        try:
            backup = _snapshot_provider() or {}
        except Exception as e:
            logger.warning(f"cf_sync snapshot provider raised: {e}")
            backup = {}

    # تعداد کاربران فعال را از اسنپ‌شات محاسبه کن (کاربر = link در این پنل)
    try:
        links = backup.get("links") or {}
        if isinstance(links, dict):
            total_users = sum(
                1 for l in links.values()
                if isinstance(l, dict) and l.get("active", True)
            )
        elif isinstance(links, list):
            total_users = sum(1 for l in links if isinstance(l, dict) and l.get("active", True))
    except Exception:
        total_users = 0

    return {
        "event_type": event_type,
        "timestamp": datetime.now().isoformat(),
        "timestamp_unix": int(time.time()),
        "details": details or {},
        "total_users": total_users,
        "full_backup": backup,
    }


async def _periodic_loop() -> None:
    """حلقه‌ی cron: هر _INTERVAL ثانیه یک بکاپ کامل می‌فرستد."""
    # کمی صبر تا سرور کاملاً بالا بیاید و load_state تمام شود
    # (حداکثر ۱۰ ثانیه؛ کمتر از interval)
    try:
        await asyncio.sleep(min(10, max(2, _INTERVAL // 4)))
    except asyncio.CancelledError:
        return

    while True:
        try:
            await _send(
                "CRON_AUTO_SYNC",
                {"reason": "periodic_full_backup", "interval_sec": _INTERVAL},
            )
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.warning(f"cf_sync cron iteration error: {e}")

        try:
            await asyncio.sleep(_INTERVAL)
        except asyncio.CancelledError:
            return
