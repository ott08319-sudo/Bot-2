import os
import sys
import time
import json
import uuid
import secrets
import hmac
import hashlib
import random
import asyncio
import logging
import html
from decimal import Decimal

import aiohttp
from aiohttp import web
import asyncpg
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandStart
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder

try:
    from flyerapi import Flyer
except Exception:
    Flyer = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ========== КОНФИГУРАЦИЯ ==========
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
ADMIN_ID = int(os.getenv("ADMIN_ID", "0") or 0)
PORT = int(os.getenv("PORT", "8080") or 8080)

PIARFLOW_API_KEY = os.getenv("PIARFLOW_API_KEY", "").strip()
FLOWLY_API_KEY = os.getenv("FLOWLY_API_KEY", "").strip()
FLYER_API_KEY = os.getenv("FLYER_API_KEY", "").strip()
BRIZAHUB_API_KEY = os.getenv("BRIZAHUB_API_KEY", "").strip()
BRIZAHUB_BASE = os.getenv("BRIZAHUB_BASE", "https://api.brizahub.com").rstrip("/")
TRAFSLY_API_KEY = os.getenv("TRAFSLY_API_KEY", "").strip()

BC_ENABLED = os.getenv("BC_ENABLED", "true").lower() in ("1", "true", "yes", "on")
BC_API_KEY = os.getenv("BC_API_KEY", "").strip()
BC_BASE = os.getenv("BC_BASE", "https://api.bytecoin.su").rstrip("/")
BC_MIN_TRANSFER = Decimal(os.getenv("BC_MIN_TRANSFER", "0.000000001"))

DEFAULT_REWARD = float(os.getenv("DEFAULT_REWARD", "100"))
DEFAULT_REF_BONUS = float(os.getenv("DEFAULT_REF_BONUS", "250"))
DEFAULT_REF_SUBS = int(os.getenv("DEFAULT_REF_SUBS", "5"))
DEFAULT_MIN_WITHDRAW = float(os.getenv("DEFAULT_MIN_WITHDRAW", "1000"))
DEFAULT_MAX_SPONSORS = int(os.getenv("DEFAULT_MAX_SPONSORS", "10"))
MAX_MAX_SPONSORS = int(os.getenv("MAX_MAX_SPONSORS", "30"))

CHECK_COOLDOWN = float(os.getenv("CHECK_COOLDOWN", "3"))
AUTOCHECK_INTERVAL = int(os.getenv("AUTOCHECK_INTERVAL", "1800"))
LAZY_THRESHOLD = int(os.getenv("LAZY_THRESHOLD", "5"))
LAZY_INTERVAL = int(os.getenv("LAZY_INTERVAL", "1800"))
FLOWLY_TIMEOUT = float(os.getenv("FLOWLY_TIMEOUT", "10"))

INCOMPLETE_STATUSES = {"incomplete", "unsubscribed", "pending", "waiting", "not_completed"}
SERVICES_ALL = ["piarflow", "flowly", "flyer", "brizahub", "trafsly"]

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()
_raw_wh_url = (
    os.getenv("WEBHOOK_URL")
    or os.getenv("RENDER_EXTERNAL_URL")
    or (f"https://{os.getenv('RAILWAY_PUBLIC_DOMAIN')}" if os.getenv("RAILWAY_PUBLIC_DOMAIN") else "")
    or ""
).strip().rstrip("/")
if _raw_wh_url.endswith("/webhook"):
    _raw_wh_url = _raw_wh_url[:-len("/webhook")].rstrip("/")
WEBHOOK_URL = _raw_wh_url

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
flyer = Flyer(FLYER_API_KEY) if (Flyer and FLYER_API_KEY) else None

_pool = None
_http = None
_states = {}
_last = {}
_user_locks = {}

# ========== БАЗОВЫЕ ХЕЛПЕРЫ ==========
async def get_pool():
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
    return _pool

async def http():
    global _http
    if _http is None or _http.closed:
        _http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
    return _http

def get_user_lock(user_id: int) -> asyncio.Lock:
    lock = _user_locks.get(user_id)
    if lock is None:
        lock = asyncio.Lock()
        _user_locks[user_id] = lock
    return lock

def escape_html(text) -> str:
    if text is None:
        return ""
    return html.escape(str(text))

def fmt_money(val) -> str:
    try:
        num = float(val or 0)
        if num == int(num):
            return str(int(num))
        s = f"{num:.6f}".rstrip("0").rstrip(".")
        return s or "0"
    except Exception:
        return str(val)

def user_display(user_id, username=None) -> str:
    if username:
        clean = str(username).lstrip("@")
        return f'<a href="tg://user?id={user_id}">@{escape_html(clean)}</a> (<code>{user_id}</code>)'
    return f'<a href="tg://user?id={user_id}">{user_id}</a>'

async def get_user_display(user_id) -> str:
    try:
        pool = await get_pool()
        async with pool.acquire() as db:
            row = await db.fetchrow("SELECT username FROM users WHERE user_id=$1", int(user_id))
        if row and row["username"]:
            return user_display(user_id, row["username"])
    except Exception:
        pass
    return user_display(user_id)

async def log_error(service: str, error_type: str, message: str):
    logging.error(f"[{service}:{error_type}] {message}")
    try:
        pool = await get_pool()
        async with pool.acquire() as db:
            await db.execute(
                "INSERT INTO error_log (service, error_type, message, created_at) VALUES ($1,$2,$3,$4)",
                str(service), str(error_type), str(message)[:1000], time.time()
            )
    except Exception:
        pass

async def notify_admin(text: str):
    if not ADMIN_ID:
        return
    try:
        await bot.send_message(ADMIN_ID, text, disable_web_page_preview=True)
    except Exception as e:
        logging.error(f"notify_admin: {e}")

async def notify_admin_new_subs(user_id, subs_list, source_label="кнопка"):
    if not ADMIN_ID or not subs_list:
        return
    try:
        user_str = await get_user_display(user_id)
        total = sum(float(x[2] or 0) for x in subs_list)
        blocks = []
        for svc, aid, rw, link, tt in subs_list:
            lines = [f"• <b>{escape_html(svc)}</b>: +{fmt_money(rw)}"]
            if link:
                lines.append(f"  🔗 {escape_html(str(link))}")
            if tt:
                lines.append(f"  📌 {escape_html(str(tt))}")
            blocks.append("\n".join(lines))
        ts = time.strftime("%d.%m %H:%M", time.localtime())
        text = (
            f"✅ <b>Новые подписки</b> ({escape_html(source_label)})\n\n"
            f"👤 <b>{user_str}</b>\n\n"
            f"📦 Выполнено: <b>{len(subs_list)}</b>\n\n"
            + "\n\n".join(blocks)
            + f"\n\n💰 Итого: <b>+{fmt_money(total)} монет</b>\n🕐 {ts}"
        )
        await bot.send_message(ADMIN_ID, text, disable_web_page_preview=True)
    except Exception as e:
        logging.error(f"notify_admin_new_subs: {e}")

async def notify_admin_new_user(user_id, username, full_name, sponsors_count, services_map, ref_id=None):
    if not ADMIN_ID:
        return
    try:
        ts = time.strftime("%d.%m %H:%M", time.localtime())
        svc_lines = "\n".join(f"  • <b>{s}</b> — {c}" for s, c in services_map.items()) if services_map else "  • нет доступных сервисов"
        ref_line = ""
        if ref_id:
            ref_line = f"\n🔗 Реферер: {await get_user_display(ref_id)}"
        user_str = user_display(user_id, username.replace("@", "") if username else None)
        text = (
            f"👤 <b>Новый пользователь</b>\n\n"
            f"🆔 {user_str}\n"
            f"📛 <b>{escape_html(full_name) or 'без имени'}</b>{ref_line}\n\n"
            f"📦 Выдано спонсоров: <b>{sponsors_count}</b>\n"
            f"🎯 По сервисам:\n{svc_lines}\n\n🕐 {ts}"
        )
        await bot.send_message(ADMIN_ID, text, disable_web_page_preview=True)
    except Exception as e:
        logging.error(f"notify_admin_new_user: {e}")

async def notify_referrer(referrer_id, ref_username, subs_count, earned):
    try:
        pool = await get_pool()
        async with pool.acquire() as db:
            row = await db.fetchrow("SELECT balance FROM users WHERE user_id=$1", referrer_id)
        bal = row["balance"] if row else 0
        text = (
            f"🎉 <b>Реферал засчитан!</b>\n\n"
            f"👤 Реферал: @{escape_html(ref_username or 'без ника')}\n"
            f"✅ Подтверждено спонсоров: <b>{subs_count}</b>\n"
            f"💎 Награда: <b>+{fmt_money(earned)} монет</b>\n"
            f"💰 Ваш баланс: <b>{fmt_money(bal)} монет</b>"
        )
        await bot.send_message(referrer_id, text, disable_web_page_preview=True)
    except Exception:
        pass

async def notify_lazy_user(user_id):
    try:
        await bot.send_message(user_id, random.choice(LAZY_USER_MSGS), disable_web_page_preview=True)
        return True
    except Exception:
        return False

async def notify_admin_lazy(user_id, subs, mins):
    try:
        await bot.send_message(
            ADMIN_ID,
            f"😴 <b>Неактивный</b>\n\n👤 {await get_user_display(user_id)}\n📊 Подписок: <b>{subs}</b>/{LAZY_THRESHOLD}\n⏱ Прошло: {mins} мин",
            disable_web_page_preview=True
        )
    except Exception:
        pass

def _bc_headers(idem=None):
    h = {"Content-Type": "application/json"}
    if BC_API_KEY.startswith("Bearer "):
        h["Authorization"] = BC_API_KEY
    else:
        h["X-API-Key"] = BC_API_KEY
    if idem:
        h["Idempotency-Key"] = idem
    return h

async def bc_transfer(user_id, amount):
    if not BC_ENABLED or not BC_API_KEY:
        return {"status": "error", "code": "DISABLED", "error": "Выплаты временно отключены"}
    amt = Decimal(str(amount)).quantize(Decimal("0.000000001"))
    if amt < BC_MIN_TRANSFER:
        return {"status": "error", "code": "VALIDATION_ERROR", "error": f"min {BC_MIN_TRANSFER}"}
    idem = f"wd-{user_id}-{uuid.uuid4()}"
    body = {"user_id": int(user_id), "sum": f"{amt:.9f}"}
    s = await http()
    for attempt in range(3):
        try:
            async with s.post(f"{BC_BASE}/service/transfer", headers=_bc_headers(idem), json=body) as r:
                data = await r.json()
                if r.status == 429:
                    await asyncio.sleep(int(r.headers.get("Retry-After", "1")))
                    continue
                return data
        except Exception as e:
            logging.error(f"bc_transfer {attempt}: {e}")
            if attempt == 2:
                return {"status": "error", "code": "NETWORK", "error": str(e)}
            await asyncio.sleep(2 ** attempt)

async def init_db():
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY, username TEXT, balance DOUBLE PRECISION DEFAULT 0,
            referrer_id BIGINT, referred_earned DOUBLE PRECISION DEFAULT 0,
            blocked BOOLEAN DEFAULT FALSE, created_at DOUBLE PRECISION)""")
        await db.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS username TEXT")
        await db.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS referrer_id BIGINT")
        await db.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS referred_earned DOUBLE PRECISION DEFAULT 0")
        await db.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS blocked BOOLEAN DEFAULT FALSE")
        await db.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS language_code TEXT")
        await db.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS ref_rewarded BOOLEAN DEFAULT FALSE")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users (LOWER(username))")
        await db.execute("""CREATE TABLE IF NOT EXISTS sponsor_tasks (
            id SERIAL PRIMARY KEY, user_id BIGINT, service TEXT, assignment_id TEXT, link TEXT,
            reward DOUBLE PRECISION DEFAULT 0, status TEXT DEFAULT 'unsubscribed',
            signature TEXT, created_at DOUBLE PRECISION, UNIQUE(user_id, service, assignment_id))""")
        await db.execute("ALTER TABLE sponsor_tasks ADD COLUMN IF NOT EXISTS task_type TEXT")
        await db.execute("ALTER TABLE sponsor_tasks ADD COLUMN IF NOT EXISTS provider_pool TEXT")
        # Удаляем зависшие неподтверждённые задания удалённого BotoHub
        await db.execute("DELETE FROM sponsor_tasks WHERE service='botohub' AND status!='subscribed'")
        await db.execute("""CREATE TABLE IF NOT EXISTS withdrawals (
            id SERIAL PRIMARY KEY, user_id BIGINT, amount DOUBLE PRECISION, status TEXT DEFAULT 'pending',
            tx_id TEXT, error TEXT, created_at DOUBLE PRECISION)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS promo_codes (
            code TEXT PRIMARY KEY, amount DOUBLE PRECISION, max_uses INT DEFAULT 1,
            used_count INT DEFAULT 0, active BOOLEAN DEFAULT TRUE, created_at DOUBLE PRECISION)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS promo_uses (
            user_id BIGINT, code TEXT, used_at DOUBLE PRECISION, PRIMARY KEY (user_id, code))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS error_log (
            id SERIAL PRIMARY KEY, service TEXT, error_type TEXT, message TEXT, created_at DOUBLE PRECISION)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS lazy_watch (
            user_id BIGINT PRIMARY KEY, referrer_id BIGINT, created_at DOUBLE PRECISION,
            last_notified DOUBLE PRECISION DEFAULT 0, admin_notified BOOLEAN DEFAULT FALSE)""")
        await db.execute("ALTER TABLE lazy_watch ADD COLUMN IF NOT EXISTS admin_notified BOOLEAN DEFAULT FALSE")
        await db.execute("""CREATE TABLE IF NOT EXISTS webhook_events (
            event_id TEXT PRIMARY KEY, service TEXT, created_at DOUBLE PRECISION)""")
        await db.execute("INSERT INTO settings (key,value) VALUES ('reward',$1) ON CONFLICT (key) DO NOTHING", str(DEFAULT_REWARD))
        await db.execute("INSERT INTO settings (key,value) VALUES ('ref_bonus',$1) ON CONFLICT (key) DO NOTHING", str(DEFAULT_REF_BONUS))
        await db.execute("INSERT INTO settings (key,value) VALUES ('ref_subs',$1) ON CONFLICT (key) DO NOTHING", str(DEFAULT_REF_SUBS))
        await db.execute("INSERT INTO settings (key,value) VALUES ('min_withdraw',$1) ON CONFLICT (key) DO NOTHING", str(DEFAULT_MIN_WITHDRAW))
        await db.execute("INSERT INTO settings (key,value) VALUES ('max_sponsors',$1) ON CONFLICT (key) DO NOTHING", str(DEFAULT_MAX_SPONSORS))
        await db.execute("INSERT INTO settings (key,value) VALUES ('autocheck','true') ON CONFLICT (key) DO NOTHING")
        await db.execute(
            "INSERT INTO settings (key,value) VALUES ('webhook_secret',$1) ON CONFLICT (key) DO NOTHING",
            WEBHOOK_SECRET or secrets.token_urlsafe(24)
        )

        # Однократная миграция старых рефералов, чтобы избежать повторных выплат
        async with db.transaction():
            migrated = await db.fetchval(
                "INSERT INTO settings (key,value) VALUES ('ref_bonus_v2_migrated','true') "
                "ON CONFLICT (key) DO NOTHING RETURNING key"
            )
            if migrated:
                await db.execute("""
                    UPDATE users u SET ref_rewarded=TRUE
                    WHERE u.referrer_id IS NOT NULL
                      AND EXISTS (
                          SELECT 1 FROM sponsor_tasks t
                          WHERE t.user_id=u.user_id AND t.status='subscribed'
                      )
                """)

async def get_setting(key, default=None):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT value FROM settings WHERE key=$1", key)
        return row["value"] if row else default

async def set_setting(key, value):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("INSERT INTO settings (key,value) VALUES ($1,$2) ON CONFLICT (key) DO UPDATE SET value=$2", key, str(value))

async def get_reward(): return float(await get_setting("reward", DEFAULT_REWARD))
async def get_ref_bonus(): return float(await get_setting("ref_bonus", DEFAULT_REF_BONUS))
async def get_ref_subs(): return int(await get_setting("ref_subs", DEFAULT_REF_SUBS))
async def get_min_withdraw(): return float(await get_setting("min_withdraw", DEFAULT_MIN_WITHDRAW))
async def get_max_sponsors(): return int(await get_setting("max_sponsors", DEFAULT_MAX_SPONSORS))

async def get_webhook_secret() -> str:
    if WEBHOOK_SECRET:
        return WEBHOOK_SECRET
    return await get_setting("webhook_secret", "")

async def is_service_enabled(service: str) -> bool:
    return (await get_setting(f"service_{service}", "true")) == "true"

async def set_service_enabled(service: str, enabled: bool):
    await set_setting(f"service_{service}", "true" if enabled else "false")

async def register_user(user_id, referrer_id=None, username=None, language_code=None):
    pool = await get_pool()
    async with pool.acquire() as db:
        exists = await db.fetchrow("SELECT user_id FROM users WHERE user_id=$1", user_id)
        if exists:
            if username:
                await db.execute("UPDATE users SET username=$1 WHERE user_id=$2", username, user_id)
            if language_code:
                await db.execute("UPDATE users SET language_code=$1 WHERE user_id=$2", language_code, user_id)
            return False
        if referrer_id == user_id:
            referrer_id = None
        if referrer_id:
            r = await db.fetchrow("SELECT user_id FROM users WHERE user_id=$1", referrer_id)
            if not r:
                referrer_id = None
        await db.execute(
            "INSERT INTO users (user_id, username, referrer_id, language_code, created_at) VALUES ($1,$2,$3,$4,$5)",
            user_id, username, referrer_id, language_code, time.time()
        )
        return True

async def update_username(user_id, username, language_code=None):
    if not username and not language_code:
        return
    try:
        pool = await get_pool()
        async with pool.acquire() as db:
            if username and language_code:
                await db.execute("UPDATE users SET username=$1, language_code=$2 WHERE user_id=$3", username, language_code, user_id)
            elif username:
                await db.execute("UPDATE users SET username=$1 WHERE user_id=$2", username, user_id)
            else:
                await db.execute("UPDATE users SET language_code=$1 WHERE user_id=$2", language_code, user_id)
    except Exception:
        pass

async def get_user(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        return await db.fetchrow(
            "SELECT balance, referrer_id, referred_earned, blocked, username, language_code FROM users WHERE user_id=$1",
            user_id
        )

async def get_user_lang(user_id):
    try:
        pool = await get_pool()
        async with pool.acquire() as db:
            row = await db.fetchrow("SELECT language_code FROM users WHERE user_id=$1", user_id)
        if row and row["language_code"]:
            return row["language_code"]
    except Exception:
        pass
    return "ru"

async def find_user(query):
    query = query.strip()
    if query.isdigit():
        pool = await get_pool()
        async with pool.acquire() as db:
            row = await db.fetchrow("SELECT user_id FROM users WHERE user_id=$1", int(query))
        return row["user_id"] if row else None
    clean = query.lstrip("@").lower()
    if not clean:
        return None
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT user_id FROM users WHERE LOWER(username)=$1 LIMIT 1", clean)
    return row["user_id"] if row else None

async def add_balance(user_id, amount):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE users SET balance = balance + $1 WHERE user_id=$2", amount, user_id)

async def add_referred_earned(user_id, amount):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE users SET referred_earned = referred_earned + $1 WHERE user_id=$2", amount, user_id)

async def save_sponsor(user_id, service, aid, link, reward, signature=None, task_type=None, provider_pool=None):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute(
            """INSERT INTO sponsor_tasks
            (user_id, service, assignment_id, link, reward, signature, task_type, provider_pool, status, created_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'unsubscribed',$9)
            ON CONFLICT (user_id, service, assignment_id) DO UPDATE
            SET link = EXCLUDED.link,
                reward = EXCLUDED.reward,
                signature = COALESCE(EXCLUDED.signature, sponsor_tasks.signature),
                task_type = COALESCE(EXCLUDED.task_type, sponsor_tasks.task_type),
                provider_pool = COALESCE(EXCLUDED.provider_pool, sponsor_tasks.provider_pool),
                status = CASE
                    WHEN sponsor_tasks.service = 'brizahub' THEN 'unsubscribed'
                    ELSE sponsor_tasks.status
                END""",
            user_id, service, str(aid), link, reward,
            str(signature) if signature is not None else None,
            task_type,
            str(provider_pool) if provider_pool is not None else None,
            time.time()
        )

async def mark_subscribed(user_id, service, aid) -> bool:
    """Атомарно переводит задание в статус subscribed. Возвращает True только при первом засчитывании."""
    pool = await get_pool()
    async with pool.acquire() as db:
        res = await db.execute(
            "UPDATE sponsor_tasks SET status='subscribed' WHERE user_id=$1 AND service=$2 AND assignment_id=$3 AND status!='subscribed'",
            user_id, service, str(aid)
        )
        return res != "UPDATE 0"

async def clear_pending_tasks(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("DELETE FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed'", user_id)

async def pending_tasks(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch(
            """SELECT service, assignment_id, link, reward, signature, task_type, provider_pool
            FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed'""",
            user_id
        )
        return [(r["service"], r["assignment_id"], r["link"], r["reward"], r["signature"], r["task_type"], r["provider_pool"]) for r in rows]

async def pending_count(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed'", user_id)
        return row["c"]

async def pending_links(user_id, limit=None):
    pool = await get_pool()
    async with pool.acquire() as db:
        q = "SELECT link FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed' ORDER BY id ASC"
        if limit:
            q += f" LIMIT {int(limit)}"
        rows = await db.fetch(q, user_id)
        return [r["link"] for r in rows]

async def pending_by_service(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch(
            "SELECT service, COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed' GROUP BY service",
            user_id
        )
        return {r["service"]: r["c"] for r in rows}

async def count_total_subs(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status='subscribed'", user_id)
        return row["c"]

async def add_lazy_watch(user_id, referrer_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute(
            "INSERT INTO lazy_watch (user_id, referrer_id, created_at, last_notified, admin_notified) VALUES ($1,$2,$3,0,FALSE) ON CONFLICT (user_id) DO NOTHING",
            user_id, referrer_id, time.time()
        )

async def remove_lazy_watch(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("DELETE FROM lazy_watch WHERE user_id=$1", user_id)

# ========== API: PIARFLOW ==========
async def get_piarflow(user_id):
    if not PIARFLOW_API_KEY:
        return []
    s = await http()
    try:
        async with s.post(
            "https://piarflow.com/v1/sponsors",
            json={"user_id": user_id, "chat_id": user_id, "max_sponsors": await get_max_sponsors()},
            headers={"Authorization": f"Bearer {PIARFLOW_API_KEY}"}
        ) as r:
            d = await r.json()
            if d.get("status") == "ok":
                return d.get("sponsors", [])
    except Exception as e:
        await log_error("piarflow", "fetch", str(e))
    return []

async def check_piarflow(user_id, links):
    if not PIARFLOW_API_KEY or not links:
        return []
    s = await http()
    try:
        async with s.post(
            "https://piarflow.com/v1/sponsors/check",
            json={"user_id": user_id, "links": links},
            headers={"Authorization": f"Bearer {PIARFLOW_API_KEY}"}
        ) as r:
            d = await r.json()
            if d.get("status") == "ok":
                return d.get("sponsors", [])
    except Exception as e:
        await log_error("piarflow", "check", str(e))
    return []

# ========== API: FLOWLY ==========
async def _flowly_request(user_id, lang="ru", is_premium=False):
    if not FLOWLY_API_KEY:
        return None
    s = await http()
    try:
        async with s.post(
            "https://flowly.guru/api/v1/offers",
            headers={"Auth": FLOWLY_API_KEY},
            json={"tg_user_id": user_id, "lang": lang or "ru", "is_premium": bool(is_premium)},
            timeout=aiohttp.ClientTimeout(total=FLOWLY_TIMEOUT)
        ) as r:
            if r.status != 200:
                return None
            return await r.json()
    except Exception:
        return None

async def get_flowly(user_id, lang="ru"):
    d = await _flowly_request(user_id, lang)
    if not d or d.get("status") == "ok":
        return []
    out = []
    for item in d.get("offers", []):
        if item.get("subscribed"):
            continue
        link = item.get("link")
        if link:
            out.append({"id": link, "link": link, "name": item.get("name", "")})
    return out

async def check_flowly(user_id, lang="ru"):
    d = await _flowly_request(user_id, lang)
    if not d:
        return None, []
    return d.get("status"), d.get("offers", [])

# ========== API: FLYER ==========
async def fetch_flyer(user_id, lang="ru"):
    if not flyer:
        return []
    try:
        tasks = await flyer.get_tasks(user_id=user_id, language_code=lang or "ru", limit=await get_max_sponsors())
        if not tasks:
            return []
        incomplete = [t for t in tasks if t.get('status') in INCOMPLETE_STATUSES]
        out = []
        for t in incomplete:
            links = t.get('links', [])
            if not links:
                continue
            out.append({
                "id": t.get('signature'),
                "link": links[0],
                "signature": t.get('signature'),
                "task": t.get('task', '')
            })
        return out
    except Exception as e:
        await log_error("flyer", "get_tasks", str(e))
    return []

async def check_flyer(signature):
    if not flyer or not signature:
        return None
    try:
        return await flyer.check_task(signature=signature)
    except Exception as e:
        await log_error("flyer", "check", str(e))
    return None

# ========== API: BRIZAHUB ==========
async def get_brizahub(user_id):
    if not BRIZAHUB_API_KEY:
        return []
    s = await http()
    limit = max(1, min(await get_max_sponsors(), 10))
    try:
        async with s.post(
            f"{BRIZAHUB_BASE}/v1/tasks",
            json={"user_id": int(user_id), "limit": limit},
            headers={"Authorization": f"Bearer {BRIZAHUB_API_KEY}", "Content-Type": "application/json"}
        ) as r:
            body = await r.text()
            if r.status != 200:
                await log_error("brizahub", "fetch_http", f"HTTP {r.status}: {body[:200]}")
                return []
            d = json.loads(body)
            out = []
            for t in d.get("tasks") or []:
                tid = t.get("task_id") if t.get("task_id") is not None else t.get("id")
                link = t.get("link") or t.get("url")
                if tid is not None and link:
                    out.append({
                        "id": str(tid),
                        "link": str(link).strip(),
                        "type": t.get("type"),
                    })
            return out
    except Exception as e:
        await log_error("brizahub", "fetch", str(e))
    return []

async def check_brizahub(user_id, task_ids):
    """Возвращает множество task_id, по которым задание выполнено."""
    if not BRIZAHUB_API_KEY or not task_ids:
        return set()
    s = await http()
    try:
        norm_ids = [int(x) if str(x).isdigit() else str(x) for x in task_ids]
        async with s.post(
            f"{BRIZAHUB_BASE}/v1/tasks/check",
            json={"user_id": int(user_id), "task_ids": norm_ids},
            headers={"Authorization": f"Bearer {BRIZAHUB_API_KEY}", "Content-Type": "application/json"}
        ) as r:
            body = await r.text()
            if r.status != 200:
                await log_error("brizahub", "check_http", f"HTTP {r.status}: {body[:200]}")
                return set()
            d = json.loads(body)
            completed = set()
            for item in d.get("results") or []:
                tid = item.get("task_id") if item.get("task_id") is not None else item.get("id")
                status = item.get("status")
                if tid is not None and status and status not in INCOMPLETE_STATUSES:
                    completed.add(str(tid))
            return completed
    except Exception as e:
        await log_error("brizahub", "check", str(e))
    return set()

# ========== API: TRAFSLY ==========
def _trafsly_normalize_ads_id(ads_id):
    if isinstance(ads_id, int):
        return ads_id
    s = str(ads_id).strip()
    if s.isdigit():
        return int(s)
    return s

async def get_trafsly(user):
    if not TRAFSLY_API_KEY:
        return []
    s = await http()
    try:
        payload = {
            "user_id": int(user.id),
            "first_name": getattr(user, "first_name", "") or "",
            "username": getattr(user, "username", "") or "",
            "language_code": getattr(user, "language_code", None) or "ru",
            "is_premium": bool(getattr(user, "is_premium", False)),
        }
        async with s.post(
            "https://api.trafsly.com/api/v1/get-sponsors",
            json=payload,
            headers={"Auth": TRAFSLY_API_KEY, "Content-Type": "application/json"}
        ) as r:
            if r.status != 200:
                return []
            d = await r.json()
            out = []
            for x in d.get("sponsors") or []:
                if x.get("status") in ("subscribed", "notgetted", "completed"):
                    continue
                link = str(x.get("link") or "").strip()
                aid = x.get("ads_id")
                if not link or aid is None:
                    continue
                if "api.trafsly.com/go" in link or "/go?token=" in link:
                    continue
                out.append({
                    "id": str(aid),
                    "link": link,
                    "source": x.get("source"),
                    "provider_pool": x.get("provider_pool"),
                })
            return out
    except Exception as e:
        await log_error("trafsly", "fetch", str(e))
    return []

async def check_trafsly(user_id, ads_id, source=None, provider_pool=None) -> bool:
    if not TRAFSLY_API_KEY or not ads_id:
        return False
    s = await http()
    norm_id = _trafsly_normalize_ads_id(ads_id)
    try:
        payload = {"user_id": int(user_id), "ads_id": norm_id}
        if source:
            payload["source"] = source
        if provider_pool:
            payload["provider_pool"] = provider_pool
        async with s.post(
            "https://api.trafsly.com/api/v1/confirm-subscription",
            json=payload,
            headers={"Auth": TRAFSLY_API_KEY, "Content-Type": "application/json"}
        ) as r:
            if r.status != 200:
                return False
            d = await r.json()
            return d.get("subscribed") is True
    except Exception as e:
        await log_error("trafsly", "check", str(e))
    return False

# ========== СБОР (с учётом выключенных) ==========
async def collect_sponsors(user):
    uid = user.id
    lang = getattr(user, "language_code", None) or await get_user_lang(uid) or "ru"
    reward = await get_reward()

    async def _call(name, coro):
        if await is_service_enabled(name):
            return await coro
        return []

    results = await asyncio.gather(
        _call("piarflow", get_piarflow(uid)),
        _call("flowly", get_flowly(uid, lang)),
        _call("flyer", fetch_flyer(uid, lang)),
        _call("brizahub", get_brizahub(uid)),
        _call("trafsly", get_trafsly(user)),
        return_exceptions=True,
    )
    piarflow, flowly, flyer_, brizahub, trafsly = results
    if isinstance(piarflow, list):
        for x in piarflow:
            if x.get("link"):
                await save_sponsor(uid, "piarflow", x["link"], x["link"], reward)
    if isinstance(flowly, list):
        for x in flowly:
            if x.get("link"):
                await save_sponsor(uid, "flowly", x["link"], x["link"], reward, task_type=x.get("name"))
    if isinstance(flyer_, list):
        for x in flyer_:
            if x.get("link") and x.get("id"):
                await save_sponsor(uid, "flyer", str(x["id"]), x["link"], reward, signature=x.get("signature"), task_type=x.get("task"))
    if isinstance(brizahub, list):
        for x in brizahub:
            if x.get("link") and x.get("id") is not None:
                await save_sponsor(uid, "brizahub", str(x["id"]), x["link"], reward, task_type=x.get("type"))
    if isinstance(trafsly, list):
        for x in trafsly:
            if x.get("link") and x.get("id") is not None:
                await save_sponsor(
                    uid, "trafsly", str(x["id"]), x["link"], reward,
                    signature=x.get("source"), provider_pool=x.get("provider_pool")
                )

# ========== РЕФЕРАЛЬНОЕ НАЧИСЛЕНИЕ (АТОМАРНОЕ) ==========
async def process_referral_reward(user_id: int):
    ref_bonus = await get_ref_bonus()
    ref_subs = await get_ref_subs()
    pool = await get_pool()
    rewarded = None

    async with pool.acquire() as db:
        async with db.transaction():
            rewarded = await db.fetchrow("""
                UPDATE users u SET ref_rewarded=TRUE
                WHERE u.user_id=$1
                  AND u.referrer_id IS NOT NULL
                  AND u.referrer_id <> u.user_id
                  AND u.ref_rewarded IS NOT TRUE
                  AND (
                      SELECT COUNT(DISTINCT t.link)
                      FROM sponsor_tasks t
                      WHERE t.user_id=u.user_id
                        AND t.status='subscribed'
                  ) >= $2
                RETURNING u.referrer_id, u.username
            """, user_id, ref_subs)

            if rewarded and ref_bonus > 0:
                await db.execute("""
                    UPDATE users
                    SET balance=balance+$1,
                        referred_earned=referred_earned+$1
                    WHERE user_id=$2
                """, ref_bonus, rewarded["referrer_id"])

    if rewarded and ref_bonus > 0:
        await notify_referrer(
            rewarded["referrer_id"],
            rewarded["username"] or "",
            ref_subs,
            ref_bonus
        )

# ========== ПРОВЕРКА (с учётом выключенных и защиты от дублей) ==========
async def _task_reward(user_id, service, aid):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow(
            "SELECT reward FROM sponsor_tasks WHERE user_id=$1 AND service=$2 AND assignment_id=$3",
            user_id, service, str(aid)
        )
        return row["reward"] if row else 0.0

async def check_all(user_id, only_service=None):
    async with get_user_lock(user_id):
        tasks = await pending_tasks(user_id)
        if not tasks:
            return 0, 0.0, 0

        enabled = set()
        for s in SERVICES_ALL:
            if only_service and s != only_service:
                continue
            if await is_service_enabled(s):
                enabled.add(s)

        lang = await get_user_lang(user_id)

        pf, fl, bh, tr = [], [], [], []
        flowly_pending = []
        link_map, task_type_map = {}, {}
        for service, aid, link, reward, sig, task_type, pool_ in tasks:
            if service not in enabled:
                continue
            link_map[aid] = link
            task_type_map[aid] = task_type
            if service == "piarflow" and link:
                pf.append(link)
            elif service == "flyer" and sig:
                fl.append((aid, sig))
            elif service == "brizahub" and aid:
                bh.append(aid)
            elif service == "trafsly" and aid:
                tr.append((aid, sig, pool_))
            elif service == "flowly":
                flowly_pending.append((service, aid, link, reward, sig, task_type))

        done_count, done_sum, skipped_count, new_subs = 0, 0.0, 0, []

        if pf:
            for r in await check_piarflow(user_id, pf):
                if r.get("status") == "subscribed":
                    lnk = r.get("link")
                    if await mark_subscribed(user_id, "piarflow", lnk):
                        rw = await _task_reward(user_id, "piarflow", lnk)
                        await add_balance(user_id, rw)
                        done_count += 1
                        done_sum += rw
                        new_subs.append(("piarflow", lnk, rw, lnk, None))
                elif r.get("status") == "not_counted":
                    skipped_count += 1

        if bh:
            completed_bh = await check_brizahub(user_id, bh)
            for aid in bh:
                if str(aid) in completed_bh and await mark_subscribed(user_id, "brizahub", aid):
                    rw = await _task_reward(user_id, "brizahub", aid)
                    await add_balance(user_id, rw)
                    done_count += 1
                    done_sum += rw
                    new_subs.append(("brizahub", aid, rw, link_map.get(aid), task_type_map.get(aid)))

        if tr:
            tr_results = await asyncio.gather(
                *[check_trafsly(user_id, aid, src, pp) for aid, src, pp in tr],
                return_exceptions=True
            )
            for (aid, _, _), ok in zip(tr, tr_results):
                if ok is True and await mark_subscribed(user_id, "trafsly", aid):
                    rw = await _task_reward(user_id, "trafsly", aid)
                    await add_balance(user_id, rw)
                    done_count += 1
                    done_sum += rw
                    new_subs.append(("trafsly", aid, rw, link_map.get(aid), None))

        for aid, sig in fl:
            if await check_flyer(sig) == "complete":
                if await mark_subscribed(user_id, "flyer", aid):
                    rw = await _task_reward(user_id, "flyer", aid)
                    await add_balance(user_id, rw)
                    done_count += 1
                    done_sum += rw
                    new_subs.append(("flyer", aid, rw, link_map.get(aid), task_type_map.get(aid)))

        if flowly_pending:
            status, offers = await check_flowly(user_id, lang)
            sub_links = {o.get("link") for o in (offers or []) if o.get("subscribed") is True and o.get("link")}
            if status == "ok" or sub_links:
                total_rw = 0.0
                for service, aid, link, reward, sig, tt in flowly_pending:
                    if status == "ok" or link in sub_links:
                        if await mark_subscribed(user_id, "flowly", aid):
                            rw = await _task_reward(user_id, "flowly", aid)
                            total_rw += rw
                            done_count += 1
                            done_sum += rw
                            new_subs.append(("flowly", aid, rw, link, tt))
                if total_rw > 0:
                    await add_balance(user_id, total_rw)

        if done_count > 0:
            await process_referral_reward(user_id)

        if new_subs:
            await notify_admin_new_subs(user_id, new_subs, source_label="кнопка" if not only_service else f"вебхук {only_service}")
        return done_count, done_sum, skipped_count

# ========== ВОРКЕРЫ ==========
async def autocheck_worker():
    await asyncio.sleep(300)
    while True:
        try:
            if await get_setting("autocheck", "true") == "true":
                pool = await get_pool()
                async with pool.acquire() as db:
                    rows = await db.fetch(
                        "SELECT user_id, username, language_code FROM users WHERE blocked = FALSE AND created_at > $1 LIMIT 500",
                        time.time() - 86400
                    )
                for r in rows:
                    uid = r["user_id"]
                    try:
                        try:
                            tg = await bot.get_chat(uid)
                            if tg.username:
                                await update_username(uid, tg.username)
                        except Exception:
                            pass
                        before = await pending_count(uid)
                        user = types.User(
                            id=uid,
                            is_bot=False,
                            first_name="",
                            language_code=r["language_code"] or "ru",
                            username=r["username"] or ""
                        )
                        await collect_sponsors(user)
                        after = await pending_count(uid)
                        if after > before:
                            try:
                                await bot.send_message(
                                    uid,
                                    f"🔔 <b>Новые задания</b>\n\n🎯 Доступно: <b>{after - before}</b>\nОткройте «🚀 Задания»."
                                )
                            except Exception:
                                pass
                    except Exception as e:
                        logging.error(f"autocheck {uid}: {e}")
                    await asyncio.sleep(0.5)
            await asyncio.sleep(AUTOCHECK_INTERVAL)
        except Exception as e:
            logging.error(f"autocheck_worker: {e}")
            await asyncio.sleep(600)

async def lazy_worker():
    await asyncio.sleep(60)
    while True:
        try:
            pool = await get_pool()
            async with pool.acquire() as db:
                rows = await db.fetch("SELECT user_id, referrer_id, created_at, last_notified, admin_notified FROM lazy_watch")
            for r in rows:
                uid = r["user_id"]
                try:
                    total = await count_total_subs(uid)
                    if total >= LAZY_THRESHOLD:
                        await remove_lazy_watch(uid)
                        continue
                    if await pending_count(uid) == 0:
                        continue
                    if time.time() - (r["last_notified"] or 0) < LAZY_INTERVAL:
                        continue
                    if await notify_lazy_user(uid):
                        pool2 = await get_pool()
                        async with pool2.acquire() as db:
                            await db.execute("UPDATE lazy_watch SET last_notified=$1 WHERE user_id=$2", time.time(), uid)
                        if not r["admin_notified"]:
                            mins = int((time.time() - (r["created_at"] or time.time())) / 60)
                            await notify_admin_lazy(uid, total, mins)
                            pool3 = await get_pool()
                            async with pool3.acquire() as db:
                                await db.execute("UPDATE lazy_watch SET admin_notified=TRUE WHERE user_id=$1", uid)
                except Exception as e:
                    logging.error(f"lazy check {uid}: {e}")
                await asyncio.sleep(0.3)
            await asyncio.sleep(LAZY_INTERVAL)
        except Exception as e:
            logging.error(f"lazy_worker: {e}")
            await asyncio.sleep(120)

# ========== ДИАГНОСТИКА ==========
async def diagnose_service(name, key, checker):
    if not key:
        return f"❌ <b>{name}</b> — ключ не задан"
    t0 = time.time()
    try:
        res = await checker()
        dt = int((time.time() - t0) * 1000)
        if res.get("error"):
            return f"❌ <b>{name}</b> — {res['error']} ({dt}мс)"
        cnt, extra = res.get("count", 0), res.get("extra", "")
        if cnt > 0:
            return f"✅ <b>{name}</b> — {cnt} заданий ({dt}мс){extra}"
        return f"🟡 <b>{name}</b> — 0 заданий ({dt}мс){extra}"
    except Exception as e:
        dt = int((time.time() - t0) * 1000)
        return f"❌ <b>{name}</b> — {escape_html(str(e)[:100])} ({dt}мс)"

async def _diag_piarflow():
    s = await http()
    async with s.post(
        "https://piarflow.com/v1/sponsors",
        json={"user_id": ADMIN_ID, "chat_id": ADMIN_ID, "max_sponsors": 5},
        headers={"Authorization": f"Bearer {PIARFLOW_API_KEY}"}
    ) as r:
        body = await r.text()
        if r.status == 404:
            return {"count": 0, "extra": " (нет офферов)"}
        if r.status != 200:
            return {"error": f"HTTP {r.status}", "count": 0}
        d = json.loads(body)
        if d.get("status") == "ok":
            return {"count": len(d.get("sponsors", []))}
        return {"error": d.get("message", "unknown"), "count": 0}

async def _diag_flowly():
    if not FLOWLY_API_KEY:
        return {"error": "ключ не задан", "count": 0}
    s = await http()
    try:
        async with s.post(
            "https://flowly.guru/api/v1/offers",
            headers={"Auth": FLOWLY_API_KEY},
            json={"tg_user_id": ADMIN_ID, "lang": "ru", "is_premium": False},
            timeout=aiohttp.ClientTimeout(total=FLOWLY_TIMEOUT)
        ) as r:
            if r.status != 200:
                return {"error": f"HTTP {r.status}", "count": 0}
            d = await r.json()
            if d.get("status") == "ok":
                return {"count": 0, "extra": " (всё выполнено)"}
            return {"count": sum(1 for o in d.get("offers", []) if not o.get("subscribed"))}
    except asyncio.TimeoutError:
        return {"error": "timeout", "count": 0}

async def _diag_flyer():
    if not flyer:
        return {"error": "flyerapi не подключён", "count": 0}
    tasks = await flyer.get_tasks(user_id=ADMIN_ID, language_code="ru", limit=5)
    if not tasks:
        return {"count": 0}
    return {"count": len([t for t in tasks if t.get('status') in INCOMPLETE_STATUSES])}

async def _diag_brizahub():
    if not BRIZAHUB_API_KEY:
        return {"error": "ключ не задан", "count": 0}
    s = await http()
    async with s.post(
        f"{BRIZAHUB_BASE}/v1/tasks",
        json={"user_id": ADMIN_ID, "limit": 5},
        headers={"Authorization": f"Bearer {BRIZAHUB_API_KEY}", "Content-Type": "application/json"}
    ) as r:
        body = await r.text()
        if r.status != 200:
            return {"error": f"HTTP {r.status}", "count": 0}
        d = json.loads(body)
        tasks = [t for t in (d.get("tasks") or []) if (t.get("task_id") is not None or t.get("id") is not None) and (t.get("link") or t.get("url"))]
        return {"count": len(tasks)}

async def _diag_trafsly():
    if not TRAFSLY_API_KEY:
        return {"error": "ключ не задан", "count": 0}
    s = await http()
    async with s.post(
        "https://api.trafsly.com/api/v1/get-sponsors",
        json={"user_id": ADMIN_ID, "language_code": "ru"},
        headers={"Auth": TRAFSLY_API_KEY, "Content-Type": "application/json"}
    ) as r:
        body = await r.text()
        if r.status != 200:
            return {"error": f"HTTP {r.status}", "count": 0}
        d = json.loads(body)
        sponsors = d.get("sponsors") or []
        direct = [x for x in sponsors if "api.trafsly.com/go" not in str(x.get("link") or "") and "/go?token=" not in str(x.get("link") or "")]
        return {"count": len(direct), "extra": f" (всего: {len(sponsors)}, прямых: {len(direct)})"}

async def run_diagnostics():
    checks = []
    mapping = [
        ("Piarflow", "piarflow", PIARFLOW_API_KEY, _diag_piarflow),
        ("Flowly", "flowly", FLOWLY_API_KEY, _diag_flowly),
        ("Flyer", "flyer", FLYER_API_KEY, _diag_flyer),
        ("BrizaHub", "brizahub", BRIZAHUB_API_KEY, _diag_brizahub),
        ("Trafsly", "trafsly", TRAFSLY_API_KEY, _diag_trafsly),
    ]
    for name, key, api, fn in mapping:
        if not await is_service_enabled(key):
            checks.append(f"⏸ <b>{name}</b> — выключен")
        else:
            checks.append(await diagnose_service(name, api, fn))
    return f"🔎 <b>Диагностика</b>\n🆔 <code>{ADMIN_ID}</code>\n\n" + "\n".join(checks)

async def get_recent_errors(hours=3):
    pool = await get_pool()
    async with pool.acquire() as db:
        return await db.fetch(
            "SELECT service, error_type, message, created_at FROM error_log WHERE created_at > $1 ORDER BY created_at DESC LIMIT 30",
            time.time() - hours * 3600
        )

async def get_top_by_balance(limit=15):
    pool = await get_pool()
    async with pool.acquire() as db:
        return await db.fetch("SELECT user_id, username, balance FROM users WHERE blocked = FALSE ORDER BY balance DESC LIMIT $1", limit)

async def get_top_by_refs(limit=15):
    pool = await get_pool()
    async with pool.acquire() as db:
        return await db.fetch(
            "SELECT u.user_id, u.username, (SELECT COUNT(*) FROM users r WHERE r.referrer_id = u.user_id AND r.ref_rewarded IS TRUE) AS refs_count FROM users u WHERE u.blocked = FALSE ORDER BY refs_count DESC, u.user_id ASC LIMIT $1",
            limit
        )

# ========== КЛАВИАТУРЫ ==========
def user_menu():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🚀 Задания"), KeyboardButton(text="👛 Кошелёк")],
        [KeyboardButton(text="📥 Вывод"), KeyboardButton(text="🤝 Партнёры")],
        [KeyboardButton(text="🎟 Бонус-код")],
    ], resize_keyboard=True, is_persistent=True)

def admin_reply_menu():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🚀 Задания"), KeyboardButton(text="👛 Кошелёк")],
        [KeyboardButton(text="📥 Вывод"), KeyboardButton(text="🤝 Партнёры")],
        [KeyboardButton(text="⚙️ Панель")],
    ], resize_keyboard=True, is_persistent=True)

def get_menu(user_id):
    return admin_reply_menu() if user_id == ADMIN_ID else user_menu()

def sponsors_kb(links):
    kb = InlineKeyboardBuilder()
    for i, link in enumerate(links, 1):
        kb.button(text=f"🔗 Спонсор {i}", url=link)
    kb.button(text="✅ Проверить подписки", callback_data="check_subs")
    rows = [2] * (len(links) // 2)
    if len(links) % 2:
        rows.append(1)
    rows.append(1)
    kb.adjust(*rows)
    return kb.as_markup()

def promo_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ Отмена", callback_data="promo_cancel")
    return kb.as_markup()

def admin_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="💰 Награда", callback_data="adm_reward")
    kb.button(text="👥 Лимит", callback_data="adm_max")
    kb.button(text="📥 Заявки", callback_data="adm_withdraws")
    kb.button(text="📊 Статистика", callback_data="adm_stats")
    kb.button(text="🏆 Топ", callback_data="adm_top")
    kb.button(text="🔎 Поиск", callback_data="adm_find")
    kb.button(text="💎 Начислить", callback_data="adm_give")
    kb.button(text="🎟 Код", callback_data="adm_promo_create")
    kb.button(text="📢 Рассылка", callback_data="adm_broadcast")
    kb.button(text="📥 Мин. вывод", callback_data="adm_minw")
    kb.button(text="🤝 Реф-бонус", callback_data="adm_refb")
    kb.button(text="🎯 Реф-порог", callback_data="adm_refs")
    kb.button(text="📊 Диагностика", callback_data="adm_diag")
    kb.button(text="🔌 Сервисы", callback_data="adm_services")
    kb.button(text="🌐 Вебхуки", callback_data="adm_webhooks")
    kb.adjust(3, 3, 3, 3, 3)
    return kb.as_markup()

def webhooks_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="🔑 Ключ Flowly", callback_data="adm_wh_flowly")
    kb.button(text="🔑 Секрет BrizaHub", callback_data="adm_wh_brizahub")
    kb.button(text="🔑 Секрет Flyer", callback_data="adm_wh_flyer")
    kb.button(text="⬅️ Назад", callback_data="diag_back")
    kb.adjust(2, 1, 1)
    return kb.as_markup()

def services_menu_kb(enabled_map):
    kb = InlineKeyboardBuilder()
    for s in SERVICES_ALL:
        emoji = "✅" if enabled_map.get(s) else "❌"
        kb.button(text=f"{emoji} {s}", callback_data=f"svc_toggle_{s}")
    kb.button(text="⬅️ Назад", callback_data="diag_back")
    kb.adjust(2, 2, 1, 1)
    return kb.as_markup()

def diag_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="🔍 Проверить сервисы", callback_data="diag_services")
    kb.button(text="⚠️ Ошибки за 3ч", callback_data="diag_errors")
    kb.button(text="⬅️ Назад", callback_data="diag_back")
    kb.adjust(2, 1)
    return kb.as_markup()

def top_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="👛 По балансу", callback_data="top_balance")
    kb.button(text="🤝 По партнёрам", callback_data="top_refs")
    kb.button(text="⬅️ Назад", callback_data="top_back")
    kb.adjust(2, 1)
    return kb.as_markup()

def wd_kb(wid):
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Подтвердить", callback_data=f"wd_done_{wid}")
    kb.button(text="❌ Отклонить", callback_data=f"wd_decl_{wid}")
    kb.adjust(2)
    return kb.as_markup()

# ========== ФРАЗЫ ==========
NO_SPONSORS_MSGS = [
    "🙂 <b>Пока новых заданий нет.</b>\n\nЗагляните через 15–20 минут — мы регулярно добавляем новых спонсоров.",
    "🌤 <b>Задания закончились.</b>\n\nОтдохните 10–15 минут — скоро появятся новые.",
    "😴 <b>Спонсоры пока отдыхают.</b>\n\nЗагляните чуть позже.",
    "🎉 <b>Всё выполнено!</b>\n\nНовые задания появятся в течение 15–20 минут.",
    "💤 <b>Пока тихо.</b>\n\nЗагляните через несколько минут."
]

LAZY_USER_MSGS = [
    "👋 <b>Здравствуйте!</b>\n\nВы зашли в бота, но пока не выполнили ни одного задания.\n\n1️⃣ «🚀 Задания»\n2️⃣ Подпишитесь\n3️⃣ «✅ Проверить»\n\n<i>Минимум 5 подписок.</i>",
    "🙂 <b>Напоминание</b>\n\nЗадания ждут: подписка занимает пару минут.",
    "📌 <b>Вы пока не начинали</b>\n\nЗайдите в «🚀 Задания».",
    "💡 <b>Всё просто</b>\n\n«🚀 Задания» → подписки → «✅ Проверить».",
    "🔔 <b>Напоминание</b>\n\nВы ещё не выполнили ни одного задания.",
    "🙌 <b>Начнём?</b>\n\nВ боте есть задания."
]

# ========== ВЫДАЧА ==========
async def issue_sponsors_message(user, chat_id, delete_msg_id=None, clean=True):
    uid = user.id
    if clean:
        await clear_pending_tasks(uid)
    await collect_sponsors(user)
    all_links = await pending_links(uid, limit=await get_max_sponsors())
    if delete_msg_id:
        try:
            await bot.delete_message(chat_id, delete_msg_id)
        except Exception:
            pass
    if not all_links:
        return False, random.choice(NO_SPONSORS_MSGS)
    reward = await get_reward()
    total_reward = reward * len(all_links)
    min_wd = await get_min_withdraw()
    text = (
        f"🎯 <b>Доступно заданий: {len(all_links)}</b>\n\n"
        f"За подписку — <b>{fmt_money(reward)} монет</b>.\n"
        f"Всего: <b>{fmt_money(total_reward)} монет</b>.\n\n"
        f"━━━━━━━━━━━━━━━\n"
        f"⚡ Награда сразу после проверки.\n"
        f"📥 Минимум: <b>{fmt_money(min_wd)} монет</b>.\n"
        f"━━━━━━━━━━━━━━━\n\n"
        f"1️⃣ Нажмите на кнопки и подпишитесь.\n"
        f"2️⃣ Нажмите «✅ Проверить подписки».\n"
        f"3️⃣ Монеты на балансе.\n\n"
        f"🔇 <i>Совет: чтобы каналы не мешали, выключите у них уведомления и уберите в Архив — отписываться не нужно.</i>"
    )
    try:
        await bot.send_message(chat_id, text, reply_markup=sponsors_kb(all_links), disable_web_page_preview=True)
    except Exception as e:
        await log_error("bot", "send_sponsors", str(e))
        return False, "❌ Ошибка отправки."
    return True, None

# ========== ХЭНДЛЕРЫ ==========
@dp.message(CommandStart())
async def start_cmd(msg: types.Message):
    args = msg.text.split()
    ref_id = None
    if len(args) > 1 and args[1].startswith("ref"):
        try:
            ref_id = int(args[1].replace("ref", ""))
        except Exception:
            pass
    un = msg.from_user.username or ""
    is_new = await register_user(msg.from_user.id, ref_id, un, msg.from_user.language_code)
    u_check = await get_user(msg.from_user.id)
    if u_check and u_check["blocked"]:
        await msg.answer("🚫 <b>Доступ ограничен.</b>")
        return
    # Выдаём постоянную клавиатуру всем при старте
    await msg.answer("👋 <b>Добро пожаловать!</b>", reply_markup=get_menu(msg.from_user.id))
    if is_new and ref_id:
        await add_lazy_watch(msg.from_user.id, ref_id)
    try:
        loading = await msg.answer("⏳ <b>Собираем задания…</b>")
    except Exception:
        loading = None
    ok, err = await issue_sponsors_message(
        msg.from_user, msg.chat.id, delete_msg_id=loading.message_id if loading else None
    )
    if is_new:
        try:
            services_map = await pending_by_service(msg.from_user.id)
            await notify_admin_new_user(
                msg.from_user.id,
                f"@{un}" if un else "",
                msg.from_user.full_name or "",
                sum(services_map.values()),
                services_map,
                ref_id
            )
        except Exception as e:
            logging.error(f"notify: {e}")
    if not ok:
        await msg.answer(err, reply_markup=get_menu(msg.from_user.id))

@dp.message(Command("admin"))
async def cmd_admin(msg: types.Message):
    if msg.from_user.id != ADMIN_ID:
        return
    await msg.answer("⚙️ <b>Панель</b>", reply_markup=admin_menu())

@dp.message(F.text == "⚙️ Панель")
async def msg_admin_panel(msg: types.Message):
    if msg.from_user.id != ADMIN_ID:
        return
    await msg.answer("⚙️ <b>Панель</b>", reply_markup=admin_menu())

@dp.message(F.text == "🚀 Задания")
async def msg_earn(msg: types.Message):
    u = await get_user(msg.from_user.id)
    if u and u["blocked"]:
        await msg.answer("🚫 <b>Доступ ограничен.</b>")
        return
    await update_username(msg.from_user.id, msg.from_user.username, msg.from_user.language_code)
    try:
        loading = await msg.answer("⏳ <b>Собираем…</b>")
    except Exception:
        loading = None
    ok, err = await issue_sponsors_message(
        msg.from_user, msg.chat.id, delete_msg_id=loading.message_id if loading else None
    )
    if not ok:
        await msg.answer(err, reply_markup=get_menu(msg.from_user.id))

@dp.message(F.text == "👛 Кошелёк")
async def msg_balance(msg: types.Message):
    u = await get_user(msg.from_user.id)
    bal = u["balance"] if u else 0
    min_wd = await get_min_withdraw()
    await msg.answer(
        f"👛 <b>Кошелёк</b>\n\n💎 Баланс: <b>{fmt_money(bal)} монет</b>\n📥 Минимум: <b>{fmt_money(min_wd)} монет</b>",
        reply_markup=get_menu(msg.from_user.id)
    )

@dp.message(F.text == "📥 Вывод")
async def msg_withdraw(msg: types.Message):
    u = await get_user(msg.from_user.id)
    if u and u["blocked"]:
        await msg.answer("🚫 <b>Доступ ограничен.</b>")
        return
    bal = u["balance"] if u else 0
    min_wd = await get_min_withdraw()
    if bal < min_wd:
        await msg.answer(
            f"❌ <b>Недостаточно</b>\n\n📥 Минимум: <b>{fmt_money(min_wd)}</b>\n💎 У вас: <b>{fmt_money(bal)}</b>",
            reply_markup=get_menu(msg.from_user.id)
        )
        return
    _states[msg.from_user.id] = "await_withdraw"
    await msg.answer(
        f"📥 <b>Вывод в Bytecoin</b>\n\n💎 Доступно: <b>{fmt_money(bal)} монет</b>\n\nОтправьте сумму:\nНапример: <code>1500</code>",
        reply_markup=get_menu(msg.from_user.id)
    )

@dp.message(F.text == "🤝 Партнёры")
async def msg_refs(msg: types.Message):
    me = await bot.get_me()
    ref_link = f"https://t.me/{me.username}?start=ref{msg.from_user.id}"
    pool = await get_pool()
    async with pool.acquire() as db:
        stats = await db.fetchrow("""
            SELECT COUNT(*) AS invited,
                   COUNT(*) FILTER (WHERE ref_rewarded IS TRUE) AS rewarded
            FROM users WHERE referrer_id=$1
        """, msg.from_user.id)
        earned = (await db.fetchrow("SELECT COALESCE(referred_earned, 0) as s FROM users WHERE user_id=$1", msg.from_user.id))["s"]
    ref_bonus = await get_ref_bonus()
    ref_subs = await get_ref_subs()
    await msg.answer(
        f"🤝 <b>Партнёры</b>\n\n"
        f"💎 Награда: <b>{fmt_money(ref_bonus)} монет</b> за реферала\n"
        f"📌 Условие: <b>{ref_subs}</b> подтверждённых спонсоров\n\n"
        f"🔗 <code>{ref_link}</code>\n\n"
        f"👤 Приглашено: <b>{stats['invited']}</b>\n"
        f"✅ Засчитано: <b>{stats['rewarded']}</b>\n"
        f"💰 Заработано: <b>{fmt_money(earned)} монет</b>",
        reply_markup=get_menu(msg.from_user.id)
    )

@dp.message(F.text == "🎟 Бонус-код")
async def msg_promo(msg: types.Message):
    if msg.from_user.id == ADMIN_ID:
        return
    _states[msg.from_user.id] = "await_promo"
    await msg.answer("🎟 <b>Бонус-код</b>\n\nОтправьте код:", reply_markup=promo_kb())

@dp.callback_query(F.data == "promo_cancel")
async def cb_promo_cancel(cq: types.CallbackQuery):
    _states.pop(cq.from_user.id, None)
    u = await get_user(cq.from_user.id)
    bal = u["balance"] if u else 0
    await cq.answer("Отменено")
    try:
        await cq.message.edit_text("Отменено.")
    except Exception:
        pass
    await cq.message.answer(f"👛 Баланс: <b>{fmt_money(bal)}</b>", reply_markup=get_menu(cq.from_user.id))

@dp.callback_query(F.data == "check_subs")
async def cb_check(cq: types.CallbackQuery):
    uid = cq.from_user.id
    if time.time() - _last.get(uid, 0) < CHECK_COOLDOWN:
        await cq.answer("⏱ Подождите")
        return
    _last[uid] = time.time()
    await cq.answer("Проверяем…")
    cnt, sm, skipped = await check_all(uid)
    remaining_links = await pending_links(uid, limit=await get_max_sponsors())
    left = len(remaining_links)
    pool = await get_pool()
    async with pool.acquire() as db:
        total_done = (await db.fetchrow("SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status='subscribed'", uid))["c"]
        total_all = (await db.fetchrow("SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1", uid))["c"]
    u = await get_user(uid)
    bal = u["balance"] if u else 0
    if left == 0 and total_all > 0:
        text = (
            f"🎉 <b>Всё выполнено!</b>\n\n"
            f"Выполнено: <b>{total_done}</b>\n"
            f"💎 Баланс: <b>{fmt_money(bal)}</b>\n\n"
            f"🔇 <i>Чтобы каналы спонсоров не засоряли ленту, просто выключите у них уведомления и отправьте в Архив — отписываться не придётся.</i>"
        )
        try:
            await cq.message.edit_text(text)
        except Exception:
            await cq.message.answer(text)
        await cq.message.answer("Меню 👇", reply_markup=get_menu(uid))
        return
    if cnt > 0:
        text = (
            f"✅ <b>Засчитано: {cnt}</b>\n\n"
            f"💰 +<b>{fmt_money(sm)}</b>\n"
            f"💎 Баланс: <b>{fmt_money(bal)}</b>\n"
            f"📊 {total_done}/{total_all}\n"
            f"⚠️ Осталось подписаться: <b>{left}</b>\n\n"
            f"🔇 <i>Лайфхак: выключите уведомления у каналов и уберите их в Архив, чтобы не мешали.</i>\n\n"
            f"<i>Подпишитесь на оставшихся спонсоров ниже и нажмите «✅ Проверить подписки»:</i>"
        )
    else:
        text = (
            f"❌ <b>Новых подписок нет</b>\n\n"
            f"📊 {total_done}/{total_all}\n"
            f"⚠️ Осталось подписаться: <b>{left}</b>\n\n"
            f"<i>Подпишитесь на оставшихся спонсоров ниже и нажмите «✅ Проверить подписки»:</i>"
        )
    markup = sponsors_kb(remaining_links)
    try:
        await cq.message.edit_text(text, reply_markup=markup, disable_web_page_preview=True)
    except Exception as e:
        if "message is not modified" not in str(e).lower():
            await cq.message.answer(text, reply_markup=markup, disable_web_page_preview=True)

@dp.callback_query(F.data == "balance_inline")
async def cb_balance_inline(cq: types.CallbackQuery):
    u = await get_user(cq.from_user.id)
    bal = u["balance"] if u else 0
    await cq.answer()
    await cq.message.answer(
        f"👛 Баланс: <b>{fmt_money(bal)}</b>\n📥 Минимум: <b>{fmt_money(await get_min_withdraw())}</b>",
        reply_markup=get_menu(cq.from_user.id)
    )

# ========== ТЕКСТОВЫЙ ХЭНДЛЕР ==========
@dp.message(F.text & ~F.text.startswith("/"))
async def text_handler(msg: types.Message):
    uid = msg.from_user.id
    state = _states.get(uid)

    if state == "await_promo":
        code = msg.text.strip()
        pool = await get_pool()
        async with pool.acquire() as db:
            async with db.transaction():
                promo = await db.fetchrow(
                    "SELECT * FROM promo_codes WHERE code=$1 AND active=TRUE FOR UPDATE",
                    code
                )
                if not promo:
                    await msg.answer("❌ Неверный", reply_markup=get_menu(uid))
                    _states.pop(uid, None)
                    return
                if promo["used_count"] >= promo["max_uses"]:
                    await msg.answer("❌ Закончился", reply_markup=get_menu(uid))
                    _states.pop(uid, None)
                    return
                if await db.fetchrow("SELECT 1 FROM promo_uses WHERE user_id=$1 AND code=$2", uid, code):
                    await msg.answer("❌ Уже активировали", reply_markup=get_menu(uid))
                    _states.pop(uid, None)
                    return
                await db.execute("UPDATE users SET balance = balance + $1 WHERE user_id=$2", promo["amount"], uid)
                await db.execute("UPDATE promo_codes SET used_count = used_count + 1 WHERE code=$1", code)
                await db.execute("INSERT INTO promo_uses (user_id, code, used_at) VALUES ($1,$2,$3)", uid, code, time.time())
        _states.pop(uid, None)
        await msg.answer(f"✅ +<b>{fmt_money(promo['amount'])} монет</b>", reply_markup=get_menu(uid))
        return

    if state == "await_withdraw":
        try:
            amount = float(msg.text.replace(",", "."))
        except Exception:
            await msg.answer("❌ Сумма?", reply_markup=get_menu(uid))
            return
        min_wd = await get_min_withdraw()
        u = await get_user(uid)
        bal = u["balance"] if u else 0
        if amount < min_wd:
            await msg.answer(f"❌ Минимум {fmt_money(min_wd)}", reply_markup=get_menu(uid))
            return
        if amount > bal:
            await msg.answer(f"❌ У вас {fmt_money(bal)}", reply_markup=get_menu(uid))
            return
        _states.pop(uid, None)
        pool = await get_pool()
        async with pool.acquire() as db:
            async with db.transaction():
                if await db.execute(
                    "UPDATE users SET balance = balance - $1 WHERE user_id=$2 AND balance >= $1",
                    amount, uid
                ) == "UPDATE 0":
                    await msg.answer("❌ Недостаточно", reply_markup=get_menu(uid))
                    return
                wid = (
                    await db.fetchrow(
                        "INSERT INTO withdrawals (user_id, amount, status, created_at) VALUES ($1,$2,'pending',$3) RETURNING id",
                        uid, amount, time.time()
                    )
                )["id"]
        await msg.answer(f"⏳ Вывод {fmt_money(amount)} монет обрабатывается…", reply_markup=get_menu(uid))
        resp = await bc_transfer(uid, amount)
        if resp.get("status") == "ok":
            pool = await get_pool()
            async with pool.acquire() as db:
                await db.execute("UPDATE withdrawals SET status='done', tx_id=$1 WHERE id=$2", resp.get("transaction_id"), wid)
            await msg.answer(
                f"✅ Выведено: <b>{fmt_money(amount)}</b>\nTX: <code>{str(resp.get('transaction_id',''))[:16]}…</code>\n\n💎 <a href=\"https://t.me/byteappbot\">@byteappbot</a>",
                reply_markup=get_menu(uid)
            )
            await notify_admin(
                f"💸 Выплата\n👤 {await get_user_display(uid)}\n💰 {fmt_money(amount)}\nTX: <code>{resp.get('transaction_id','')}</code>"
            )
        else:
            pool = await get_pool()
            async with pool.acquire() as db:
                await db.execute("UPDATE users SET balance = balance + $1 WHERE user_id=$2", amount, uid)
                await db.execute("UPDATE withdrawals SET status='failed', error=$1 WHERE id=$2", resp.get("error", ""), wid)
            await msg.answer(f"❌ Ошибка: {resp.get('error','unknown')}\nСредства возвращены.", reply_markup=get_menu(uid))
        return

    if state in ("adm_wh_flowly", "adm_wh_brizahub", "adm_wh_flyer"):
        if uid != ADMIN_ID:
            _states.pop(uid, None)
            return
        val = msg.text.strip()
        key_map = {
            "adm_wh_flowly": ("wh_key_flowly", "Flowly"),
            "adm_wh_brizahub": ("wh_key_brizahub", "BrizaHub"),
            "adm_wh_flyer": ("wh_key_flyer", "Flyer"),
        }
        db_key, label = key_map[state]
        if val in ("-", "очистить", "удалить", "clear"):
            await set_setting(db_key, "")
            _states.pop(uid, None)
            await msg.answer(f"🗑 Секрет <b>{label}</b> очищен.", reply_markup=webhooks_kb())
            return
        await set_setting(db_key, val)
        _states.pop(uid, None)
        await msg.answer(f"✅ Секрет <b>{label}</b> сохранён!", reply_markup=webhooks_kb())
        return

    if state == "adm_reward":
        try:
            v = float(msg.text.replace(",", "."))
            if v <= 0:
                await msg.answer("❌ > 0")
                return
        except Exception:
            await msg.answer("❌ Число")
            return
        await set_setting("reward", v)
        _states.pop(uid, None)
        await msg.answer(f"✅ {fmt_money(v)}", reply_markup=admin_menu())
        return

    if state == "adm_max":
        try:
            v = int(msg.text)
        except Exception:
            await msg.answer("❌ Целое")
            return
        if v > MAX_MAX_SPONSORS or v < 1:
            await msg.answer(f"❌ 1-{MAX_MAX_SPONSORS}")
            return
        await set_setting("max_sponsors", v)
        _states.pop(uid, None)
        await msg.answer(f"✅ {v}", reply_markup=admin_menu())
        return

    if state == "adm_minw":
        try:
            v = float(msg.text.replace(",", "."))
            if v <= 0:
                await msg.answer("❌ > 0")
                return
        except Exception:
            await msg.answer("❌ Число")
            return
        await set_setting("min_withdraw", v)
        _states.pop(uid, None)
        await msg.answer(f"✅ {fmt_money(v)}", reply_markup=admin_menu())
        return

    if state == "adm_refb":
        try:
            v = float(msg.text.replace(",", "."))
            if v < 0:
                await msg.answer("❌ ≥ 0")
                return
        except Exception:
            await msg.answer("❌ Число")
            return
        await set_setting("ref_bonus", v)
        _states.pop(uid, None)
        await msg.answer(f"✅ {fmt_money(v)}", reply_markup=admin_menu())
        return

    if state == "adm_refs":
        try:
            v = int(msg.text)
            if v < 1:
                await msg.answer("❌ ≥ 1")
                return
        except Exception:
            await msg.answer("❌ Целое")
            return
        await set_setting("ref_subs", v)
        _states.pop(uid, None)
        await msg.answer(f"✅ Порог подписок: {v}", reply_markup=admin_menu())
        return

    if state == "adm_broadcast":
        _states.pop(uid, None)
        await msg.answer("📢 Рассылка…")
        pool = await get_pool()
        async with pool.acquire() as db:
            rows = await db.fetch("SELECT user_id FROM users WHERE blocked=FALSE")
        ok = 0
        fail = 0
        for r in rows:
            try:
                await msg.copy_to(r["user_id"])
                ok += 1
            except Exception:
                fail += 1
            await asyncio.sleep(0.05)
        await msg.answer(f"✅ {ok}\n❌ {fail}", reply_markup=admin_menu())
        return

    if state == "adm_promo_create":
        parts = [p.strip() for p in msg.text.split("|")]
        if len(parts) != 3:
            await msg.answer("❌ <code>КОД | СУММА | АКТ</code>")
            return
        code, a_s, u_s = parts
        try:
            a = float(a_s)
            u = int(u_s)
        except Exception:
            await msg.answer("❌ Числа")
            return
        pool = await get_pool()
        async with pool.acquire() as db:
            try:
                await db.execute(
                    "INSERT INTO promo_codes (code, amount, max_uses, created_at) VALUES ($1,$2,$3,$4)",
                    code, a, u, time.time()
                )
            except Exception:
                await msg.answer("❌ Уже есть")
                return
        _states.pop(uid, None)
        await msg.answer(f"✅ <code>{code}</code>: {fmt_money(a)} × {u}", reply_markup=admin_menu())
        return

    if state == "adm_find":
        query = msg.text.strip()
        _states.pop(uid, None)
        target = await find_user(query)
        if not target:
            await msg.answer("❌ Не найден", reply_markup=admin_menu())
            return
        u = await get_user(target)
        pool = await get_pool()
        async with pool.acquire() as db:
            refs = (await db.fetchrow("SELECT COUNT(*) as c FROM users WHERE referrer_id=$1 AND ref_rewarded IS TRUE", target))["c"]
            subs = (await db.fetchrow("SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status='subscribed'", target))["c"]
        await msg.answer(
            f"🔎 {user_display(target, u['username'])}\n💰 {fmt_money(u['balance'])}\n📊 {subs}\n🤝 {refs}",
            reply_markup=admin_menu()
        )
        return

    if state == "adm_give":
        parts = msg.text.strip().split()
        if len(parts) != 2:
            await msg.answer("❌ <code>ID СУММА</code>")
            return
        target = await find_user(parts[0])
        if not target:
            await msg.answer("❌ Не найден")
            return
        try:
            a = float(parts[1].replace(",", "."))
        except Exception:
            await msg.answer("❌ Сумма")
            return
        await add_balance(target, a)
        _states.pop(uid, None)
        await msg.answer(f"✅ {fmt_money(a)} → {await get_user_display(target)}", reply_markup=admin_menu())
        try:
            await bot.send_message(target, f"🎁 +<b>{fmt_money(a)} монет</b>")
        except Exception:
            pass
        return

# ========== КОМАНДЫ ==========
@dp.message(Command("ban"))
async def cmd_ban(msg: types.Message):
    if msg.from_user.id != ADMIN_ID:
        return
    try:
        q = msg.text.split()[1]
    except Exception:
        await msg.answer("Формат: /ban ID")
        return
    t = await find_user(q)
    if not t:
        await msg.answer("❌")
        return
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE users SET blocked=TRUE WHERE user_id=$1", t)
    await msg.answer(f"🚫 {await get_user_display(t)}", reply_markup=admin_menu())

@dp.message(Command("unban"))
async def cmd_unban(msg: types.Message):
    if msg.from_user.id != ADMIN_ID:
        return
    try:
        q = msg.text.split()[1]
    except Exception:
        await msg.answer("Формат: /unban ID")
        return
    t = await find_user(q)
    if not t:
        await msg.answer("❌")
        return
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE users SET blocked=FALSE WHERE user_id=$1", t)
    await msg.answer(f"✅ {await get_user_display(t)}", reply_markup=admin_menu())

@dp.message(Command("give"))
async def cmd_give(msg: types.Message):
    if msg.from_user.id != ADMIN_ID:
        return
    try:
        p = msg.text.split()
        q = p[1]
        a = float(p[2].replace(",", "."))
    except Exception:
        await msg.answer("Формат: /give ID СУММА")
        return
    t = await find_user(q)
    if not t:
        await msg.answer("❌")
        return
    await add_balance(t, a)
    await msg.answer(f"✅ {fmt_money(a)} → {await get_user_display(t)}", reply_markup=admin_menu())
    try:
        await bot.send_message(t, f"🎁 +<b>{fmt_money(a)} монет</b>")
    except Exception:
        pass

@dp.message(Command("errors"))
async def cmd_errors(msg: types.Message):
    if msg.from_user.id != ADMIN_ID:
        return
    try:
        rows = await get_recent_errors(hours=3)
    except Exception as e:
        await msg.answer(f"❌ {e}")
        return
    if not rows:
        await msg.answer("✅ Ошибок нет")
        return
    text = "⚠️ <b>Ошибки:</b>\n\n"
    for r in rows[:15]:
        ts = time.strftime("%d.%m %H:%M", time.localtime(r["created_at"]))
        text += f"<b>{escape_html(r['service'])}</b> · {escape_html(r['error_type'])}\n<code>{escape_html((r['message'] or '')[:100])}</code>\n<i>{ts}</i>\n\n"
    await msg.answer(text[:4000])

# ========== МЕДИА ==========
@dp.message((F.photo | F.video | F.document | F.animation | F.sticker | F.voice) & (F.from_user.id == ADMIN_ID))
async def media_broadcast_handler(msg: types.Message):
    uid = msg.from_user.id
    if _states.get(uid) != "adm_broadcast":
        return
    _states.pop(uid, None)
    await msg.answer("📢 Рассылка…")
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch("SELECT user_id FROM users WHERE blocked=FALSE")
    ok = 0
    fail = 0
    for r in rows:
        try:
            await msg.copy_to(r["user_id"])
            ok += 1
        except Exception:
            fail += 1
        await asyncio.sleep(0.05)
    await msg.answer(f"✅ {ok}\n❌ {fail}", reply_markup=admin_menu())

# ========== АДМИН CALLBACKS ==========
@dp.callback_query(F.data == "adm_reward")
async def adm_reward(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    _states[cq.from_user.id] = "adm_reward"
    await cq.answer()
    try:
        await cq.message.edit_text(f"💰 Текущая: <b>{fmt_money(await get_reward())}</b>\n\nНовое:")
    except Exception:
        pass

@dp.callback_query(F.data == "adm_max")
async def adm_max(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    _states[cq.from_user.id] = "adm_max"
    await cq.answer()
    try:
        await cq.message.edit_text(f"👥 Текущий: <b>{await get_max_sponsors()}</b>\n\n1-{MAX_MAX_SPONSORS}:")
    except Exception:
        pass

@dp.callback_query(F.data == "adm_minw")
async def adm_minw(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    _states[cq.from_user.id] = "adm_minw"
    await cq.answer()
    try:
        await cq.message.edit_text(f"📥 Мин: <b>{fmt_money(await get_min_withdraw())}</b>\n\nНовое:")
    except Exception:
        pass

@dp.callback_query(F.data == "adm_refb")
async def adm_refb(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    _states[cq.from_user.id] = "adm_refb"
    await cq.answer()
    try:
        await cq.message.edit_text(f"🤝 Реф-награда: <b>{fmt_money(await get_ref_bonus())}</b>\n\nНовое:")
    except Exception:
        pass

@dp.callback_query(F.data == "adm_refs")
async def adm_refs(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    _states[cq.from_user.id] = "adm_refs"
    await cq.answer()
    try:
        await cq.message.edit_text(f"🎯 Подписок для зачёта реферала: <b>{await get_ref_subs()}</b>\n\nНовое (≥ 1):")
    except Exception:
        pass

@dp.callback_query(F.data == "adm_webhooks")
async def adm_webhooks(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    await cq.answer()
    secret = await get_webhook_secret()
    base = WEBHOOK_URL or "https://ВАШ-ДОМЕН"
    fl_key = "✅ задан" if await get_setting("wh_key_flowly", "") else "❌ не задан"
    bh_key = "✅ задан" if await get_setting("wh_key_brizahub", "") else "❌ не задан"
    fy_key = "✅ задан" if await get_setting("wh_key_flyer", "") else "❌ не задан"
    lines = [
        "🌐 <b>URL-адреса вебхуков</b>\n",
        "Скопируйте нужный URL в настройки кабинета сервиса:\n",
    ]
    for svc in SERVICES_ALL:
        lines.append(f"• <b>{svc}</b>:\n<code>{base}/webhook/{svc}/{secret}</code>")
    lines.append(
        f"\n🔐 <b>Секреты подписи (HMAC):</b>\n"
        f"• Flowly: {fl_key}\n"
        f"• BrizaHub: {bh_key}\n"
        f"• Flyer: {fy_key}\n\n"
        f"💡 <i>Если сервис не принимает токен в URL, укажите <code>{base}/webhook/&lt;сервис&gt;</code> и сохраните его секрет кнопками ниже:</i>"
    )
    try:
        await cq.message.edit_text("\n".join(lines), reply_markup=webhooks_kb(), disable_web_page_preview=True)
    except Exception:
        await cq.message.answer("\n".join(lines), reply_markup=webhooks_kb(), disable_web_page_preview=True)

@dp.callback_query(F.data.in_({"adm_wh_flowly", "adm_wh_brizahub", "adm_wh_flyer"}))
async def adm_wh_key_prompt(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    _states[cq.from_user.id] = cq.data
    await cq.answer()
    hints = {
        "adm_wh_flowly": "🔑 Отправьте <b>ключ подписи Flowly</b> (из настроек вебхука Flowly):\n\n<i>Для очистки отправьте <code>-</code></i>",
        "adm_wh_brizahub": "🔑 Отправьте <b>секрет BrizaHub</b> (начинается с <code>whsec_...</code>):\n\n<i>Для очистки отправьте <code>-</code></i>",
        "adm_wh_flyer": "🔑 Отправьте <b>секрет вебхука Flyer</b> (если задан в кабинете Flyer):\n\n<i>Для очистки отправьте <code>-</code></i>",
    }
    try:
        await cq.message.edit_text(hints[cq.data])
    except Exception:
        await cq.message.answer(hints[cq.data])

@dp.callback_query(F.data == "adm_stats")
async def adm_stats(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    pool = await get_pool()
    async with pool.acquire() as db:
        total = (await db.fetchrow("SELECT COUNT(*) as c FROM users"))["c"]
        blocked = (await db.fetchrow("SELECT COUNT(*) as c FROM users WHERE blocked=TRUE"))["c"]
        balances = (await db.fetchrow("SELECT COALESCE(SUM(balance),0) as s FROM users"))["s"]
        subs = (await db.fetchrow("SELECT COUNT(*) as c FROM sponsor_tasks WHERE status='subscribed'"))["c"]
        paid = (await db.fetchrow("SELECT COALESCE(SUM(amount),0) as s FROM withdrawals WHERE status='done'"))["s"]
        pend = (await db.fetchrow("SELECT COUNT(*) as c FROM withdrawals WHERE status='pending'"))["c"]
        errs = (await db.fetchrow("SELECT COUNT(*) as c FROM error_log WHERE created_at > $1", time.time() - 86400))["c"]
    await cq.answer()
    try:
        await cq.message.edit_text(
            f"📊 <b>Статистика</b>\n\n👥 {total} (🚫 {blocked})\n💰 {fmt_money(balances)}\n✅ {subs}\n💸 {fmt_money(paid)}\n📥 {pend}\n⚠️ {errs}",
            reply_markup=admin_menu()
        )
    except Exception:
        pass

@dp.callback_query(F.data == "adm_find")
async def adm_find(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    _states[cq.from_user.id] = "adm_find"
    await cq.answer()
    try:
        await cq.message.edit_text("🔎 <b>Найти юзера</b>\n\nID или @username:")
    except Exception:
        pass

@dp.callback_query(F.data == "adm_give")
async def adm_give(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    _states[cq.from_user.id] = "adm_give"
    await cq.answer()
    try:
        await cq.message.edit_text("💎 <b>Начислить</b>\n\n<code>ID СУММА</code>")
    except Exception:
        pass

@dp.callback_query(F.data == "adm_broadcast")
async def adm_broadcast(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    _states[cq.from_user.id] = "adm_broadcast"
    await cq.answer()
    try:
        await cq.message.edit_text("📢 Отправьте сообщение:")
    except Exception:
        pass

@dp.callback_query(F.data == "adm_promo_create")
async def adm_promo_create(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    _states[cq.from_user.id] = "adm_promo_create"
    await cq.answer()
    try:
        await cq.message.edit_text("Формат: <code>КОД | СУММА | АКТ</code>")
    except Exception:
        pass

@dp.callback_query(F.data == "adm_withdraws")
async def adm_withdraws(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch(
            "SELECT w.id, w.user_id, w.amount, w.status, u.username FROM withdrawals w LEFT JOIN users u ON u.user_id = w.user_id ORDER BY w.id DESC LIMIT 20"
        )
    if not rows:
        await cq.answer("Нет заявок", show_alert=True)
        return
    text = "📥 <b>Последние выводы:</b>\n\n"
    for r in rows:
        emoji = {"done": "✅", "pending": "⏳", "failed": "❌"}.get(r["status"], "•")
        text += f"{emoji} #{r['id']} · {user_display(r['user_id'], r['username'])} · <b>{fmt_money(r['amount'])}</b>\n"
    await cq.answer()
    try:
        await cq.message.edit_text(text[:4000], reply_markup=admin_menu())
    except Exception:
        pass

@dp.callback_query(F.data == "adm_diag")
async def adm_diag(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    await cq.answer()
    try:
        await cq.message.edit_text("📊 <b>Диагностика</b>", reply_markup=diag_kb())
    except Exception:
        await cq.message.answer("📊 <b>Диагностика</b>", reply_markup=diag_kb())

@dp.callback_query(F.data == "diag_back")
async def cb_diag_back(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    await cq.answer()
    try:
        await cq.message.edit_text("⚙️ <b>Панель</b>", reply_markup=admin_menu())
    except Exception:
        pass

@dp.callback_query(F.data == "diag_services")
async def cb_diag_services(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    await cq.answer("Проверяем…")
    try:
        await cq.message.edit_text("🔎 <b>Проверяем…</b>")
    except Exception:
        pass
    try:
        report = await run_diagnostics()
    except Exception as e:
        report = f"❌ {e}"
    if len(report) > 4000:
        report = report[:4000] + "…"
    kb = InlineKeyboardBuilder()
    kb.button(text="🔄", callback_data="diag_services")
    kb.button(text="⚠️", callback_data="diag_errors")
    kb.button(text="⬅️", callback_data="diag_back")
    kb.adjust(2, 1)
    try:
        await cq.message.edit_text(report, reply_markup=kb.as_markup())
    except Exception:
        try:
            await cq.message.answer(report, reply_markup=kb.as_markup())
        except Exception:
            pass

@dp.callback_query(F.data == "diag_errors")
async def cb_diag_errors(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    await cq.answer()
    try:
        rows = await get_recent_errors(hours=3)
    except Exception as e:
        await cq.answer(f"Ошибка БД: {e}", show_alert=True)
        return
    if not rows:
        text = "✅ <b>Ошибок нет</b>"
    else:
        text = "⚠️ <b>Ошибки:</b>\n\n"
        for r in rows[:15]:
            ts = time.strftime("%d.%m %H:%M", time.localtime(r["created_at"]))
            text += f"<b>{escape_html(r['service'])}</b> · {escape_html(r['error_type'])}\n<code>{escape_html((r['message'] or '')[:100])}</code>\n<i>{ts}</i>\n\n"
    if len(text) > 4000:
        text = text[:4000] + "…"
    kb = InlineKeyboardBuilder()
    kb.button(text="🔄", callback_data="diag_errors")
    kb.button(text="🔍", callback_data="diag_services")
    kb.button(text="⬅️", callback_data="diag_back")
    kb.adjust(2, 1)
    try:
        await cq.message.edit_text(text, reply_markup=kb.as_markup())
    except Exception:
        try:
            await cq.message.answer(text, reply_markup=kb.as_markup())
        except Exception:
            pass

@dp.callback_query(F.data == "adm_top")
async def adm_top(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    await cq.answer()
    try:
        await cq.message.edit_text("🏆 <b>Топ</b>", reply_markup=top_kb())
    except Exception:
        await cq.message.answer("🏆 <b>Топ</b>", reply_markup=top_kb())

@dp.callback_query(F.data == "top_back")
async def cb_top_back(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    await cq.answer()
    try:
        await cq.message.edit_text("⚙️ <b>Панель</b>", reply_markup=admin_menu())
    except Exception:
        pass

@dp.callback_query(F.data == "top_balance")
async def cb_top_balance(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    await cq.answer()
    rows = await get_top_by_balance(15)
    text = (
        "🏆 <b>Топ по балансу</b>\n\n"
        + "".join(f"{i}. {user_display(r['user_id'], r['username'])} — <b>{fmt_money(r['balance'])}</b>\n" for i, r in enumerate(rows, 1))
        if rows else "📭 Пусто"
    )
    kb = InlineKeyboardBuilder()
    kb.button(text="🤝", callback_data="top_refs")
    kb.button(text="🔄", callback_data="top_balance")
    kb.button(text="⬅️", callback_data="top_back")
    kb.adjust(2, 1)
    try:
        await cq.message.edit_text(text[:4000], reply_markup=kb.as_markup())
    except Exception:
        pass

@dp.callback_query(F.data == "top_refs")
async def cb_top_refs(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    await cq.answer()
    rows = await get_top_by_refs(15)
    text = (
        "🏆 <b>Топ по партнёрам</b>\n\n"
        + "".join(f"{i}. {user_display(r['user_id'], r['username'])} — <b>{r['refs_count'] or 0}</b>\n" for i, r in enumerate(rows, 1))
        if rows else "📭 Пусто"
    )
    kb = InlineKeyboardBuilder()
    kb.button(text="👛", callback_data="top_balance")
    kb.button(text="🔄", callback_data="top_refs")
    kb.button(text="⬅️", callback_data="top_back")
    kb.adjust(2, 1)
    try:
        await cq.message.edit_text(text[:4000], reply_markup=kb.as_markup())
    except Exception:
        pass

# ========== СЕРВИСЫ ==========
@dp.callback_query(F.data == "adm_services")
async def adm_services(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    await cq.answer()
    enabled_map = {s: await is_service_enabled(s) for s in SERVICES_ALL}
    text = "🔌 <b>Управление сервисами</b>\n\nНажми на сервис, чтобы включить/выключить:"
    try:
        await cq.message.edit_text(text, reply_markup=services_menu_kb(enabled_map))
    except Exception:
        await cq.message.answer(text, reply_markup=services_menu_kb(enabled_map))

@dp.callback_query(F.data.startswith("svc_toggle_"))
async def svc_toggle(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    service = cq.data.replace("svc_toggle_", "")
    cur = await is_service_enabled(service)
    await set_service_enabled(service, not cur)
    await cq.answer(f"{service}: {'выкл' if cur else 'вкл'}")
    enabled_map = {s: await is_service_enabled(s) for s in SERVICES_ALL}
    text = "🔌 <b>Управление сервисами</b>\n\nНажми на сервис, чтобы включить/выключить:"
    try:
        await cq.message.edit_text(text, reply_markup=services_menu_kb(enabled_map))
    except Exception:
        pass

# ========== ВЕБХУКИ СЕРВИСОВ ==========
SERVICE_KEYS = {
    "piarflow": PIARFLOW_API_KEY,
    "flowly": FLOWLY_API_KEY,
    "flyer": FLYER_API_KEY,
    "brizahub": BRIZAHUB_API_KEY,
    "trafsly": TRAFSLY_API_KEY,
}

UNSUB_STATUSES = {
    "unsubscribed", "unsubscribe", "left", "leave", "task.left",
    "abort", "cancelled", "canceled", "revoked", "removed"
}

PENDING_STATUSES = {
    "pending", "task.pending", "waiting", "hold"
}

SUB_STATUSES = {
    "subscribed", "subscribe", "complete", "completed", "task.completed",
    "done", "success", "new_status", "sub_completed"
}

def _extract_webhook_token(request: web.Request) -> str:
    if request.match_info.get("secret"):
        return request.match_info["secret"].strip()
    for qk in ("secret", "token", "key", "auth"):
        if request.query.get(qk):
            return request.query[qk].strip()
    for hk in ("X-Webhook-Secret", "X-Api-Key", "Auth", "Authorization"):
        val = request.headers.get(hk, "").strip()
        if val:
            if val.lower().startswith("bearer "):
                return val[7:].strip()
            return val
    return ""

async def _verify_webhook_auth(service: str, request: web.Request, raw_body: bytes, provided_token: str) -> bool:
    expected_secret = await get_webhook_secret()
    service_key = SERVICE_KEYS.get(service, "")
    valid_tokens = {t for t in (expected_secret, service_key) if t}
    token_ok = bool(provided_token and provided_token in valid_tokens)

    # 1. Flowly HMAC-SHA256: X-Flowly-Signature = hex(HMAC_SHA256(key, raw_body))
    if service == "flowly":
        fl_secret = (await get_setting("wh_key_flowly", "")).strip()
        fl_sig = request.headers.get("X-Flowly-Signature", "").strip()
        if fl_secret and fl_sig:
            clean_sig = fl_sig[7:].strip().lower() if fl_sig.lower().startswith("sha256=") else fl_sig.lower()
            calc = hmac.new(fl_secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
            return hmac.compare_digest(calc, clean_sig)
        if fl_secret and not fl_sig:
            return token_ok
        return token_ok or not valid_tokens

    # 2. BrizaHub HMAC-SHA256: X-Briza-Signature = "sha256=" + hex(HMAC_SHA256(whsec_..., f"{ts}.{body}"))
    if service == "brizahub":
        bh_secret = (await get_setting("wh_key_brizahub", "")).strip()
        bh_sig = request.headers.get("X-Briza-Signature", "").strip()
        bh_ts = request.headers.get("X-Briza-Timestamp", "").strip()
        if bh_secret and bh_sig:
            signed_bytes = (f"{bh_ts}.".encode("utf-8") + raw_body) if bh_ts else raw_body
            calc = "sha256=" + hmac.new(bh_secret.encode("utf-8"), signed_bytes, hashlib.sha256).hexdigest()
            return hmac.compare_digest(calc, bh_sig)
        if bh_secret and not bh_sig:
            return token_ok
        return token_ok or not valid_tokens

    # 3. Flyer webhook: при первичной проверке/пинге или с HMAC/токеном
    if service == "flyer":
        fy_secret = (await get_setting("wh_key_flyer", "")).strip()
        fy_sig = (
            request.headers.get("X-Flyer-Signature", "")
            or request.headers.get("X-Signature", "")
        ).strip()
        if fy_secret and fy_sig:
            clean_sig = fy_sig[7:].strip().lower() if fy_sig.lower().startswith("sha256=") else fy_sig.lower()
            calc = hmac.new(fy_secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
            return hmac.compare_digest(calc, clean_sig)
        if token_ok:
            return True
        # Разрешаем тестовый ping/challenge от Flyer при подключении URL в кабинете
        if not raw_body or len(raw_body) < 300:
            return True
        return not valid_tokens

    return token_ok or not valid_tokens

def _parse_raw_payload(request: web.Request, raw_body: bytes) -> dict:
    data = {}
    if raw_body:
        try:
            body = json.loads(raw_body.decode("utf-8"))
            if isinstance(body, dict):
                data.update(body)
        except Exception:
            try:
                from urllib.parse import parse_qs
                parsed = parse_qs(raw_body.decode("utf-8", errors="ignore"))
                for k, v in parsed.items():
                    if v:
                        data[k] = v[0]
            except Exception:
                pass
    for k, v in request.query.items():
        data.setdefault(k, v)
    nested = data.get("data")
    if isinstance(nested, dict):
        merged = dict(nested)
        merged.update({k: v for k, v in data.items() if k != "data"})
        return merged
    return data

def _extract_user_id(payload: dict):
    for key in ("user_id", "tg_user_id", "chat_id", "telegram_id", "uid"):
        val = payload.get(key)
        if val is not None and str(val).strip().lstrip("-").isdigit():
            return int(str(val).strip())
    return None

def _normalize_id_variants(raw_val) -> set:
    variants = set()
    if raw_val is None:
        return variants
    s = str(raw_val).strip()
    if not s:
        return variants
    variants.add(s)
    low = s.lower()
    if low.startswith("http://"):
        variants.add("https://" + s[7:].rstrip("/"))
    if low.startswith("https://t.me/") or low.startswith("http://t.me/") or low.startswith("t.me/"):
        clean_url = s.rstrip("/")
        if low.startswith("t.me/"):
            clean_url = "https://" + clean_url
        elif low.startswith("http://"):
            clean_url = "https://" + clean_url[7:]
        variants.add(clean_url)
        slug = clean_url.split("t.me/", 1)[-1]
        if slug and not slug.startswith("+") and "/" not in slug:
            variants.add("@" + slug.lstrip("@"))
            variants.add(slug.lstrip("@"))
    elif s.startswith("@") and len(s) > 1:
        uname = s[1:]
        variants.add(uname)
        variants.add(f"https://t.me/{uname}")
    return variants

def _extract_identifiers(payload: dict) -> set:
    ids = set()
    id_keys = (
        "ads_id", "task_id", "id", "offer_id", "sponsor_id",
        "signature", "link", "url", "sponsor_link", "invite_link",
        "channel_link", "channel", "channel_username"
    )
    nested_keys = id_keys + ("username",)
    for key in id_keys:
        if key in payload:
            ids.update(_normalize_id_variants(payload.get(key)))
    for obj_key in ("sponsor", "task", "offer", "channel"):
        obj = payload.get(obj_key)
        if isinstance(obj, dict):
            for sub_k in nested_keys:
                if sub_k in obj:
                    ids.update(_normalize_id_variants(obj.get(sub_k)))
    for list_key in ("links", "task_ids", "offers", "sponsors"):
        arr = payload.get(list_key)
        if isinstance(arr, list):
            for item in arr:
                if isinstance(item, (str, int)):
                    ids.update(_normalize_id_variants(item))
                elif isinstance(item, dict):
                    for sub_k in nested_keys:
                        if sub_k in item:
                            ids.update(_normalize_id_variants(item.get(sub_k)))
    return ids

async def _register_webhook_event(event_id: str, service: str) -> bool:
    if not event_id:
        return True
    pool = await get_pool()
    async with pool.acquire() as db:
        inserted = await db.fetchval(
            "INSERT INTO webhook_events (event_id, service, created_at) VALUES ($1,$2,$3) "
            "ON CONFLICT (event_id) DO NOTHING RETURNING event_id",
            str(event_id), service, time.time()
        )
        return inserted is not None

async def webhook_handler(request: web.Request):
    service = request.match_info.get("service", "").lower().strip()
    if service not in SERVICES_ALL:
        return web.json_response({"ok": False, "error": "unknown_service"}, status=404)

    raw_body = await request.read()
    provided_token = _extract_webhook_token(request)

    if not await _verify_webhook_auth(service, request, raw_body, provided_token):
        await log_error(service, "webhook_auth", f"Invalid signature/token from {request.remote}")
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

    payload = _parse_raw_payload(request, raw_body)

    # Ответ на challenge-проверку при добавлении вебхука (например, Flyer)
    challenge = payload.get("challenge") or request.query.get("challenge")
    if challenge is not None:
        if request.method == "GET":
            return web.Response(text=str(challenge), status=200)
        return web.json_response({"ok": True, "challenge": challenge})

    user_id = _extract_user_id(payload)

    # Тестовый ping от биржи при сохранении URL в кабинете
    if not user_id:
        return web.json_response({"ok": True, "status": "pong"})

    event_id = payload.get("event_id")
    if event_id and not await _register_webhook_event(str(event_id), service):
        return web.json_response({"ok": True, "duplicate": True})

    if not await is_service_enabled(service):
        return web.json_response({"ok": True, "ignored": "service_disabled"})

    raw_status = str(
        payload.get("event")
        or payload.get("status")
        or payload.get("type")
        or payload.get("action")
        or ""
    ).lower().strip()
    sub_flag = payload.get("subscribed")
    if isinstance(sub_flag, str):
        sub_flag = {"true": True, "1": True, "false": False, "0": False}.get(sub_flag.lower())

    # 0. Промежуточный статус (например, BrizaHub task.pending — подписался, ждём оплаты)
    if raw_status in PENDING_STATUSES:
        return web.json_response({"ok": True, "status": "pending"})

    identifiers = _extract_identifiers(payload)

    # 1. Обработка отписки (Flowly unsubscribed / BrizaHub task.left и др.)
    if raw_status in UNSUB_STATUSES or sub_flag is False:
        if not identifiers:
            raw_preview = raw_body.decode("utf-8", errors="ignore")[:500]
            await log_error(
                service,
                "webhook_unsub_no_id",
                f"user_id={user_id}, payload={raw_preview}"
            )
            await notify_admin(
                f"⚠️ <b>Отписка (вебхук {escape_html(service)})</b>\n"
                f"👤 {await get_user_display(user_id)}\n"
                f"🛡 <b>Списание заблокировано:</b> сервис не передал идентификатор канала.\n"
                f"<code>{escape_html(raw_preview[:300])}</code>"
            )
            return web.json_response({"ok": True, "action": "unsubscribed_ignored_no_id"})

        deducted_total = 0.0
        revoked_count = 0
        revoked_links = []
        async with get_user_lock(user_id):
            pool = await get_pool()
            async with pool.acquire() as db:
                rows = await db.fetch(
                    """UPDATE sponsor_tasks SET status='unsubscribed'
                       WHERE user_id=$1 AND service=$2 AND status='subscribed'
                         AND (
                             assignment_id = ANY($3::text[])
                             OR signature = ANY($3::text[])
                             OR link = ANY($3::text[])
                             OR RTRIM(link, '/') = ANY($3::text[])
                         )
                       RETURNING reward, link""",
                    user_id, service, list(identifiers)
                )
                for r in rows:
                    revoked_count += 1
                    deducted_total += float(r["reward"] or 0)
                    if r["link"]:
                        revoked_links.append(str(r["link"]))
                if deducted_total > 0:
                    await db.execute(
                        "UPDATE users SET balance = GREATEST(0, balance - $1) WHERE user_id=$2",
                        deducted_total, user_id
                    )

        if revoked_count == 0:
            raw_preview = raw_body.decode("utf-8", errors="ignore")[:500]
            await log_error(
                service,
                "webhook_unsub_unmatched",
                f"user_id={user_id}, ids={list(identifiers)[:10]}, payload={raw_preview}"
            )
            return web.json_response({"ok": True, "action": "unsubscribed_not_matched", "deducted": 0})

        deduct_note = f"\n💸 Списано с баланса: <b>-{fmt_money(deducted_total)} монет</b>" if deducted_total > 0 else ""
        links_note = ""
        if revoked_links:
            links_note = "\n" + "\n".join(f"🔗 {escape_html(lnk)}" for lnk in revoked_links[:5])
        await notify_admin(
            f"⚠️ <b>Отписка (вебхук {escape_html(service)})</b>\n"
            f"👤 {await get_user_display(user_id)}\n"
            f"📦 Отменено заданий: <b>{revoked_count}</b>{deduct_note}{links_note}"
        )
        return web.json_response({"ok": True, "action": "unsubscribed", "revoked": revoked_count, "deducted": deducted_total})

    # 2. Обработка подтверждения подписки (BrizaHub task.completed / Flowly / Flyer / Trafsly / Piarflow)
    if raw_status in SUB_STATUSES or sub_flag is True or not raw_status:
        credited_count = 0
        credited_sum = 0.0
        new_subs = []

        async with get_user_lock(user_id):
            tasks = await pending_tasks(user_id)
            matched = []
            for svc, aid, link, rw, sig, tt, pp in tasks:
                if svc != service:
                    continue
                if not identifiers or (aid in identifiers) or (link in identifiers) or (sig and sig in identifiers):
                    matched.append((aid, link, rw, tt))

            for aid, link, rw, tt in matched:
                if await mark_subscribed(user_id, service, aid):
                    await add_balance(user_id, rw)
                    credited_count += 1
                    credited_sum += rw
                    new_subs.append((service, aid, rw, link or aid, tt))

        # Если задача не нашлась в pending или пришли не все поля — допроверяем через API сервиса
        if credited_count == 0:
            cnt, sm, _ = await check_all(user_id, only_service=service)
            credited_count += cnt
            credited_sum += sm
        else:
            await process_referral_reward(user_id)
            if new_subs:
                await notify_admin_new_subs(user_id, new_subs, source_label=f"вебхук {service}")

        if credited_count > 0:
            u = await get_user(user_id)
            bal = u["balance"] if u else 0
            try:
                await bot.send_message(
                    user_id,
                    f"⚡ <b>Подписка засчитана автоматически!</b>\n\n"
                    f"💰 +<b>{fmt_money(credited_sum)} монет</b>\n"
                    f"💎 Баланс: <b>{fmt_money(bal)} монет</b>\n\n"
                    f"🔇 <i>Совет: выключите уведомления у канала и уберите его в Архив, чтобы не мешал.</i>",
                    reply_markup=get_menu(user_id)
                )
            except Exception:
                pass

        return web.json_response({"ok": True, "credited": credited_count})

    return web.json_response({"ok": True, "ignored": raw_status})

# ========== WEB + MAIN ==========
async def health(_):
    return web.Response(text="ok")

async def start_web():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_post("/webhook/{service}", webhook_handler)
    app.router.add_get("/webhook/{service}", webhook_handler)
    app.router.add_post("/webhook/{service}/{secret}", webhook_handler)
    app.router.add_get("/webhook/{service}/{secret}", webhook_handler)
    # Совместимость со ссылками, где продублировалось /webhook/webhook/...
    app.router.add_post("/webhook/webhook/{service}", webhook_handler)
    app.router.add_get("/webhook/webhook/{service}", webhook_handler)
    app.router.add_post("/webhook/webhook/{service}/{secret}", webhook_handler)
    app.router.add_get("/webhook/webhook/{service}/{secret}", webhook_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    logging.info(f"Web on :{PORT}")

async def on_shutdown(*args, **kwargs):
    global _http
    if _http and not _http.closed:
        await _http.close()

async def main():
    await init_db()
    await start_web()
    asyncio.create_task(autocheck_worker())
    asyncio.create_task(lazy_worker())
    dp.shutdown.register(on_shutdown)
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
