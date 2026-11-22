import os
import json
import time
import uuid
import asyncio
import logging
import secrets
import random
import hmac
import hashlib
from decimal import Decimal

import aiohttp
import asyncpg
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandStart
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardMarkup, KeyboardButton
from aiohttp import web

# ========== НАСТРОЙКИ И КОНСТАНТЫ ==========
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
DATABASE_URL = os.getenv("DATABASE_URL", "")
PORT = int(os.getenv("PORT", "8080"))

WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").rstrip("/")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

BC_API_KEY = os.getenv("BC_API_KEY", "")
BC_BASE = os.getenv("BC_BASE", "https://byteappbot.com/api/v1")
BC_ENABLED = bool(BC_API_KEY)
BC_MIN_TRANSFER = Decimal("0.000000001")

PIARFLOW_API_KEY = os.getenv("PIARFLOW_API_KEY", "")
FLOWLY_API_KEY = os.getenv("FLOWLY_API_KEY", "")
FLYER_API_KEY = os.getenv("FLYER_API_KEY", "")
BRIZAHUB_API_KEY = os.getenv("BRIZAHUB_API_KEY", "")
BRIZAHUB_BASE = os.getenv("BRIZAHUB_BASE", "https://api.brizahub.com")
TRAFSLY_API_KEY = os.getenv("TRAFSLY_API_KEY", "")

SERVICES_ALL = ["piarflow", "flowly", "flyer", "brizahub", "trafsly"]

# Настройки по умолчанию (сохраняются в БД)
DEFAULT_REWARD = 0.5
DEFAULT_REF_BONUS = 0.5
DEFAULT_REF_SUBS = 3
DEFAULT_MIN_WITHDRAW = 5.0
DEFAULT_MAX_SPONSORS = 15
MAX_MAX_SPONSORS = 30

AUTOCHECK_INTERVAL = 3600
LAZY_INTERVAL = 86400
LAZY_THRESHOLD = 5
CHECK_COOLDOWN = 15
FLOWLY_TIMEOUT = 10

INCOMPLETE_STATUSES = {"not_started", "in_progress", "pending", "left", "unsubscribed"}
UNSUB_STATUSES = {"unsubscribed", "unsubscribe", "left", "leave", "abort", "cancelled", "canceled", "revoked", "removed"}
SUB_STATUSES = {"subscribed", "subscribe", "complete", "completed", "done", "success", "task_completed", "sub_completed"}

# ========== ИНИЦИАЛИЗАЦИЯ ==========
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher()

_pool = None
_http = None
_states = {}
_last = {}
_user_locks = {}

# Если у вас есть библиотека flyerapi:
try:
    from flyerapi import FlyerAPI
    flyer = FlyerAPI(FLYER_API_KEY) if FLYER_API_KEY else None
except ImportError:
    flyer = None

def escape_html(text):
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def fmt_money(value):
    s = f"{float(value):.3f}".rstrip("0").rstrip(".")
    return "0" if s == "" else s

async def get_pool():
    global _pool
    if not _pool:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    return _pool

async def http():
    global _http
    if not _http or _http.closed:
        _http = aiohttp.ClientSession()
    return _http

def get_user_lock(user_id):
    if user_id not in _user_locks:
        _user_locks[user_id] = asyncio.Lock()
    return _user_locks[user_id]

# ========== БАЗА И УТИЛИТЫ ==========
async def get_user_display(user_id):
    try:
        pool = await get_pool()
        async with pool.acquire() as db:
            row = await db.fetchrow("SELECT username FROM users WHERE user_id=$1", user_id)
        if row and row["username"]:
            return f"@{escape_html(row['username'])} (<code>{user_id}</code>)"
    except Exception:
        pass
    return f"<code>{user_id}</code>"

def user_display(user_id, username):
    if username:
        return f"@{escape_html(username)} (<code>{user_id}</code>)"
    return f"<code>{user_id}</code>"

async def log_error(service, error_type, message):
    try:
        pool = await get_pool()
        async with pool.acquire() as db:
            recent = await db.fetchrow(
                "SELECT id FROM error_log WHERE service=$1 AND error_type=$2 AND created_at > $3 LIMIT 1",
                service, error_type, time.time() - 300
            )
            if recent:
                return
            await db.execute(
                "INSERT INTO error_log (service, error_type, message, created_at) VALUES ($1,$2,$3,$4)",
                service, error_type, message[:500], time.time()
            )
    except Exception as e:
        logging.error(f"log_error failed: {e}")

async def notify_admin(text):
    if not ADMIN_ID: return
    try:
        await bot.send_message(ADMIN_ID, text, disable_web_page_preview=True)
    except Exception:
        pass

async def notify_admin_new_subs(user_id, subs_list, source_label="проверка"):
    if not ADMIN_ID or not subs_list:
        return
    try:
        pool = await get_pool()
        async with pool.acquire() as db:
            row = await db.fetchrow("SELECT username FROM users WHERE user_id=$1", user_id)
        username = row["username"] if row else None
        user_str = user_display(user_id, username)
        total = sum(item[2] for item in subs_list)
        by_service = {}
        for service, aid, reward, link, task_type in subs_list:
            by_service.setdefault(service, [])
            by_service[service].append((aid, reward, link, task_type))
        blocks = []
        for service, items in by_service.items():
            cnt = len(items)
            sm = sum(r for _, r, _, _ in items)
            lines = [f"🎯 <b>{service}</b> ({cnt}, +{fmt_money(sm)})"]
            for aid, reward, link, tt in items:
                short_link = str(link) if link else str(aid)
                if len(short_link) > 60:
                    short_link = short_link[:57] + "…"
                lines.append(f"  • <a href=\"{short_link}\">{short_link}</a>")
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

LAZY_USER_MSGS = [
    "👋 <b>Здравствуйте!</b>\n\nВы зашли в бота, но пока не выполнили ни одного задания.\n\n1️⃣ «🚀 Задания»\n2️⃣ Подпишитесь\n3️⃣ «✅ Проверить»",
    "🙂 <b>Напоминание</b>\n\nЗадания ждут: подписка занимает пару минут."
]

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
        
        await db.execute("""CREATE TABLE IF NOT EXISTS sponsor_tasks (
            id SERIAL PRIMARY KEY, user_id BIGINT, service TEXT, assignment_id TEXT, link TEXT,
            reward DOUBLE PRECISION DEFAULT 0, status TEXT DEFAULT 'unsubscribed',
            signature TEXT, created_at DOUBLE PRECISION, UNIQUE(user_id, service, assignment_id))""")
        await db.execute("ALTER TABLE sponsor_tasks ADD COLUMN IF NOT EXISTS task_type TEXT")
        await db.execute("ALTER TABLE sponsor_tasks ADD COLUMN IF NOT EXISTS provider_pool TEXT")
        
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
        return await db.fetchrow("SELECT balance, referrer_id, referred_earned, blocked, username, language_code FROM users WHERE user_id=$1", user_id)

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
    if not clean: return None
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT user_id FROM users WHERE LOWER(username)=$1 LIMIT 1", clean)
    return row["user_id"] if row else None

async def add_balance(user_id, amount):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE users SET balance = balance + $1 WHERE user_id=$2", amount, user_id)

async def save_sponsor(user_id, service, aid, link, reward, signature=None, task_type=None, provider_pool=None):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute(
            """INSERT INTO sponsor_tasks
            (user_id, service, assignment_id, link, reward, signature, task_type, provider_pool, status, created_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'unsubscribed',$9)
            ON CONFLICT (user_id, service, assignment_id) DO UPDATE
            SET link = EXCLUDED.link, reward = EXCLUDED.reward,
                signature = COALESCE(EXCLUDED.signature, sponsor_tasks.signature),
                task_type = COALESCE(EXCLUDED.task_type, sponsor_tasks.task_type),
                provider_pool = COALESCE(EXCLUDED.provider_pool, sponsor_tasks.provider_pool),
                status = CASE
                    WHEN sponsor_tasks.service = 'brizahub' THEN 'unsubscribed'
                    ELSE sponsor_tasks.status
                END""",
            user_id, service, str(aid), link, reward,
            str(signature) if signature is not None else None,
            task_type, str(provider_pool) if provider_pool is not None else None, time.time()
        )

async def mark_subscribed(user_id, service, aid) -> bool:
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
            "SELECT service, assignment_id, link, reward, signature, task_type, provider_pool FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed'",
            user_id
        )
        return [(r["service"], r["assignment_id"], r["link"], r["reward"], r["signature"], r["task_type"], r["provider_pool"]) for r in rows]

async def pending_count(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        return (await db.fetchrow("SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed'", user_id))["c"]

async def pending_links(user_id, limit=None):
    pool = await get_pool()
    async with pool.acquire() as db:
        q = "SELECT link FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed' ORDER BY id ASC"
        if limit: q += f" LIMIT {int(limit)}"
        rows = await db.fetch(q, user_id)
        return [r["link"] for r in rows]

async def pending_by_service(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch("SELECT service, COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed' GROUP BY service", user_id)
        return {r["service"]: r["c"] for r in rows}

async def count_total_subs(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        return (await db.fetchrow("SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status='subscribed'", user_id))["c"]

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

# ========== API МЕТОДЫ ==========
async def get_piarflow(user_id):
    if not PIARFLOW_API_KEY: return []
    s = await http()
    try:
        async with s.post(
            "https://piarflow.com/v1/sponsors",
            json={"user_id": user_id, "chat_id": user_id, "max_sponsors": await get_max_sponsors()},
            headers={"Authorization": f"Bearer {PIARFLOW_API_KEY}"}
        ) as r:
            d = await r.json()
            if d.get("status") == "ok": return d.get("sponsors", [])
    except Exception as e:
        await log_error("piarflow", "fetch", str(e))
    return []

async def check_piarflow(user_id, links):
    if not PIARFLOW_API_KEY or not links: return []
    s = await http()
    try:
        async with s.post(
            "https://piarflow.com/v1/sponsors/check",
            json={"user_id": user_id, "links": links},
            headers={"Authorization": f"Bearer {PIARFLOW_API_KEY}"}
        ) as r:
            d = await r.json()
            if d.get("status") == "ok": return d.get("sponsors", [])
    except Exception as e:
        await log_error("piarflow", "check", str(e))
    return []

async def _flowly_request(user_id, lang="ru", is_premium=False):
    if not FLOWLY_API_KEY: return None
    s = await http()
    try:
        async with s.post(
            "https://flowly.guru/api/v1/offers",
            headers={"Auth": FLOWLY_API_KEY},
            json={"tg_user_id": user_id, "lang": lang or "ru", "is_premium": bool(is_premium)},
            timeout=aiohttp.ClientTimeout(total=FLOWLY_TIMEOUT)
        ) as r:
            if r.status != 200: return None
            return await r.json()
    except Exception:
        return None

async def get_flowly(user_id, lang="ru"):
    d = await _flowly_request(user_id, lang)
    if not d or d.get("status") == "ok": return []
    out = []
    for item in d.get("offers", []):
        if not item.get("subscribed") and item.get("link"):
            out.append({"id": item["link"], "link": item["link"], "name": item.get("name", "")})
    return out

async def check_flowly(user_id, lang="ru"):
    d = await _flowly_request(user_id, lang)
    if not d: return None, []
    return d.get("status"), d.get("offers", [])

async def fetch_flyer(user_id, lang="ru"):
    if not flyer: return []
    try:
        tasks = await flyer.get_tasks(user_id=user_id, language_code=lang or "ru", limit=await get_max_sponsors())
        out = []
        for t in (tasks or []):
            if t.get('status') in INCOMPLETE_STATUSES and t.get('links'):
                out.append({"id": t.get('signature'), "link": t['links'][0], "signature": t.get('signature'), "task": t.get('task', '')})
        return out
    except Exception as e:
        await log_error("flyer", "get_tasks", str(e))
    return []

async def check_flyer(signature):
    if not flyer or not signature: return None
    try:
        return await flyer.check_task(signature=signature)
    except Exception as e:
        await log_error("flyer", "check", str(e))
    return None

async def get_brizahub(user_id):
    if not BRIZAHUB_API_KEY: return []
    s = await http()
    limit = max(1, min(await get_max_sponsors(), 10))
    try:
        async with s.post(
            f"{BRIZAHUB_BASE}/v1/tasks",
            json={"user_id": int(user_id), "limit": limit},
            headers={"Authorization": f"Bearer {BRIZAHUB_API_KEY}", "Content-Type": "application/json"}
        ) as r:
            body = await r.text()
            if r.status != 200: return []
            d = json.loads(body)
            out = []
            for t in d.get("tasks") or []:
                tid = t.get("task_id") if t.get("task_id") is not None else t.get("id")
                link = t.get("link") or t.get("url")
                if tid is not None and link:
                    out.append({"id": str(tid), "link": str(link).strip(), "type": t.get("type")})
            return out
    except Exception as e:
        await log_error("brizahub", "fetch", str(e))
    return []

async def check_brizahub(user_id, task_ids):
    if not BRIZAHUB_API_KEY or not task_ids: return set()
    s = await http()
    try:
        norm_ids = [int(x) if str(x).isdigit() else str(x) for x in task_ids]
        async with s.post(
            f"{BRIZAHUB_BASE}/v1/tasks/check",
            json={"user_id": int(user_id), "task_ids": norm_ids},
            headers={"Authorization": f"Bearer {BRIZAHUB_API_KEY}", "Content-Type": "application/json"}
        ) as r:
            if r.status != 200: return set()
            d = await r.json()
            completed = set()
            for item in d.get("results") or []:
                tid = item.get("task_id") if item.get("task_id") is not None else item.get("id")
                if tid is not None and item.get("status") not in INCOMPLETE_STATUSES:
                    completed.add(str(tid))
            return completed
    except Exception as e:
        await log_error("brizahub", "check", str(e))
    return set()

async def get_trafsly(user):
    if not TRAFSLY_API_KEY: return []
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
            if r.status != 200: return []
            d = await r.json()
            out = []
            for x in d.get("sponsors") or []:
                if x.get("status") in ("subscribed", "notgetted", "completed"): continue
                link = str(x.get("link") or "").strip()
                aid = x.get("ads_id")
                if not link or aid is None: continue
                if "api.trafsly.com/go" in link or "/go?token=" in link: continue
                out.append({"id": str(aid), "link": link, "source": x.get("source"), "provider_pool": x.get("provider_pool")})
            return out
    except Exception as e:
        await log_error("trafsly", "fetch", str(e))
    return []

async def check_trafsly(user_id, ads_id, source=None, provider_pool=None) -> bool:
    if not TRAFSLY_API_KEY or not ads_id: return False
    s = await http()
    try:
        payload = {"user_id": int(user_id), "ads_id": int(ads_id) if str(ads_id).isdigit() else ads_id}
        if source: payload["source"] = source
        if provider_pool: payload["provider_pool"] = provider_pool
        async with s.post(
            "https://api.trafsly.com/api/v1/confirm-subscription",
            json=payload,
            headers={"Auth": TRAFSLY_API_KEY, "Content-Type": "application/json"}
        ) as r:
            if r.status != 200: return False
            d = await r.json()
            return d.get("subscribed") is True
    except Exception as e:
        await log_error("trafsly", "check", str(e))
    return False

# ========== СБОР ==========
async def collect_sponsors(user):
    uid = user.id
    lang = getattr(user, "language_code", None) or await get_user_lang(uid) or "ru"
    reward = await get_reward()

    async def _call(name, coro):
        return await coro if await is_service_enabled(name) else []

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
            if x.get("link"): await save_sponsor(uid, "piarflow", x["link"], x["link"], reward)
    if isinstance(flowly, list):
        for x in flowly:
            if x.get("link"): await save_sponsor(uid, "flowly", x["link"], x["link"], reward, task_type=x.get("name"))
    if isinstance(flyer_, list):
        for x in flyer_:
            if x.get("link") and x.get("id"): await save_sponsor(uid, "flyer", str(x["id"]), x["link"], reward, signature=x.get("signature"), task_type=x.get("task"))
    if isinstance(brizahub, list):
        for x in brizahub:
            if x.get("link") and x.get("id") is not None: await save_sponsor(uid, "brizahub", str(x["id"]), x["link"], reward, task_type=x.get("type"))
    if isinstance(trafsly, list):
        for x in trafsly:
            if x.get("link") and x.get("id") is not None:
                await save_sponsor(uid, "trafsly", str(x["id"]), x["link"], reward, signature=x.get("source"), provider_pool=x.get("provider_pool"))

# ========== РЕФЕРАЛЬНОЕ НАЧИСЛЕНИЕ ==========
async def process_referral_reward(user_id: int):
    ref_bonus = await get_ref_bonus()
    ref_subs = await get_ref_subs()
    pool = await get_pool()
    rewarded = None

    async with pool.acquire() as db:
        async with db.transaction():
            rewarded = await db.fetchrow("""
                UPDATE users u SET ref_rewarded=TRUE
                WHERE u.user_id=$1 AND u.referrer_id IS NOT NULL AND u.referrer_id <> u.user_id
                  AND u.ref_rewarded IS NOT TRUE
                  AND (SELECT COUNT(DISTINCT t.link) FROM sponsor_tasks t WHERE t.user_id=u.user_id AND t.status='subscribed') >= $2
                RETURNING u.referrer_id, u.username
            """, user_id, ref_subs)

            if rewarded and ref_bonus > 0:
                await db.execute("UPDATE users SET balance=balance+$1, referred_earned=referred_earned+$1 WHERE user_id=$2", ref_bonus, rewarded["referrer_id"])

    if rewarded and ref_bonus > 0:
        await notify_referrer(rewarded["referrer_id"], rewarded["username"] or "", ref_subs, ref_bonus)

async def _task_reward(user_id, service, aid):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT reward FROM sponsor_tasks WHERE user_id=$1 AND service=$2 AND assignment_id=$3", user_id, service, str(aid))
        return row["reward"] if row else 0.0

async def check_all(user_id, only_service=None):
    async with get_user_lock(user_id):
        tasks = await pending_tasks(user_id)
        if not tasks: return 0, 0.0, 0

        enabled = {s for s in SERVICES_ALL if await is_service_enabled(s) and (not only_service or s == only_service)}
        lang = await get_user_lang(user_id)

        pf, fl, bh, tr = [], [], [], []
        flowly_pending = []
        link_map, task_type_map = {}, {}
        
        for service, aid, link, reward, sig, task_type, pool_ in tasks:
            if service not in enabled: continue
            link_map[aid] = link
            task_type_map[aid] = task_type
            if service == "piarflow" and link: pf.append(link)
            elif service == "flyer" and sig: fl.append((aid, sig))
            elif service == "brizahub" and aid: bh.append(aid)
            elif service == "trafsly" and aid: tr.append((aid, sig, pool_))
            elif service == "flowly": flowly_pending.append((service, aid, link, reward, sig, task_type))

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
            tr_results = await asyncio.gather(*[check_trafsly(user_id, aid, src, pp) for aid, src, pp in tr], return_exceptions=True)
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
                for service, aid, link, reward, sig, tt in flowly_pending:
                    if status == "ok" or link in sub_links:
                        if await mark_subscribed(user_id, "flowly", aid):
                            rw = await _task_reward(user_id, "flowly", aid)
                            await add_balance(user_id, rw)
                            done_count += 1
                            done_sum += rw
                            new_subs.append(("flowly", aid, rw, link, tt))

        if done_count > 0:
            await process_referral_reward(user_id)
        if new_subs:
            await notify_admin_new_subs(user_id, new_subs, source_label="кнопка" if not only_service else f"вебхук {only_service}")
        return done_count, done_sum, skipped_count

# ========== ВОРКЕРЫ И ДИАГНОСТИКА ==========
async def autocheck_worker():
    await asyncio.sleep(60)
    while True:
        try:
            if await get_setting("autocheck", "true") == "true":
                pool = await get_pool()
                async with pool.acquire() as db:
                    rows = await db.fetch("SELECT user_id, username, language_code FROM users WHERE blocked = FALSE AND created_at > $1 LIMIT 500", time.time() - 86400)
                for r in rows:
                    uid = r["user_id"]
                    try:
                        before = await pending_count(uid)
                        user = types.User(id=uid, is_bot=False, first_name="", language_code=r["language_code"] or "ru", username=r["username"] or "")
                        await collect_sponsors(user)
                        after = await pending_count(uid)
                        if after > before:
                            try: await bot.send_message(uid, f"🔔 <b>Новые задания</b>\n\n🎯 Доступно: <b>{after - before}</b>\nОткройте «🚀 Задания».")
                            except Exception: pass
                    except Exception: pass
                    await asyncio.sleep(0.5)
            await asyncio.sleep(AUTOCHECK_INTERVAL)
        except Exception:
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
                    if await pending_count(uid) == 0: continue
                    if time.time() - (r["last_notified"] or 0) < LAZY_INTERVAL: continue
                    if await notify_lazy_user(uid):
                        pool2 = await get_pool()
                        async with pool2.acquire() as db: await db.execute("UPDATE lazy_watch SET last_notified=$1 WHERE user_id=$2", time.time(), uid)
                        if not r["admin_notified"]:
                            mins = int((time.time() - (r["created_at"] or time.time())) / 60)
                            await notify_admin_lazy(uid, total, mins)
                            pool3 = await get_pool()
                            async with pool3.acquire() as db: await db.execute("UPDATE lazy_watch SET admin_notified=TRUE WHERE user_id=$1", uid)
                except Exception: pass
                await asyncio.sleep(0.3)
            await asyncio.sleep(LAZY_INTERVAL)
        except Exception:
            await asyncio.sleep(120)

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
    if len(links) % 2: rows.append(1)
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
    kb.button(text="🔌 Сервисы", callback_data="adm_services")
    kb.button(text="🌐 Вебхуки", callback_data="adm_webhooks")
    kb.adjust(3, 3, 3, 3, 2)
    return kb.as_markup()

def services_menu_kb(enabled_map):
    kb = InlineKeyboardBuilder()
    for s in SERVICES_ALL:
        kb.button(text=f"{'✅' if enabled_map.get(s) else '❌'} {s}", callback_data=f"svc_toggle_{s}")
    kb.button(text="⬅️ Назад", callback_data="diag_back")
    kb.adjust(2, 2, 1, 1)
    return kb.as_markup()

async def issue_sponsors_message(user, chat_id, delete_msg_id=None, clean=True):
    uid = user.id
    if clean: await clear_pending_tasks(uid)
    await collect_sponsors(user)
    all_links = await pending_links(uid, limit=await get_max_sponsors())
    if delete_msg_id:
        try: await bot.delete_message(chat_id, delete_msg_id)
        except Exception: pass
    if not all_links:
        return False, random.choice(["🙂 <b>Пока новых заданий нет.</b>", "🌤 <b>Задания закончились.</b>", "😴 <b>Спонсоры отдыхают.</b>"])
    reward = await get_reward()
    text = (
        f"🎯 <b>Доступно заданий: {len(all_links)}</b>\n\n"
        f"За подписку — <b>{fmt_money(reward)} монет</b>.\n"
        f"Всего: <b>{fmt_money(reward * len(all_links))} монет</b>.\n\n"
        f"1️⃣ Нажмите на кнопки и подпишитесь.\n"
        f"2️⃣ Нажмите «✅ Проверить подписки».\n"
        f"3️⃣ Монеты на балансе.\n\n"
        f"🔇 <i>Совет: выключите уведомления и уберите каналы в Архив.</i>"
    )
    try:
        await bot.send_message(chat_id, text, reply_markup=sponsors_kb(all_links), disable_web_page_preview=True)
    except Exception as e:
        return False, "❌ Ошибка отправки."
    return True, None

# ========== ХЭНДЛЕРЫ БОТА ==========
@dp.message(CommandStart())
async def start_cmd(msg: types.Message):
    args = msg.text.split()
    ref_id = int(args[1].replace("ref", "")) if len(args) > 1 and args[1].startswith("ref") else None
    un = msg.from_user.username or ""
    is_new = await register_user(msg.from_user.id, ref_id, un, msg.from_user.language_code)
    u_check = await get_user(msg.from_user.id)
    if u_check and u_check["blocked"]: return await msg.answer("🚫 <b>Доступ ограничен.</b>")
    await msg.answer("👋 <b>Добро пожаловать!</b>", reply_markup=get_menu(msg.from_user.id))
    if is_new and ref_id: await add_lazy_watch(msg.from_user.id, ref_id)
    try: loading = await msg.answer("⏳ <b>Собираем задания…</b>")
    except Exception: loading = None
    ok, err = await issue_sponsors_message(msg.from_user, msg.chat.id, delete_msg_id=loading.message_id if loading else None)
    if is_new:
        try:
            svc_map = await pending_by_service(msg.from_user.id)
            await notify_admin_new_user(msg.from_user.id, f"@{un}" if un else "", msg.from_user.full_name or "", sum(svc_map.values()), svc_map, ref_id)
        except Exception: pass
    if not ok: await msg.answer(err, reply_markup=get_menu(msg.from_user.id))

@dp.message(Command("admin"))
async def cmd_admin(msg: types.Message):
    if msg.from_user.id == ADMIN_ID: await msg.answer("⚙️ <b>Панель</b>", reply_markup=admin_menu())

@dp.message(F.text == "⚙️ Панель")
async def msg_admin_panel(msg: types.Message):
    if msg.from_user.id == ADMIN_ID: await msg.answer("⚙️ <b>Панель</b>", reply_markup=admin_menu())

@dp.message(F.text == "🚀 Задания")
async def msg_earn(msg: types.Message):
    u = await get_user(msg.from_user.id)
    if u and u["blocked"]: return await msg.answer("🚫 <b>Доступ ограничен.</b>")
    await update_username(msg.from_user.id, msg.from_user.username, msg.from_user.language_code)
    try: loading = await msg.answer("⏳ <b>Собираем…</b>")
    except Exception: loading = None
    ok, err = await issue_sponsors_message(msg.from_user, msg.chat.id, delete_msg_id=loading.message_id if loading else None)
    if not ok: await msg.answer(err, reply_markup=get_menu(msg.from_user.id))

@dp.message(F.text == "👛 Кошелёк")
async def msg_balance(msg: types.Message):
    u = await get_user(msg.from_user.id)
    await msg.answer(f"👛 <b>Кошелёк</b>\n\n💎 Баланс: <b>{fmt_money(u['balance'] if u else 0)} монет</b>\n📥 Минимум: <b>{fmt_money(await get_min_withdraw())} монет</b>", reply_markup=get_menu(msg.from_user.id))

@dp.message(F.text == "📥 Вывод")
async def msg_withdraw(msg: types.Message):
    u = await get_user(msg.from_user.id)
    if u and u["blocked"]: return await msg.answer("🚫 <b>Доступ ограничен.</b>")
    bal = u["balance"] if u else 0
    min_wd = await get_min_withdraw()
    if bal < min_wd: return await msg.answer(f"❌ Минимум: <b>{fmt_money(min_wd)}</b>\n💎 У вас: <b>{fmt_money(bal)}</b>", reply_markup=get_menu(msg.from_user.id))
    _states[msg.from_user.id] = "await_withdraw"
    await msg.answer(f"📥 <b>Вывод в Bytecoin</b>\n\nДоступно: <b>{fmt_money(bal)} монет</b>\nОтправьте сумму (например: <code>1500</code>):", reply_markup=get_menu(msg.from_user.id))

@dp.message(F.text == "🤝 Партнёры")
async def msg_refs(msg: types.Message):
    me = await bot.get_me()
    ref_link = f"https://t.me/{me.username}?start=ref{msg.from_user.id}"
    pool = await get_pool()
    async with pool.acquire() as db:
        stats = await db.fetchrow("SELECT COUNT(*) AS invited, COUNT(*) FILTER (WHERE ref_rewarded IS TRUE) AS rewarded FROM users WHERE referrer_id=$1", msg.from_user.id)
        earned = (await db.fetchrow("SELECT COALESCE(referred_earned, 0) as s FROM users WHERE user_id=$1", msg.from_user.id))["s"]
    await msg.answer(
        f"🤝 <b>Партнёры</b>\n\n💎 Награда: <b>{fmt_money(await get_ref_bonus())} монет</b> за реферала (нужно {await get_ref_subs()} подписок)\n\n"
        f"🔗 <code>{ref_link}</code>\n\n👤 Приглашено: <b>{stats['invited']}</b>\n✅ Засчитано: <b>{stats['rewarded']}</b>\n💰 Заработано: <b>{fmt_money(earned)} монет</b>",
        reply_markup=get_menu(msg.from_user.id)
    )

@dp.message(F.text == "🎟 Бонус-код")
async def msg_promo(msg: types.Message):
    if msg.from_user.id == ADMIN_ID: return
    _states[msg.from_user.id] = "await_promo"
    await msg.answer("🎟 <b>Бонус-код</b>\n\nОтправьте код:", reply_markup=promo_kb())

@dp.callback_query(F.data == "check_subs")
async def cb_check(cq: types.CallbackQuery):
    uid = cq.from_user.id
    if time.time() - _last.get(uid, 0) < CHECK_COOLDOWN: return await cq.answer("⏱ Подождите")
    _last[uid] = time.time()
    await cq.answer("Проверяем…")
    cnt, sm, skipped = await check_all(uid)
    remaining_links = await pending_links(uid, limit=await get_max_sponsors())
    pool = await get_pool()
    async with pool.acquire() as db:
        total_done = (await db.fetchrow("SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status='subscribed'", uid))["c"]
        total_all = (await db.fetchrow("SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1", uid))["c"]
    u = await get_user(uid)
    if len(remaining_links) == 0 and total_all > 0:
        text = f"🎉 <b>Всё выполнено!</b>\n\nВыполнено: <b>{total_done}</b>\n💎 Баланс: <b>{fmt_money(u['balance'] if u else 0)}</b>"
        try: await cq.message.edit_text(text)
        except Exception: await cq.message.answer(text)
        await cq.message.answer("Меню 👇", reply_markup=get_menu(uid))
        return
    text = f"{'✅ <b>Засчитано: ' + str(cnt) + '</b>\n\n💰 +<b>' + fmt_money(sm) + '</b>\n' if cnt > 0 else '❌ <b>Новых подписок нет</b>\n\n'}"
    text += f"💎 Баланс: <b>{fmt_money(u['balance'] if u else 0)}</b>\n📊 {total_done}/{total_all}\n⚠️ Осталось подписаться: <b>{len(remaining_links)}</b>"
    try: await cq.message.edit_text(text, reply_markup=sponsors_kb(remaining_links), disable_web_page_preview=True)
    except Exception as e:
        if "message is not modified" not in str(e).lower(): await cq.message.answer(text, reply_markup=sponsors_kb(remaining_links), disable_web_page_preview=True)

@dp.message(F.text & ~F.text.startswith("/"))
async def text_handler(msg: types.Message):
    uid = msg.from_user.id
    state = _states.get(uid)

    if state == "await_withdraw":
        try: amount = float(msg.text.replace(",", "."))
        except Exception: return await msg.answer("❌ Сумма?")
        min_wd = await get_min_withdraw()
        u = await get_user(uid)
        if amount < min_wd or amount > (u["balance"] if u else 0): return await msg.answer("❌ Ошибка суммы")
        _states.pop(uid, None)
        pool = await get_pool()
        async with pool.acquire() as db:
            async with db.transaction():
                if await db.execute("UPDATE users SET balance = balance - $1 WHERE user_id=$2 AND balance >= $1", amount, uid) == "UPDATE 0": return await msg.answer("❌ Недостаточно")
                wid = (await db.fetchrow("INSERT INTO withdrawals (user_id, amount, status, created_at) VALUES ($1,$2,'pending',$3) RETURNING id", uid, amount, time.time()))["id"]
        await msg.answer(f"⏳ Вывод {fmt_money(amount)} монет обрабатывается…", reply_markup=get_menu(uid))
        resp = await bc_transfer(uid, amount)
        if resp and resp.get("status") == "ok":
            async with pool.acquire() as db: await db.execute("UPDATE withdrawals SET status='done', tx_id=$1 WHERE id=$2", resp.get("transaction_id"), wid)
            await msg.answer(f"✅ Выведено: <b>{fmt_money(amount)}</b>\nTX: <code>{str(resp.get('transaction_id',''))[:16]}…</code>", reply_markup=get_menu(uid))
        else:
            async with pool.acquire() as db:
                await db.execute("UPDATE users SET balance = balance + $1 WHERE user_id=$2", amount, uid)
                await db.execute("UPDATE withdrawals SET status='failed', error=$1 WHERE id=$2", resp.get("error", "") if resp else "network", wid)
            await msg.answer(f"❌ Ошибка: {resp.get('error','unknown') if resp else 'network'}\nСредства возвращены.", reply_markup=get_menu(uid))
        return

    # Admin Set Webhook Secrets
    if state and state.startswith("adm_wh_"):
        svc = state.replace("adm_wh_", "")
        await set_setting(f"wh_secret_{svc}", msg.text.strip())
        _states.pop(uid, None)
        await msg.answer(f"✅ Секрет для {svc} сохранён!", reply_markup=admin_menu())
        return

    if state == "adm_reward":
        try:
            v = float(msg.text.replace(",", "."))
            if v > 0: await set_setting("reward", v)
            _states.pop(uid, None)
            await msg.answer(f"✅ {fmt_money(v)}", reply_markup=admin_menu())
        except Exception: pass
        return

    # Basic text cleanup
    if state: _states.pop(uid, None)

@dp.callback_query(F.data == "adm_webhooks")
async def adm_webhooks(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    await cq.answer()
    secret = await get_webhook_secret()
    
    # Нормализуем базовый URL, убирая двойной webhook, если он есть
    base = WEBHOOK_URL or "https://ВАШ-ДОМЕН"
    if base.endswith("/webhook"): base = base[:-8]

    lines = [
        "🌐 <b>URL-адреса вебхуков</b>\n",
        "Скопируйте URL в настройки кабинета (поддерживаются POST и GET):\n",
    ]
    for svc in SERVICES_ALL:
        lines.append(f"• <b>{svc}</b>:\n<code>{base}/webhook/{svc}/{secret}</code>")
    lines.append("\n💡 <i>Ниже укажите ключи подписи для Flowly, Brizahub и Flyer (если сервис этого требует):</i>")
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🔑 Flowly Key", callback_data="adm_set_wh_flowly")
    kb.button(text="🔑 Brizahub Key", callback_data="adm_set_wh_brizahub")
    kb.button(text="🔑 Flyer Key", callback_data="adm_set_wh_flyer")
    kb.button(text="⬅️ Назад", callback_data="diag_back")
    kb.adjust(1, 1, 1, 1)

    try: await cq.message.edit_text("\n".join(lines), reply_markup=kb.as_markup(), disable_web_page_preview=True)
    except Exception: await cq.message.answer("\n".join(lines), reply_markup=kb.as_markup(), disable_web_page_preview=True)

@dp.callback_query(F.data.startswith("adm_set_wh_"))
async def adm_set_wh(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    svc = cq.data.replace("adm_set_wh_", "")
    _states[cq.from_user.id] = f"adm_wh_{svc}"
    await cq.answer()
    try: await cq.message.edit_text(f"🔑 <b>Настройка {svc}</b>\n\nОтправьте в чат секретный ключ подписи для этого сервиса:")
    except Exception: pass

@dp.callback_query(F.data == "diag_back")
async def cb_diag_back(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    await cq.answer()
    try: await cq.message.edit_text("⚙️ <b>Панель</b>", reply_markup=admin_menu())
    except Exception: pass

@dp.callback_query(F.data.startswith("svc_toggle_"))
async def svc_toggle(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    service = cq.data.replace("svc_toggle_", "")
    cur = await is_service_enabled(service)
    await set_service_enabled(service, not cur)
    await cq.answer(f"{service}: {'выкл' if cur else 'вкл'}")
    enabled_map = {s: await is_service_enabled(s) for s in SERVICES_ALL}
    try: await cq.message.edit_text("🔌 <b>Управление сервисами</b>", reply_markup=services_menu_kb(enabled_map))
    except Exception: pass

# ========== WEBHOOK HANDLERS ==========
def _extract_webhook_token(request: web.Request) -> str:
    if request.match_info.get("secret"): return request.match_info["secret"].strip()
    for qk in ("secret", "token", "key", "auth"):
        if request.query.get(qk): return request.query[qk].strip()
    for hk in ("X-Webhook-Secret", "X-Api-Key", "Auth", "Authorization"):
        val = request.headers.get(hk, "").strip()
        if val: return val[7:].strip() if val.lower().startswith("bearer ") else val
    return ""

async def _parse_request_payload(request: web.Request, raw_body: bytes) -> dict:
    data = {}
    if request.method == "POST" and raw_body:
        try: data.update(json.loads(raw_body))
        except Exception:
            try: data.update(dict(await request.post()))
            except Exception: pass
    for k, v in request.query.items(): data.setdefault(k, v)
    nested = data.get("data")
    if isinstance(nested, dict):
        merged = dict(nested)
        merged.update({k: v for k, v in data.items() if k != "data"})
        return merged
    return data

def _extract_user_id(payload: dict):
    for key in ("user_id", "tg_user_id", "chat_id", "telegram_id", "uid"):
        val = payload.get(key)
        if val is not None and str(val).strip().lstrip("-").isdigit(): return int(str(val).strip())
    return None

def _extract_identifiers(payload: dict) -> set:
    ids = set()
    for key in ("ads_id", "task_id", "id", "offer_id", "signature", "link", "url", "sponsor_link"):
        val = payload.get(key)
        if val is not None and str(val).strip(): ids.add(str(val).strip())
    for list_key in ("links", "task_ids", "offers", "sponsors"):
        arr = payload.get(list_key)
        if isinstance(arr, list):
            for item in arr:
                if isinstance(item, (str, int)): ids.add(str(item).strip())
                elif isinstance(item, dict):
                    for sub_k in ("ads_id", "task_id", "id", "signature", "link", "url"):
                        if item.get(sub_k) is not None: ids.add(str(item[sub_k]).strip())
    return ids

async def webhook_handler(request: web.Request):
    service = request.match_info.get("service", "").lower().strip()
    if service not in SERVICES_ALL:
        return web.json_response({"ok": False, "error": "unknown_service"}, status=404)

    raw_body = await request.read()
    headers = request.headers

    # Обработка GET / Ping / Setup
    if request.method == "GET" and "challenge" in request.query:
        return web.Response(text=request.query["challenge"])
    if not raw_body and request.method == "POST":
        return web.json_response({"ok": True, "status": "pong"})

    # ========== ВАЛИДАЦИЯ ПОДПИСЕЙ (HMAC) ==========
    if service == "flowly":
        secret = await get_setting("wh_secret_flowly", "")
        if secret:
            sig = headers.get("X-Flowly-Signature", "")
            exp = hmac.new(secret.encode(), raw_body, hashlib.sha256).hexdigest()
            if not hmac.compare_digest(sig, exp):
                return web.json_response({"error": "invalid signature"}, status=401)
                
    elif service == "brizahub":
        secret = await get_setting("wh_secret_brizahub", "")
        if secret:
            ts = headers.get("X-Briza-Timestamp", "")
            sig = headers.get("X-Briza-Signature", "")
            if ts and sig:
                payload_str = ts.encode() + b"." + raw_body
                exp = "sha256=" + hmac.new(secret.encode(), payload_str, hashlib.sha256).hexdigest()
                if not hmac.compare_digest(sig, exp):
                    return web.json_response({"error": "invalid signature"}, status=401)

    elif service == "flyer":
        secret = await get_setting("wh_secret_flyer", "")
        if secret:
            sig = headers.get("X-Flyer-Signature", "")
            if sig:
                exp = hmac.new(secret.encode(), raw_body, hashlib.sha256).hexdigest()
                if not hmac.compare_digest(sig, exp):
                    return web.json_response({"error": "invalid signature"}, status=401)

    # Проверка обычного токена в URL для остальных
    else:
        expected_secret = await get_webhook_secret()
        provided_token = _extract_webhook_token(request)
        if provided_token != expected_secret:
            return web.json_response({"error": "unauthorized"}, status=401)

    payload = await _parse_request_payload(request, raw_body)
    user_id = _extract_user_id(payload)
    if not user_id:
        return web.json_response({"ok": True, "status": "pong"})

    if not await is_service_enabled(service):
        return web.json_response({"ok": True, "ignored": "service_disabled"})

    event = str(payload.get("event") or "").lower().strip()
    raw_status = ""
    if event == "task.left": raw_status = "unsubscribed"
    elif event == "task.completed": raw_status = "subscribed"
    elif event == "task.pending": raw_status = "pending"
    else: raw_status = str(payload.get("status") or payload.get("type") or payload.get("action") or event).lower().strip()
    
    sub_flag = payload.get("subscribed")
    if isinstance(sub_flag, str):
        sub_flag = {"true": True, "1": True, "false": False, "0": False}.get(sub_flag.lower())

    identifiers = list(_extract_identifiers(payload))

    # 1. ОБРАБОТКА ОТПИСКИ И СПИСАНИЯ
    if raw_status in UNSUB_STATUSES or sub_flag is False:
        pool = await get_pool()
        async with pool.acquire() as db:
            if identifiers:
                rows = await db.fetch(
                    """SELECT id, reward FROM sponsor_tasks WHERE user_id=$1 AND service=$2 AND status='subscribed'
                       AND (assignment_id = ANY($3::text[]) OR signature = ANY($3::text[]) OR link = ANY($3::text[]))""",
                    user_id, service, identifiers
                )
            else:
                rows = await db.fetch("SELECT id, reward FROM sponsor_tasks WHERE user_id=$1 AND service=$2 AND status='subscribed'", user_id, service)
            
            if rows:
                total_deduct = sum(r["reward"] for r in rows)
                task_ids = [r["id"] for r in rows]
                
                await db.execute("UPDATE sponsor_tasks SET status='unsubscribed' WHERE id = ANY($1::int[])", task_ids)
                await db.execute("UPDATE users SET balance = balance - $1 WHERE user_id=$2", total_deduct, user_id)
                await notify_admin(f"⚠️ <b>Отписка ({service})</b>\n👤 {await get_user_display(user_id)}\nСписано: {fmt_money(total_deduct)}")
                
        return web.json_response({"ok": True, "action": "unsubscribed"})

    # 2. ОБРАБОТКА ПОДПИСКИ И НАЧИСЛЕНИЯ
    if raw_status in SUB_STATUSES or sub_flag is True or not raw_status:
        credited_count, credited_sum, new_subs = 0, 0.0, []
        async with get_user_lock(user_id):
            tasks = await pending_tasks(user_id)
            matched = []
            for svc, aid, link, rw, sig, tt, pp in tasks:
                if svc == service and (not identifiers or aid in identifiers or link in identifiers or (sig and sig in identifiers)):
                    matched.append((aid, link, rw, tt))
            for aid, link, rw, tt in matched:
                if await mark_subscribed(user_id, service, aid):
                    await add_balance(user_id, rw)
                    credited_count += 1
                    credited_sum += rw
                    new_subs.append((service, aid, rw, link or aid, tt))
                    
        if credited_count == 0:
            cnt, sm, _ = await check_all(user_id, only_service=service)
            credited_count += cnt
            credited_sum += sm
        else:
            await process_referral_reward(user_id)
            if new_subs: await notify_admin_new_subs(user_id, new_subs, source_label=f"вебхук {service}")

        if credited_count > 0:
            try:
                u = await get_user(user_id)
                await bot.send_message(user_id, f"⚡ <b>Засчитано автоматически!</b>\n💰 +<b>{fmt_money(credited_sum)} монет</b>\n💎 Баланс: <b>{fmt_money(u['balance'] if u else 0)}</b>\n🔇 <i>Совет: уберите канал в Архив.</i>", reply_markup=get_menu(user_id))
            except Exception: pass
        return web.json_response({"ok": True, "credited": credited_count})

    return web.json_response({"ok": True, "ignored": raw_status})

# ========== ЗАПУСК ==========
async def health(_): return web.Response(text="ok")

async def start_web():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_post("/webhook/{service}", webhook_handler)
    app.router.add_get("/webhook/{service}", webhook_handler)
    app.router.add_post("/webhook/{service}/{secret}", webhook_handler)
    app.router.add_get("/webhook/{service}/{secret}", webhook_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()

async def on_shutdown(*args, **kwargs):
    if _http and not _http.closed: await _http.close()
    if _pool: await _pool.close()

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
