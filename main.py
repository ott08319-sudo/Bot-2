import os
import time
import uuid
import asyncio
import logging
import json
import random
import aiohttp
import asyncpg
from decimal import Decimal
from aiohttp import web
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

try:
    import uvloop
    uvloop.install()
except ImportError:
    pass

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.client.default import DefaultBotProperties

try:
    from flyerapi import Flyer, APIError as FlyerAPIError
    FLYER_AVAILABLE = True
except ImportError:
    FLYER_AVAILABLE = False
    FlyerAPIError = Exception

logging.basicConfig(level=logging.INFO)

# ========== ENV ==========
BOT_TOKEN       = os.getenv("BOT_TOKEN")
ADMIN_ID        = int(os.getenv("ADMIN_ID", "0") or "0")
PORT            = int(os.getenv("PORT", "10000"))
DATABASE_URL    = os.getenv("DATABASE_URL", "")

BC_API_KEY      = os.getenv("BYTECOIN_API_KEY", "")
BC_ENABLED      = os.getenv("BC_ENABLED", "true").lower() == "true"
BC_BASE         = "https://bytecoin.space/api/public/v1"
BC_MIN_TRANSFER = Decimal("0.0000001")

PIARFLOW_API_KEY = os.getenv("PIARFLOW_API_KEY", "")
AXIONNA_API_KEY  = os.getenv("AXIONNA_API_KEY", "")
FLYER_API_KEY    = os.getenv("FLYER_API_KEY", "")
FLOWLY_API_KEY   = os.getenv("FLOWLY_API_KEY", "")
TRAFFY_API_KEY   = os.getenv("TRAFFY_API_KEY", "")
DARKBOOST_API_KEY = os.getenv("DARKBOOST_API_KEY", "")

flyer = Flyer(FLYER_API_KEY) if (FLYER_AVAILABLE and FLYER_API_KEY) else None

# ========== КОНСТАНТЫ ==========
CHECK_COOLDOWN       = 5
DEFAULT_REWARD       = 200.0
DEFAULT_REF_BONUS    = 300.0
DEFAULT_MAX_SPONSORS = 60
MAX_MAX_SPONSORS     = 70
DEFAULT_MIN_WITHDRAW = 500.0
CACHE_TTL            = 300
AUTOCHECK_INTERVAL   = 3600
LAZY_THRESHOLD       = 5
LAZY_INTERVAL        = 300
FLOWLY_TIMEOUT       = 4
INCOMPLETE_STATUSES  = ('incomplete', 'abort')

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

_http = None
_pool = None
_last = {}
_states = {}
BOT_ID = 0

async def http():
    global _http
    if _http is None or _http.closed:
        _http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12))
    return _http

def escape_html(text):
    if not text: return ""
    return (str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))

def fmt_money(value):
    s = f"{value:.2f}".rstrip("0").rstrip(".")
    return s if s else "0"

def clean_db_url(url):
    if not url: return url
    url = url.replace("postgres://", "postgresql://", 1)
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    qs.pop("sslmode", None)
    qs.pop("channel_binding", None)
    if parsed.hostname and "neon.tech" in parsed.hostname:
        qs["ssl"] = ["true"]
    return urlunparse(parsed._replace(query=urlencode(qs, doseq=True)))

async def get_pool():
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(clean_db_url(DATABASE_URL), min_size=1, max_size=5)
    return _pool

# ========== ХЕЛПЕРЫ ==========
def user_display(user_id, username=None):
    if username: return f"@{escape_html(username)} (<code>{user_id}</code>)"
    return f"<code>{user_id}</code>"

async def get_user_display(user_id):
    try:
        pool = await get_pool()
        async with pool.acquire() as db:
            row = await db.fetchrow("SELECT username FROM users WHERE user_id=$1", user_id)
        if row and row["username"]:
            return f"@{escape_html(row['username'])} (<code>{user_id}</code>)"
    except Exception: pass
    return f"<code>{user_id}</code>"

# ========== ЛОГИРОВАНИЕ ==========
async def log_error(service, error_type, message):
    try:
        pool = await get_pool()
        async with pool.acquire() as db:
            recent = await db.fetchrow("SELECT id FROM error_log WHERE service=$1 AND error_type=$2 AND created_at > $3 LIMIT 1", service, error_type, time.time() - 300)
            if recent: return
            await db.execute("INSERT INTO error_log (service, error_type, message, created_at) VALUES ($1, $2, $3, $4)", service, error_type, message[:500], time.time())
    except Exception as e:
        logging.error(f"log_error failed: {e}")

async def notify_admin(text):
    try: await bot.send_message(ADMIN_ID, text, disable_web_page_preview=True)
    except Exception: pass

async def notify_admin_new_subs(user_id, subs_list):
    if not ADMIN_ID or not subs_list: return
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
            header = f"🎯 <b>{service}</b> ({cnt} шт, +{fmt_money(sm)})"
            lines = [header]
            for aid, reward, link, tt in items:
                short_link = str(link) if link else str(aid)
                if len(short_link) > 60: short_link = short_link[:57] + "…"
                label = {"start bot": "➕ Запустить", "subscribe channel": "➕ Подписаться", "give boost": "➕ Голосовать", "follow link": "➕ Перейти", "perform action": "➕ Выполнить"}.get(tt, "")
                if label: lines.append(f"  • {label} — <a href=\"{short_link}\">{short_link}</a>")
                else: lines.append(f"  • <a href=\"{short_link}\">{short_link}</a>")
            blocks.append("\n".join(lines))
        
        ts = time.strftime("%d.%m %H:%M", time.localtime())
        text = f"🆕 <b>Новые подписки!</b>\n\n👤 <b>{user_str}</b>\n\n📦 <b>{len(subs_list)}</b> заданий выполнено:\n\n" + "\n\n".join(blocks) + f"\n\n💰 Итого: <b>+{fmt_money(total)} монет</b>\n🕐 {ts}"
        await bot.send_message(ADMIN_ID, text, disable_web_page_preview=True)
    except Exception as e: logging.error(f"notify_admin_new_subs: {e}")

async def notify_admin_new_user(user_id, username, full_name, sponsors_count, services_map, ref_id=None):
    if not ADMIN_ID: return
    try:
        ts = time.strftime("%d.%m %H:%M", time.localtime())
        svc_lines = "\n".join(f"  • <b>{s}</b> — {c}" for s, c in services_map.items()) if services_map else "  • нет доступных сервисов"
        ref_line = ""
        if ref_id:
            ref_str = await get_user_display(ref_id)
            ref_line = f"\n🔗 Реферер: {ref_str}"
        user_str = user_display(user_id, username.replace("@", "") if username else None)
        text = f"👤 <b>Новый пользователь!</b>\n\n🆔 {user_str}\n📛 <b>{escape_html(full_name) or 'без имени'}</b>{ref_line}\n\n📦 Выдано спонсоров: <b>{sponsors_count}</b>\n🎯 По сервисам:\n{svc_lines}\n\n🕐 {ts}"
        await bot.send_message(ADMIN_ID, text, disable_web_page_preview=True)
    except Exception as e: logging.error(f"notify_admin_new_user: {e}")

async def notify_referrer(referrer_id, ref_username, subs_count, earned):
    try:
        pool = await get_pool()
        async with pool.acquire() as db:
            row = await db.fetchrow("SELECT balance FROM users WHERE user_id=$1", referrer_id)
        bal = row["balance"] if row else 0
        text = f"🎉 <b>Твой реферал подписался!</b>\n\n👤 Реферал: @{escape_html(ref_username or 'без ника')}\n✅ Подписок: <b>{subs_count}</b>\n💎 Ты получаешь: <b>+{fmt_money(earned)} монет</b>\n💰 Твой баланс: <b>{fmt_money(bal)} монет</b>"
        await bot.send_message(referrer_id, text, disable_web_page_preview=True)
    except Exception: pass

# ========== ЛЕНИВЫЕ ==========
LAZY_USER_MSGS = [
    "😡 <b>Эй, ты чего?!</b>\n\nТы пришёл заработать монеты, а сам ничего не сделал?\nТак дело не пойдёт.\n\n🔥 Жми «🎯 Заработать», подпишись на <b>ВСЕХ</b> спонсоров и нажми «✅ Я подписался — проверить». Это 2 минуты.\n\n⚠️ <i>Пока не подпишешься на 5+ каналов — ты НЕ заработаешь ничего.</i>",
    "😤 <b>Слушай, так не работает!</b>\n\nТы нажал /start и думаешь, что монеты упадут сами?\nНет, дружок. Так не бывает.\n\n💎 Зайди в «🎯 Заработать», подпишись на спонсоров, проверь подписки — и получишь свои первые монеты.\n\n🚀 <i>Хватит лениться, начни зарабатывать!</i>",
    "😠 <b>Ты серьёзно?</b>\n\nДругие юзеры уже зарабатывают монеты, а ты даже не подписался ни на одного.\n\n⚠️ <b>Хочешь монеты — работай:</b>\n1️⃣ Жми «🎯 Заработать»\n2️⃣ Подпишись на ВСЕХ\n3️⃣ Жми «✅ Проверить»\n\n💰 <i>Минимум 5 подписок — иначе ничего не получишь.</i>"
]

async def notify_lazy_user(user_id):
    msg = random.choice(LAZY_USER_MSGS)
    try:
        await bot.send_message(user_id, msg, disable_web_page_preview=True)
        return True
    except Exception: return False

async def notify_admin_lazy(user_id, subs, mins):
    user_str = await get_user_display(user_id)
    msg = f"😴 <b>Ленивый юзер</b>\n\n👤 {user_str}\n📊 Подписок: <b>{subs}</b>/{LAZY_THRESHOLD}\n⏱ Прошло: {mins} мин"
    try: await bot.send_message(ADMIN_ID, msg, disable_web_page_preview=True)
    except Exception: pass

# ========== BYTECOIN ==========
def _bc_headers(idem=None):
    h = {"Content-Type": "application/json"}
    if BC_API_KEY.startswith("Bearer "): h["Authorization"] = BC_API_KEY
    else: h["X-API-Key"] = BC_API_KEY
    if idem: h["Idempotency-Key"] = idem
    return h

async def bc_transfer(user_id, amount):
    if not BC_ENABLED: return {"status": "error", "code": "DISABLED", "error": "disabled"}
    amt = Decimal(str(amount)).quantize(Decimal("0.000000001"))
    if amt < BC_MIN_TRANSFER: return {"status": "error", "code": "VALIDATION_ERROR", "error": f"min {BC_MIN_TRANSFER}"}
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
            if attempt == 2: return {"status": "error", "code": "NETWORK", "error": str(e)}
            await asyncio.sleep(2 ** attempt)

# ========== DARKBOOST КЛАСС ==========
class DarkBoostError(Exception):
    def __init__(self, http_status):
        self.http_status = http_status
        super().__init__(f"DarkBoost HTTP {http_status}")

class DarkBoostOP:
    def __init__(self, http_session, pool, bot_id: int):
        self.http = http_session
        self.pool = pool
        self.bot_id = bot_id
        self.key = DARKBOOST_API_KEY

    async def _post(self, path: str, payload: dict) -> dict:
        url = "https://darkboosts.com" + path
        for attempt in range(3):
            try:
                async with self.http.post(url, json=payload, headers={"Auth": self.key, "Content-Type": "application/json"}, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status == 429 or response.status >= 500:
                        retry_after = response.headers.get("Retry-After")
                        if attempt < 2:
                            try: delay = min(float(retry_after), 15)
                            except (TypeError, ValueError): delay = min(2 ** attempt + random.random(), 15)
                            await asyncio.sleep(max(delay, 0))
                            continue
                    if response.status != 200: raise DarkBoostError(response.status)
                    data = await response.json()
            except (aiohttp.ClientError, asyncio.TimeoutError):
                if attempt == 2: raise
                await asyncio.sleep(min(2 ** attempt + random.random(), 15))
                continue
            if not isinstance(data, dict): raise ValueError("DarkBoost returned a non-object response")
            if data.get("status") == "busy" and attempt < 2:
                delay_ms = data.get("retry_after_ms")
                if isinstance(delay_ms, (int, float)) and delay_ms >= 0:
                    await asyncio.sleep(min(delay_ms / 1000, 15))
                    continue
            return data
        raise RuntimeError("DarkBoost retry limit reached")

    async def get_darkboost(self, user_id: int, *, chat_id=None, max_sponsors=10, profile=None) -> dict:
        if not 1 <= max_sponsors <= 10: raise ValueError("max_sponsors must be between 1 and 10")
        chat_id = user_id if chat_id is None else chat_id
        fields = {k: v for k, v in (profile or {}).items() if k in {"username", "language_code", "is_premium"} and v is not None}
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("INSERT INTO darkboost_op(bot_id, user_id, chat_id) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING", self.bot_id, user_id, chat_id)
                row = await conn.fetchrow("SELECT session_id, sponsors FROM darkboost_op WHERE bot_id=$1 AND user_id=$2 AND chat_id=$3 FOR UPDATE", self.bot_id, user_id, chat_id)
                if row["session_id"] is not None: return {"status": "ok", "session_id": row["session_id"], "sponsors": row["sponsors"]}
                data = await self._post("/api/v1/sponsors", {"user_id": user_id, "chat_id": chat_id, "max_sponsors": max_sponsors, **fields})
                if data.get("status") == "ok":
                    sponsors = data.get("sponsors")
                    if not isinstance(sponsors, list) or data.get("session_id") is None: raise ValueError("Invalid DarkBoost sponsors response")
                    await conn.execute("UPDATE darkboost_op SET session_id=$4, sponsors=$5::jsonb WHERE bot_id=$1 AND user_id=$2 AND chat_id=$3", self.bot_id, user_id, chat_id, data["session_id"], json.dumps(sponsors))
                return data

    async def check_darkboost(self, user_id: int, task_ids=None, *, chat_id=None) -> dict:
        chat_id = user_id if chat_id is None else chat_id
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow("SELECT session_id, sponsors FROM darkboost_op WHERE bot_id=$1 AND user_id=$2 AND chat_id=$3 FOR UPDATE", self.bot_id, user_id, chat_id)
                if row is None or row["session_id"] is None: raise ValueError("No saved DarkBoost session")
                data = await self._post("/api/v1/check", {"user_id": user_id, "chat_id": chat_id, "session_id": row["session_id"]})
                if data.get("status") == "expired":
                    await conn.execute("UPDATE darkboost_op SET session_id=NULL, sponsors=NULL WHERE bot_id=$1 AND user_id=$2 AND chat_id=$3", self.bot_id, user_id, chat_id)
                return data

darkboost = None# ========== БД ==========
async def init_db():
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            balance DOUBLE PRECISION DEFAULT 0,
            referrer_id BIGINT,
            referred_earned DOUBLE PRECISION DEFAULT 0,
            blocked BOOLEAN DEFAULT FALSE,
            created_at DOUBLE PRECISION)""")
        await db.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS username TEXT")
        await db.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS referrer_id BIGINT")
        await db.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS referred_earned DOUBLE PRECISION DEFAULT 0")
        await db.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS blocked BOOLEAN DEFAULT FALSE")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users (LOWER(username))")

        await db.execute("""CREATE TABLE IF NOT EXISTS sponsor_tasks (
            id SERIAL PRIMARY KEY,
            user_id BIGINT, service TEXT, assignment_id TEXT, link TEXT,
            reward DOUBLE PRECISION DEFAULT 0, status TEXT DEFAULT 'unsubscribed',
            signature TEXT, created_at DOUBLE PRECISION,
            UNIQUE(user_id, service, assignment_id))""")
        await db.execute("ALTER TABLE sponsor_tasks ADD COLUMN IF NOT EXISTS task_type TEXT")

        await db.execute("""CREATE TABLE IF NOT EXISTS withdrawals (
            id SERIAL PRIMARY KEY,
            user_id BIGINT, amount DOUBLE PRECISION, status TEXT DEFAULT 'pending',
            tx_id TEXT, error TEXT, created_at DOUBLE PRECISION)""")

        await db.execute("""CREATE TABLE IF NOT EXISTS promo_codes (
            code TEXT PRIMARY KEY, amount DOUBLE PRECISION, max_uses INT DEFAULT 1,
            used_count INT DEFAULT 0, active BOOLEAN DEFAULT TRUE, created_at DOUBLE PRECISION)""")

        await db.execute("""CREATE TABLE IF NOT EXISTS promo_uses (
            user_id BIGINT, code TEXT, used_at DOUBLE PRECISION,
            PRIMARY KEY (user_id, code))""")

        await db.execute("""CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY, value TEXT)""")

        await db.execute("""CREATE TABLE IF NOT EXISTS sponsor_cache (
            cache_key TEXT PRIMARY KEY,
            payload TEXT NOT NULL,
            expires_at DOUBLE PRECISION NOT NULL)""")

        await db.execute("""CREATE TABLE IF NOT EXISTS error_log (
            id SERIAL PRIMARY KEY,
            service TEXT, error_type TEXT, message TEXT, created_at DOUBLE PRECISION)""")

        await db.execute("""CREATE TABLE IF NOT EXISTS lazy_watch (
            user_id BIGINT PRIMARY KEY,
            referrer_id BIGINT,
            created_at DOUBLE PRECISION,
            last_notified DOUBLE PRECISION DEFAULT 0,
            admin_notified BOOLEAN DEFAULT FALSE)""")
        await db.execute("ALTER TABLE lazy_watch ADD COLUMN IF NOT EXISTS admin_notified BOOLEAN DEFAULT FALSE")

        # ⚡ ТАБЛИЦА ДЛЯ DARKBOOST
        await db.execute("""CREATE TABLE IF NOT EXISTS darkboost_op (
            bot_id bigint NOT NULL,
            user_id bigint NOT NULL,
            chat_id bigint NOT NULL,
            session_id bigint,
            sponsors jsonb,
            PRIMARY KEY (bot_id, user_id, chat_id))""")

        await db.execute("INSERT INTO settings (key,value) VALUES ('reward',$1) ON CONFLICT (key) DO NOTHING", str(DEFAULT_REWARD))
        await db.execute("INSERT INTO settings (key,value) VALUES ('ref_bonus',$1) ON CONFLICT (key) DO NOTHING", str(DEFAULT_REF_BONUS))
        await db.execute("INSERT INTO settings (key,value) VALUES ('min_withdraw',$1) ON CONFLICT (key) DO NOTHING", str(DEFAULT_MIN_WITHDRAW))
        await db.execute("INSERT INTO settings (key,value) VALUES ('max_sponsors',$1) ON CONFLICT (key) DO NOTHING", str(DEFAULT_MAX_SPONSORS))
        await db.execute("INSERT INTO settings (key,value) VALUES ('autocheck', 'true') ON CONFLICT (key) DO NOTHING")

async def get_setting(key, default=None):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT value FROM settings WHERE key=$1", key)
        return row["value"] if row else default

async def set_setting(key, value):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("INSERT INTO settings (key,value) VALUES ($1,$2) ON CONFLICT (key) DO UPDATE SET value=$2", key, str(value))

async def get_reward():
    return float(await get_setting("reward", DEFAULT_REWARD))

async def get_ref_bonus():
    return float(await get_setting("ref_bonus", DEFAULT_REF_BONUS))

async def get_min_withdraw():
    return float(await get_setting("min_withdraw", DEFAULT_MIN_WITHDRAW))

async def get_max_sponsors():
    return int(await get_setting("max_sponsors", DEFAULT_MAX_SPONSORS))

async def register_user(user_id, referrer_id=None, username=None):
    pool = await get_pool()
    async with pool.acquire() as db:
        exists = await db.fetchrow("SELECT user_id FROM users WHERE user_id=$1", user_id)
        if exists:
            if username:
                await db.execute("UPDATE users SET username=$1 WHERE user_id=$2", username, user_id)
            return False
        if referrer_id == user_id: referrer_id = None
        if referrer_id:
            r = await db.fetchrow("SELECT user_id FROM users WHERE user_id=$1", referrer_id)
            if not r: referrer_id = None
        await db.execute("INSERT INTO users (user_id, username, referrer_id, created_at) VALUES ($1,$2,$3,$4)", user_id, username, referrer_id, time.time())
        return True

async def update_username(user_id, username):
    if not username: return
    try:
        pool = await get_pool()
        async with pool.acquire() as db:
            await db.execute("UPDATE users SET username=$1 WHERE user_id=$2", username, user_id)
    except Exception: pass

async def get_user(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        return await db.fetchrow("SELECT balance, referrer_id, referred_earned, blocked, username FROM users WHERE user_id=$1", user_id)

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

async def add_referred_earned(user_id, amount):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE users SET referred_earned = referred_earned + $1 WHERE user_id=$2", amount, user_id)

async def save_sponsor(user_id, service, aid, link, reward, signature=None, task_type=None):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("""INSERT INTO sponsor_tasks
            (user_id, service, assignment_id, link, reward, signature, task_type, status, created_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,'unsubscribed',$8)
            ON CONFLICT (user_id, service, assignment_id) DO NOTHING""",
            user_id, service, str(aid), link, reward, str(signature) if signature is not None else None, task_type, time.time())

async def mark_subscribed(user_id, service, aid):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE sponsor_tasks SET status='subscribed' WHERE user_id=$1 AND service=$2 AND assignment_id=$3", user_id, service, aid)

async def clear_pending_tasks(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("DELETE FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed'", user_id)
        await db.execute("DELETE FROM sponsor_cache WHERE cache_key=$1", f"all_sponsors_{user_id}")
        # ⚡ СБРОС СЕССИИ DARKBOOST
        await db.execute("UPDATE darkboost_op SET session_id=NULL, sponsors=NULL WHERE bot_id=$1 AND user_id=$2", BOT_ID, user_id)

async def pending_tasks(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch("""SELECT service, assignment_id, link, reward, signature, task_type
            FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed'""", user_id)
        return [(r["service"], r["assignment_id"], r["link"], r["reward"], r["signature"], r["task_type"]) for r in rows]

async def pending_count(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed'", user_id)
        return row["c"]

async def pending_links(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch("""SELECT link FROM sponsor_tasks
            WHERE user_id=$1 AND status!='subscribed' ORDER BY id ASC""", user_id)
        return [r["link"] for r in rows]

async def pending_by_service(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch("""SELECT service, COUNT(*) as c FROM sponsor_tasks
            WHERE user_id=$1 AND status!='subscribed' GROUP BY service""", user_id)
        return {r["service"]: r["c"] for r in rows}

async def count_total_subs(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status='subscribed'", user_id)
        return row["c"]

# ========== LAZY WATCH ==========
async def add_lazy_watch(user_id, referrer_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("""INSERT INTO lazy_watch (user_id, referrer_id, created_at, last_notified, admin_notified)
            VALUES ($1,$2,$3,0,FALSE) ON CONFLICT (user_id) DO NOTHING""", user_id, referrer_id, time.time())

async def remove_lazy_watch(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("DELETE FROM lazy_watch WHERE user_id=$1", user_id)

# ========== КЭШ ==========
async def get_cached_sponsors(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT payload, expires_at FROM sponsor_cache WHERE cache_key=$1", f"all_sponsors_{user_id}")
        if row and row["expires_at"] > time.time():
            try: return json.loads(row["payload"])
            except Exception: return None
    return None

async def set_cached_sponsors(user_id, links):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("""INSERT INTO sponsor_cache (cache_key, payload, expires_at)
            VALUES ($1,$2,$3) ON CONFLICT (cache_key) DO UPDATE SET payload=$2, expires_at=$3""",
            f"all_sponsors_{user_id}", json.dumps(links), time.time() + CACHE_TTL)# ========== PIARFLOW ==========
async def get_piarflow(user_id):
    if not PIARFLOW_API_KEY: return []
    s = await http()
    try:
        async with s.post("https://piarflow.com/v1/sponsors",
            json={"user_id": user_id, "chat_id": user_id, "max_sponsors": await get_max_sponsors()},
            headers={"Authorization": f"Bearer {PIARFLOW_API_KEY}"}) as r:
            d = await r.json()
            if d.get("status") == "ok": return d.get("sponsors", [])
    except Exception as e:
        await log_error("piarflow", "fetch", str(e))
    return []

async def check_piarflow(user_id, links):
    if not PIARFLOW_API_KEY or not links: return []
    s = await http()
    try:
        async with s.post("https://piarflow.com/v1/sponsors/check",
            json={"user_id": user_id, "links": links},
            headers={"Authorization": f"Bearer {PIARFLOW_API_KEY}"}) as r:
            d = await r.json()
            if d.get("status") == "ok": return d.get("sponsors", [])
    except Exception as e:
        await log_error("piarflow", "check", str(e))
    return []

# ========== AXIONNA ==========
async def get_axionna(user_id):
    if not AXIONNA_API_KEY: return []
    s = await http()
    try:
        async with s.post("https://axionna.org/api/sponsors",
            json={"user_id": user_id, "max_sponsors": 20},
            headers={"Authorization": f"Bearer {AXIONNA_API_KEY}", "Content-Type": "application/json"}) as r:
            if r.status != 200: return []
            d = await r.json()
            if d.get("status") == "ok": return d.get("sponsors", [])
    except Exception as e:
        await log_error("axionna", "fetch", str(e))
    return []

async def check_axionna(user_id, task_ids):
    if not AXIONNA_API_KEY or not task_ids: return {}
    s = await http()
    try:
        async with s.post("https://axionna.org/api/check",
            json={"user_id": user_id, "task_ids": task_ids},
            headers={"Authorization": f"Bearer {AXIONNA_API_KEY}", "Content-Type": "application/json"}) as r:
            if r.status != 200: return {}
            d = await r.json()
            if d.get("status") == "ok":
                result = {}
                for x in d.get("results", []):
                    tid = str(x.get("id"))
                    ok = (x.get("subscribed") is True) and (x.get("credited") is True)
                    result[tid] = ok
                return result
    except Exception as e:
        await log_error("axionna", "check", str(e))
    return {}

# ========== FLOWLY ==========
async def _flowly_request(user_id, lang="ru", is_premium=False):
    if not FLOWLY_API_KEY: return None
    try:
        timeout = aiohttp.ClientTimeout(total=FLOWLY_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.post("https://flowly.guru/api/v1/offers",
                headers={"Auth": FLOWLY_API_KEY},
                json={"tg_user_id": user_id, "lang": lang or "ru", "is_premium": bool(is_premium)}
            ) as r:
                if r.status != 200: return None
                return await r.json()
    except asyncio.TimeoutError: return None
    except Exception: return None

async def get_flowly(user_id, lang="ru"):
    d = await _flowly_request(user_id, lang)
    if not d: return []
    if d.get("status") == "ok": return []
    offers = d.get("offers", [])
    out = []
    for item in offers:
        if item.get("subscribed"): continue
        link = item.get("link")
        if not link: continue
        out.append({"id": link, "link": link, "name": item.get("name", "")})
    return out

async def check_flowly(user_id, lang="ru"):
    d = await _flowly_request(user_id, lang)
    if not d: return None, []
    return d.get("status"), d.get("offers", [])

# ========== FLYER ==========
async def fetch_flyer(user_id, lang="ru"):
    if not flyer: return []
    try:
        tasks = await flyer.get_tasks(user_id=user_id, language_code=lang or "ru", limit=await get_max_sponsors())
        if not tasks: return []
        incomplete = [t for t in tasks if t.get('status') in INCOMPLETE_STATUSES]
        out = []
        for t in incomplete:
            links = t.get('links', [])
            if not links: continue
            out.append({
                "id": t.get('signature'),
                "link": links[0],
                "signature": t.get('signature'),
                "task": t.get('task', ''),
            })
        return out
    except FlyerAPIError as e:
        await log_error("flyer", "get_tasks_api", str(e))
    except Exception as e:
        await log_error("flyer", "get_tasks", str(e))
    return []

async def check_flyer(signature):
    if not flyer or not signature: return None
    try:
        return await flyer.check_task(signature=signature)
    except FlyerAPIError as e:
        await log_error("flyer", "check_api", str(e))
    except Exception as e:
        await log_error("flyer", "check", str(e))
    return None

# ========== TRAFFY ==========
async def get_traffy(user_id, first_name="", username="", language_code="ru"):
    if not TRAFFY_API_KEY: return []
    s = await http()
    try:
        payload = {
            "telegram_id": int(user_id),
            "limit": min(await get_max_sponsors(), 10),
            "language_code": language_code or "ru",
        }
        if first_name: payload["first_name"] = first_name
        if username: payload["username"] = username

        async with s.post("https://api.traffy.ai/publisher/tasks",
            json=payload,
            headers={"x-publisher-api-key": TRAFFY_API_KEY, "Content-Type": "application/json"}) as r:
            if r.status not in (200, 201): return []
            d = await r.json()
            if d.get("ok") is True: return d.get("tasks", [])
    except Exception as e:
        await log_error("traffy", "fetch", str(e))
    return []

async def check_traffy(user_id, assignment_ids):
    if not TRAFFY_API_KEY or not assignment_ids: return {}
    s = await http()
    try:
        async with s.post("https://api.traffy.ai/publisher/tasks/check",
            json={"telegram_id": int(user_id), "assignment_ids": assignment_ids},
            headers={"x-publisher-api-key": TRAFFY_API_KEY, "Content-Type": "application/json"}) as r:
            if r.status not in (200, 201): return {}
            d = await r.json()
            if d.get("ok") is True:
                result = {}
                for x in d.get("results", []):
                    aid = str(x.get("assignment_id"))
                    result[aid] = (x.get("status") == "completed")
                return result
    except Exception as e:
        await log_error("traffy", "check", str(e))
    return {}

# ========== СБОР СПОНСОРОВ ==========
async def collect_sponsors(user):
    uid = user.id
    un = user.username or ""
    fn = getattr(user, "first_name", "") or ""
    lang = user.language_code or "ru"
    reward = await get_reward()

    # Piarflow
    for x in await get_piarflow(uid):
        link = x.get("link")
        if link:
            await save_sponsor(uid, "piarflow", link, link, reward)

    # Flowly
    for x in await get_flowly(uid, lang):
        link = x.get("link")
        if link:
            await save_sponsor(uid, "flowly", link, link, reward, task_type=x.get("name"))

    # Flyer
    for x in await fetch_flyer(uid, lang):
        link = x.get("link")
        aid = x.get("id")
        if link and aid:
            await save_sponsor(uid, "flyer", str(aid), link, reward,
                               signature=x.get("signature"),
                               task_type=x.get("task"))

    # Traffy
    traffy_tasks = await get_traffy(uid, fn, un, lang)
    for x in traffy_tasks:
        aid = x.get("assignment_id")
        link = x.get("target_link")
        if aid and link:
            await save_sponsor(uid, "traffy", str(aid), link, reward,
                               task_type=x.get("button_text"))

    # Axionna
    for x in await get_axionna(uid):
        link = x.get("link")
        aid = x.get("id")
        if link and aid:
            await save_sponsor(uid, "axionna", str(aid), link, reward)

    # ⚡ DARKBOOST (с сессиями и получением ссылок)
    if darkboost:
        try:
            profile = {"username": un, "language_code": lang, "is_premium": False}
            res = await darkboost.get_darkboost(uid, profile=profile)
            if res.get("status") == "ok":
                sponsors = res.get("sponsors", [])
                for x in sponsors:
                    link = x.get("link")
                    aid = x.get("id")
                    if link and aid:
                        await save_sponsor(uid, "darkboost", str(aid), link, reward)
        except Exception as e:
            await log_error("darkboost", "collect", str(e))# ========== ПРОВЕРКА + НАЧИСЛЕНИЕ ==========
async def _task_reward(user_id, service, aid):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT reward FROM sponsor_tasks WHERE user_id=$1 AND service=$2 AND assignment_id=$3", user_id, service, aid)
        return row["reward"] if row else 0.0


async def check_all(user_id):
    tasks = await pending_tasks(user_id)
    if not tasks: return 0, 0.0, 0

    pf, ax, fl, tr = [], [], [], []
    flowly_pending = []
    link_map = {}
    task_type_map = {}
    for service, aid, link, reward, sig, task_type in tasks:
        link_map[aid] = link
        task_type_map[aid] = task_type
        if service == "piarflow" and link: pf.append(link)
        elif service == "axionna" and aid: ax.append(aid)
        elif service == "flyer" and sig: fl.append((aid, sig))
        elif service == "traffy" and aid: tr.append(aid)
        elif service == "flowly": flowly_pending.append((service, aid, link, reward, sig, task_type))

    done_count = 0
    done_sum = 0.0
    skipped_count = 0
    new_subs = []

    if pf:
        for r in await check_piarflow(user_id, pf):
            st = r.get("status")
            if st == "subscribed":
                await mark_subscribed(user_id, "piarflow", r.get("link"))
                rw = await _task_reward(user_id, "piarflow", r.get("link"))
                await add_balance(user_id, rw)
                done_count += 1; done_sum += rw
                new_subs.append(("piarflow", r.get("link"), rw, r.get("link"), None))
            elif st == "not_counted":
                skipped_count += 1

    if ax:
        res = await check_axionna(user_id, ax)
        for aid, ok in res.items():
            if ok:
                await mark_subscribed(user_id, "axionna", aid)
                rw = await _task_reward(user_id, "axionna", aid)
                await add_balance(user_id, rw)
                done_count += 1; done_sum += rw
                new_subs.append(("axionna", aid, rw, link_map.get(aid), None))
            else:
                skipped_count += 1

    for aid, sig in fl:
        status = await check_flyer(sig)
        if status == "complete":
            await mark_subscribed(user_id, "flyer", aid)
            rw = await _task_reward(user_id, "flyer", aid)
            await add_balance(user_id, rw)
            done_count += 1; done_sum += rw
            new_subs.append(("flyer", aid, rw, link_map.get(aid), task_type_map.get(aid)))

    if tr:
        res = await check_traffy(user_id, tr)
        for aid, ok in res.items():
            if ok:
                await mark_subscribed(user_id, "traffy", aid)
                rw = await _task_reward(user_id, "traffy", aid)
                await add_balance(user_id, rw)
                done_count += 1; done_sum += rw
                new_subs.append(("traffy", aid, rw, link_map.get(aid), task_type_map.get(aid)))
            else:
                skipped_count += 1

    if flowly_pending:
        status, _ = await check_flowly(user_id)
        if status == "ok":
            total_rw = 0.0
            for service, aid, link, reward, sig, tt in flowly_pending:
                await mark_subscribed(user_id, "flowly", aid)
                rw = await _task_reward(user_id, "flowly", aid)
                total_rw += rw
                done_count += 1; done_sum += rw
                new_subs.append(("flowly", aid, rw, link, tt))
            if total_rw > 0:
                await add_balance(user_id, total_rw)

    # ⚡ DARKBOOST (проверка через сессию)
    if darkboost:
        try:
            res = await darkboost.check_darkboost(user_id)
            if res.get("status") == "ok":
                completed_ids = [str(x) for x in res.get("completed", [])]
                for aid in completed_ids:
                    await mark_subscribed(user_id, "darkboost", aid)
                    rw = await _task_reward(user_id, "darkboost", aid)
                    await add_balance(user_id, rw)
                    done_count += 1; done_sum += rw
                    new_subs.append(("darkboost", aid, rw, link_map.get(aid), None))
            elif res.get("status") == "not_ok":
                missing = res.get("missing", [])
                if missing:
                    skipped_count += len(missing)
        except Exception as e:
            await log_error("darkboost", "check", str(e))

    # ⚡ РЕФЕРАЛЬНЫЕ НАЧИСЛЕНИЯ (в монетах)
    if done_count > 0:
        pool = await get_pool()
        async with pool.acquire() as db:
            row = await db.fetchrow("SELECT referrer_id, username FROM users WHERE user_id=$1", user_id)
        if row and row["referrer_id"] and row["referrer_id"] != user_id:
            ref_bonus_per_sub = await get_ref_bonus()
            ref_total = ref_bonus_per_sub * done_count
            if ref_total > 0:
                await add_balance(row["referrer_id"], ref_total)
                await add_referred_earned(row["referrer_id"], ref_total)
                await notify_referrer(row["referrer_id"], row["username"] or "", done_count, ref_total)

    if new_subs:
        await notify_admin_new_subs(user_id, new_subs)

    return done_count, done_sum, skipped_count


# ========== АВТОПРОВЕРКА ==========
async def autocheck_worker():
    await asyncio.sleep(300)
    while True:
        try:
            enabled = await get_setting("autocheck", "true")
            if enabled == "true":
                pool = await get_pool()
                async with pool.acquire() as db:
                    rows = await db.fetch("""
                        SELECT user_id, username FROM users
                        WHERE blocked = FALSE AND created_at > $1 LIMIT 500
                    """, time.time() - 86400)

                for r in rows:
                    uid = r["user_id"]
                    try:
                        try:
                            tg = await bot.get_chat(uid)
                            if tg.username: await update_username(uid, tg.username)
                        except Exception: pass

                        before = await pending_count(uid)
                        user = types.User(id=uid, is_bot=False, first_name="", language_code="ru", username=r["username"] or "")
                        await collect_sponsors(user)
                        after = await pending_count(uid)
                        if after > before:
                            new_count = after - before
                            try:
                                await bot.send_message(uid,
                                    f"🔔 <b>Новые задания ждут тебя!</b>\n\n"
                                    f"🎯 Доступно: <b>{new_count}</b>\n"
                                    f"Нажми «🎯 Заработать», чтобы забрать награду.")
                            except Exception: pass
                    except Exception as e:
                        logging.error(f"autocheck {uid}: {e}")
                    await asyncio.sleep(0.5)
            await asyncio.sleep(AUTOCHECK_INTERVAL)
        except Exception as e:
            logging.error(f"autocheck_worker: {e}")
            await asyncio.sleep(600)


# ========== ЛЕНИВЫЙ ВОРКЕР ==========
async def lazy_worker():
    await asyncio.sleep(60)
    while True:
        try:
            pool = await get_pool()
            async with pool.acquire() as db:
                rows = await db.fetch("""
                    SELECT user_id, referrer_id, created_at, last_notified, admin_notified
                    FROM lazy_watch""")

            for r in rows:
                uid = r["user_id"]
                try:
                    total = await count_total_subs(uid)
                    if total >= LAZY_THRESHOLD:
                        await remove_lazy_watch(uid)
                        continue

                    pend = await pending_count(uid)
                    if pend == 0: continue

                    if time.time() - (r["last_notified"] or 0) < LAZY_INTERVAL: continue

                    sent = await notify_lazy_user(uid)
                    if sent:
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
    if not key: return f"❌ <b>{name}</b> — ключ не задан"
    t0 = time.time()
    try:
        res = await checker()
        dt = int((time.time() - t0) * 1000)
        if res.get("error"): return f"❌ <b>{name}</b> — {res['error']} ({dt}мс)"
        cnt = res.get("count", 0)
        extra = res.get("extra", "")
        if dt > 3000: extra += f" ⚠️ медленно"
        if cnt > 0: return f"✅ <b>{name}</b> — {cnt} заданий ({dt}мс){extra}"
        return f"🟡 <b>{name}</b> — 0 заданий ({dt}мс){extra}"
    except asyncio.TimeoutError:
        dt = int((time.time() - t0) * 1000)
        return f"❌ <b>{name}</b> — timeout ({dt}мс)"
    except Exception as e:
        dt = int((time.time() - t0) * 1000)
        return f"❌ <b>{name}</b> — {escape_html(str(e)[:100])} ({dt}мс)"

async def _diag_piarflow():
    s = await http()
    async with s.post("https://piarflow.com/v1/sponsors", json={"user_id": ADMIN_ID, "chat_id": ADMIN_ID, "max_sponsors": 5}, headers={"Authorization": f"Bearer {PIARFLOW_API_KEY}"}) as r:
        body = await r.text()
        if r.status == 404: return {"count": 0, "extra": " (нет офферов)"}
        if r.status != 200: return {"error": f"HTTP {r.status}", "count": 0}
        d = json.loads(body)
        if d.get("status") == "ok": return {"count": len(d.get("sponsors", []))}
        return {"error": d.get("message", "unknown"), "count": 0}

async def _diag_axionna():
    s = await http()
    async with s.post("https://axionna.org/api/sponsors", json={"user_id": ADMIN_ID, "max_sponsors": 5}, headers={"Authorization": f"Bearer {AXIONNA_API_KEY}", "Content-Type": "application/json"}) as r:
        body = await r.text()
        if r.status != 200: return {"error": f"HTTP {r.status}: {body[:100]}", "count": 0}
        d = json.loads(body)
        if d.get("registration_required"): return {"error": "registration_required", "count": 0}
        if d.get("status") == "ok": return {"count": len(d.get("sponsors", []))}
        return {"error": f"status={d.get('status')}", "count": 0}

async def _diag_flowly():
    if not FLOWLY_API_KEY: return {"error": "ключ не задан", "count": 0}
    try:
        timeout = aiohttp.ClientTimeout(total=FLOWLY_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.post("https://flowly.guru/api/v1/offers", headers={"Auth": FLOWLY_API_KEY}, json={"tg_user_id": ADMIN_ID, "lang": "ru", "is_premium": False}) as r:
                if r.status != 200: return {"error": f"HTTP {r.status}", "count": 0}
                d = await r.json()
                if d.get("status") == "ok": return {"count": 0, "extra": " (всё выполнено)"}
                if d.get("status") == "no_offers": return {"count": 0, "extra": " (нет офферов)"}
                offers = d.get("offers", [])
                return {"count": sum(1 for o in offers if not o.get("subscribed"))}
    except asyncio.TimeoutError: return {"error": f"timeout >{FLOWLY_TIMEOUT}s", "count": 0}

async def _diag_flyer():
    if not flyer: return {"error": "flyerapi не подключён", "count": 0}
    tasks = await flyer.get_tasks(user_id=ADMIN_ID, language_code="ru", limit=5)
    if not tasks: return {"count": 0}
    incomplete = [t for t in tasks if t.get('status') in INCOMPLETE_STATUSES]
    return {"count": len(incomplete)}

async def _diag_traffy():
    s = await http()
    async with s.get("https://api.traffy.ai/publisher/me", headers={"x-publisher-api-key": TRAFFY_API_KEY}) as r:
        body = await r.text()
        if r.status == 401: return {"error": "HTTP 401 (бот не прошёл модерацию)", "count": 0}
        if r.status != 200: return {"error": f"HTTP {r.status}: {body[:100]}", "count": 0}
        d = json.loads(body)
        res = d.get("resource", {})
        api_enabled = res.get("api_enabled", False)
        if api_enabled: return {"count": 1, "extra": " (api активен)"}
        return {"error": "api_enabled=false", "count": 0}

async def _diag_darkboost():
    if not darkboost: return {"error": "ключ не задан", "count": 0}
    try:
        # Просто пробуем запросить спонсоров для админа
        res = await darkboost.get_darkboost(ADMIN_ID, max_sponsors=5)
        if res.get("status") == "ok":
            return {"count": len(res.get("sponsors", []))}
        return {"error": f"status={res.get('status')}", "count": 0}
    except Exception as e:
        return {"error": str(e)[:100], "count": 0}

async def run_diagnostics():
    results = await asyncio.gather(
        diagnose_service("Piarflow", PIARFLOW_API_KEY, _diag_piarflow),
        diagnose_service("Flowly", FLOWLY_API_KEY, _diag_flowly),
        diagnose_service("Flyer", FLYER_API_KEY, _diag_flyer),
        diagnose_service("Traffy", TRAFFY_API_KEY, _diag_traffy),
        diagnose_service("Axionna", AXIONNA_API_KEY, _diag_axionna),
        diagnose_service("DarkBoost", DARKBOOST_API_KEY, _diag_darkboost),
    )
    header = f"🔎 <b>Диагностика сервисов</b>\n🆔 user_id: <code>{ADMIN_ID}</code>\n\n"
    return header + "\n".join(results)

async def get_recent_errors(hours=3):
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch("""
            SELECT service, error_type, message, created_at
            FROM error_log WHERE created_at > $1
            ORDER BY created_at DESC LIMIT 30""", time.time() - hours * 3600)
    return rows

async def get_top_by_balance(limit=15):
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch("""
            SELECT user_id, username, balance FROM users WHERE blocked = FALSE
            ORDER BY balance DESC LIMIT $1""", limit)
    return rows

async def get_top_by_refs(limit=15):
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch("""
            SELECT u.user_id, u.username,
                   (SELECT COUNT(*) FROM users r WHERE r.referrer_id = u.user_id) AS refs_count
            FROM users u WHERE u.blocked = FALSE
            ORDER BY refs_count DESC, u.user_id ASC LIMIT $1""", limit)
    return rows# ========== КЛАВИАТУРЫ ==========
def user_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🎯 Заработать"), KeyboardButton(text="💰 Баланс")],
            [KeyboardButton(text="💸 Вывести"), KeyboardButton(text="👥 Рефералы")],
            [KeyboardButton(text="🎁 Промокод")],
        ], resize_keyboard=True, is_persistent=True
    )

def admin_reply_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🎯 Заработать"), KeyboardButton(text="💰 Баланс")],
            [KeyboardButton(text="💸 Вывести"), KeyboardButton(text="👥 Рефералы")],
            [KeyboardButton(text="🛠 Админка")],
        ], resize_keyboard=True, is_persistent=True
    )

def get_menu(user_id):
    return admin_reply_menu() if user_id == ADMIN_ID else user_menu()

def sponsors_kb(links):
    kb = InlineKeyboardBuilder()
    for i, link in enumerate(links, 1):
        kb.button(text=f"🔗 {i}", url=link)
    kb.button(text="✅ Я подписался — проверить", callback_data="check_subs")
    kb.button(text="🔄 Обновить список", callback_data="rebuild_subs")
    rows = [2] * (len(links) // 2)
    if len(links) % 2: rows.append(1)
    rows.append(2)
    kb.adjust(*rows)
    return kb.as_markup()

def promo_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ Назад", callback_data="promo_cancel")
    return kb.as_markup()

def admin_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="💰 Награда", callback_data="adm_reward")
    kb.button(text="👥 Макс спонсоров", callback_data="adm_max")
    kb.button(text="💸 Заявки", callback_data="adm_withdraws")
    kb.button(text="📊 Статистика", callback_data="adm_stats")
    kb.button(text="🏆 Топ игроков", callback_data="adm_top")
    kb.button(text="🔎 Найти юзера", callback_data="adm_find")
    kb.button(text="💎 Начислить", callback_data="adm_give")
    kb.button(text="🎁 Создать промокод", callback_data="adm_promo_create")
    kb.button(text="📢 Рассылка", callback_data="adm_broadcast")
    kb.button(text="💸 Мин. вывод", callback_data="adm_minw")
    kb.button(text="👥 Реф-бонус", callback_data="adm_refb")
    kb.button(text="📊 Диагностика", callback_data="adm_diag")
    kb.adjust(3, 3, 3, 3)
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
    kb.button(text="💰 По балансу", callback_data="top_balance")
    kb.button(text="👥 По рефералам", callback_data="top_refs")
    kb.button(text="⬅️ Назад", callback_data="top_back")
    kb.adjust(2, 1)
    return kb.as_markup()

# ========== ВЫДАЧА СПОНСОРОВ ==========
async def issue_sponsors_message(user, chat_id, delete_msg_id=None, clean=True):
    uid = user.id
    if clean:
        await clear_pending_tasks(uid)

    cached = await get_cached_sponsors(uid)
    if cached:
        all_links = cached
    else:
        await collect_sponsors(user)
        all_links = await pending_links(uid)
        max_sp = await get_max_sponsors()
        all_links = all_links[:max_sp]
        if all_links:
            await set_cached_sponsors(uid, all_links)

    if not all_links:
        return False, ("😕 <b>Заданий пока нет, но ты молодец!</b>\n\nТы уже забрал всё, что было. Загляни через 15-20 минут — мы постоянно добавляем новых спонсоров 💎")

    reward = await get_reward()
    total_reward = reward * len(all_links)
    min_wd = await get_min_withdraw()

    text = (
        f"🎯 <b>Тебе доступно {len(all_links)} заданий!</b>\n\n"
        f"💎 Это твой шанс заработать <b>{fmt_money(total_reward)} монет</b> за 1 минуту!\n\n"
        f"━━━━━━━━━━━━━━━\n"
        f"💰 За каждого спонсора: <b>{fmt_money(reward)} монет</b>\n"
        f"⚡ Награда сразу на баланс\n"
        f"📤 Минимум вывода: <b>{fmt_money(min_wd)} монет</b>\n"
        f"━━━━━━━━━━━━━━━\n\n"
        f"🚀 <b>Как забрать монеты:</b>\n"
        f"1️⃣ Жми на <b>каждую кнопку</b> ниже и подписывайся\n"
        f"2️⃣ Вернись и нажми <b>«✅ Я подписался — проверить»</b>\n"
        f"3️⃣ Получи монеты сразу на баланс 🎉\n\n"
        f"💡 <i>Чем больше спонсоров — тем больше монет! Подпишись на ВСЕХ, не пропускай ни одного 👇</i>"
    )

    if delete_msg_id:
        try: await bot.delete_message(chat_id, delete_msg_id)
        except Exception: pass

    try:
        await bot.send_message(chat_id, text, reply_markup=sponsors_kb(all_links), disable_web_page_preview=True)
    except Exception as e:
        await log_error("bot", "send_sponsors", str(e))
        return False, "❌ Ошибка отправки. Попробуй позже."
    return True, None

# ========== START ==========
@dp.message(CommandStart())
async def start_cmd(msg: types.Message):
    args = msg.text.split()
    ref_id = None
    if len(args) > 1 and args[1].startswith("ref"):
        try: ref_id = int(args[1].replace("ref", ""))
        except Exception: pass

    un = msg.from_user.username or ""
    is_new = await register_user(msg.from_user.id, ref_id, un)

    u_check = await get_user(msg.from_user.id)
    if u_check and u_check["blocked"]:
        await msg.answer("🚫 <b>Ты заблокирован.</b>\nОбратись к админу.")
        return

    if is_new and ref_id:
        await add_lazy_watch(msg.from_user.id, ref_id)

    try: loading = await msg.answer("⏳ <b>Собираю спонсоров…</b>\n\n<i>Это займёт 5-10 секунд</i>")
    except Exception: loading = None

    ok, err = await issue_sponsors_message(msg.from_user, msg.chat.id, delete_msg_id=loading.message_id if loading else None)

    if is_new:
        try:
            services_map = await pending_by_service(msg.from_user.id)
            total_sp = sum(services_map.values())
            await notify_admin_new_user(
                user_id=msg.from_user.id,
                username=f"@{un}" if un else "",
                full_name=msg.from_user.full_name or "",
                sponsors_count=total_sp,
                services_map=services_map,
                ref_id=ref_id,
            )
        except Exception as e: logging.error(f"notify new user failed: {e}")

    if not ok:
        await msg.answer(err, reply_markup=get_menu(msg.from_user.id))

@dp.message(Command("admin"))
async def cmd_admin(msg: types.Message):
    if msg.from_user.id != ADMIN_ID: return
    await msg.answer("🛠 <b>Админ-панель</b>\n\nВыбери действие:", reply_markup=admin_menu())

@dp.message(F.text == "🛠 Админка")
async def msg_admin_panel(msg: types.Message):
    if msg.from_user.id != ADMIN_ID: return
    await msg.answer("🛠 <b>Админ-панель</b>\n\nВыбери действие:", reply_markup=admin_menu())

# ========== REPLY BUTTONS ==========
@dp.message(F.text == "🎯 Заработать")
async def msg_earn(msg: types.Message):
    await update_username(msg.from_user.id, msg.from_user.username)
    try: loading = await msg.answer("⏳ <b>Собираю спонсоров…</b>\n\n<i>Это займёт 5-10 секунд</i>")
    except Exception: loading = None

    ok, err = await issue_sponsors_message(msg.from_user, msg.chat.id, delete_msg_id=loading.message_id if loading else None)
    if not ok:
        await msg.answer(err, reply_markup=get_menu(msg.from_user.id))

@dp.message(F.text == "💰 Баланс")
async def msg_balance(msg: types.Message):
    u = await get_user(msg.from_user.id)
    bal = u["balance"] if u else 0
    min_wd = await get_min_withdraw()
    await msg.answer(
        f"💰 <b>Твой баланс</b>\n\n"
        f"━━━━━━━━━━━━━━━\n"
        f"💎 Доступно: <b>{fmt_money(bal)} монет</b>\n"
        f"📤 Минимум вывода: <b>{fmt_money(min_wd)} монет</b>\n"
        f"━━━━━━━━━━━━━━━",
        reply_markup=get_menu(msg.from_user.id))

@dp.message(F.text == "💸 Вывести")
async def msg_withdraw(msg: types.Message):
    u = await get_user(msg.from_user.id)
    bal = u["balance"] if u else 0
    min_wd = await get_min_withdraw()
    if bal < min_wd:
        await msg.answer(
            f"❌ <b>Недостаточно монет</b>\n\n"
            f"📤 Минимум: <b>{fmt_money(min_wd)}</b>\n"
            f"💰 У тебя: <b>{fmt_money(bal)}</b>",
            reply_markup=get_menu(msg.from_user.id))
        return
    _states[msg.from_user.id] = "await_withdraw"
    await msg.answer(
        f"💸 <b>Вывод в Bytecoin</b>\n\n"
        f"💰 Доступно: <b>{fmt_money(bal)} монет</b>\n\n"
        f"Отправь сумму одним сообщением:\n"
        f"Например: <code>1500</code>",
        reply_markup=get_menu(msg.from_user.id))

@dp.message(F.text == "👥 Рефералы")
async def msg_refs(msg: types.Message):
    me = await bot.get_me()
    ref_link = f"https://t.me/{me.username}?start=ref{msg.from_user.id}"
    pool = await get_pool()
    async with pool.acquire() as db:
        refs_count = (await db.fetchrow("SELECT COUNT(*) as c FROM users WHERE referrer_id=$1", msg.from_user.id))["c"]
        earned = (await db.fetchrow("SELECT COALESCE(referred_earned, 0) as s FROM users WHERE user_id=$1", msg.from_user.id))["s"]

    ref_bonus = await get_ref_bonus()
    text = (
        f"👥 <b>Реферальная система</b>\n\n"
        f"━━━━━━━━━━━━━━━\n"
        f"💎 Бонус: <b>{fmt_money(ref_bonus)} монет</b> за каждого спонсора реферала\n"
        f"━━━━━━━━━━━━━━━\n\n"
        f"🔗 Твоя ссылка:\n"
        f"<code>{ref_link}</code>\n\n"
        f"👤 Приглашено: <b>{refs_count}</b>\n"
        f"💰 Заработано: <b>{fmt_money(earned)} монет</b>"
    )
    await msg.answer(text, reply_markup=get_menu(msg.from_user.id))

@dp.message(F.text == "🎁 Промокод")
async def msg_promo(msg: types.Message):
    if msg.from_user.id == ADMIN_ID: return
    _states[msg.from_user.id] = "await_promo"
    await msg.answer("🎁 <b>Активация промокода</b>\n\nОтправь код одним сообщением:", reply_markup=promo_kb())

@dp.callback_query(F.data == "promo_cancel")
async def cb_promo_cancel(cq: types.CallbackQuery):
    _states.pop(cq.from_user.id, None)
    u = await get_user(cq.from_user.id)
    bal = u["balance"] if u else 0
    await cq.answer("Отменено")
    try: await cq.message.edit_text("Отменено.")
    except Exception: pass
    await cq.message.answer(f"💰 Баланс: <b>{fmt_money(bal)} монет</b>", reply_markup=get_menu(cq.from_user.id))

# ========== ПРОВЕРКА ПОДПИСОК ==========
@dp.callback_query(F.data == "check_subs")
async def cb_check(cq: types.CallbackQuery):
    uid = cq.from_user.id
    if time.time() - _last.get(uid, 0) < CHECK_COOLDOWN:
        await cq.answer("⏱ Подожди пару секунд")
        return
    _last[uid] = time.time()
    await cq.answer("Проверяю…")

    cnt, sm, skipped = await check_all(uid)
    left = await pending_count(uid)

    pool = await get_pool()
    async with pool.acquire() as db:
        total_done = (await db.fetchrow("SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status='subscribed'", uid))["c"]
        total_all = (await db.fetchrow("SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1", uid))["c"]

    u = await get_user(uid)
    bal = u["balance"] if u else 0
    min_wd = await get_min_withdraw()
    reward = await get_reward()

    if left == 0 and total_all > 0:
        text = (
            f"🎉 <b>Ты просто космос!</b>\n\n"
            f"Все <b>{total_done}</b> заданий выполнены — ты легенда 💎\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"💰 Баланс: <b>{fmt_money(bal)} монет</b>\n"
            f"📤 Минимум вывода: <b>{fmt_money(min_wd)} монет</b>\n"
            f"━━━━━━━━━━━━━━━\n\n"
            f"<i>Заглядывай позже — мы добавляем новых спонсоров! 🚀</i>"
        )
        try: await cq.message.edit_text(text)
        except Exception: await cq.message.answer(text)
        await cq.message.answer("Меню 👇", reply_markup=get_menu(uid))
        return

    if cnt > 0 and skipped == 0:
        text = (
            f"✅ <b>Красавчик! Засчитано: {cnt}</b>\n\n"
            f"💰 Начислено: <b>+{fmt_money(sm)} монет</b>\n"
            f"💎 Баланс: <b>{fmt_money(bal)} монет</b>\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"📊 Прогресс: <b>{total_done} / {total_all}</b>\n"
            f"━━━━━━━━━━━━━━━\n\n"
            f"⚠️ Осталось: <b>{left}</b>\n"
            f"💰 За каждого ещё: <b>{fmt_money(reward)} монет</b>\n\n"
            f"🔥 <i>Не останавливайся! Ты почти у цели.</i>"
        )
    elif cnt > 0 and skipped > 0:
        text = (
            f"📊 <b>Результат проверки</b>\n\n"
            f"✅ Засчитано: <b>{cnt}</b>\n"
            f"🚫 Отсеяно: <b>{skipped}</b>\n"
            f"💰 Начислено: <b>+{fmt_money(sm)} монет</b>\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"📊 Прогресс: <b>{total_done} / {total_all}</b>\n"
            f"━━━━━━━━━━━━━━━\n\n"
            f"💡 <i>Часть подписок не засчитана — сервис отсеял их (антифрод). Это не твоя вина. Попробуй позже.</i>\n\n"
            f"⚠️ Осталось: <b>{left}</b>"
        )
    elif skipped > 0 and cnt == 0:
        text = (
            f"⚠️ <b>Подписки не засчитаны</b>\n\n"
            f"🚫 Отсеяно: <b>{skipped}</b>\n\n"
            f"💡 <i>Сервис не оплатил эти подписки (антифрод). Это не твоя вина — просто попробуй позже.</i>\n\n"
            f"📊 Прогресс: <b>{total_done} / {total_all}</b>\n"
            f"⚠️ Осталось: <b>{left}</b>"
        )
    else:
        text = (
            f"❌ <b>Новых подписок не найдено</b>\n\n"
            f"📊 Прогресс: <b>{total_done} / {total_all}</b>\n\n"
            f"⚠️ Осталось: <b>{left}</b> спонсоров\n\n"
            f"<i>💡 Подпишись на ВСЕ кнопки, что были выше, потом жми проверку. Если подписался — подожди 5-10 минут.</i>"
        )

    kb = InlineKeyboardBuilder()
    kb.button(text="🔄 Обновить список", callback_data="rebuild_subs")
    kb.button(text="💰 Баланс", callback_data="balance_inline")
    kb.adjust(1)

    try: await cq.message.edit_text(text, reply_markup=kb.as_markup())
    except Exception: await cq.message.answer(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "rebuild_subs")
async def cb_rebuild(cq: types.CallbackQuery):
    uid = cq.from_user.id
    await cq.answer("Проверяю и пересобираю…")
    await check_all(uid)
    try: await cq.message.delete()
    except Exception: pass
    ok, err = await issue_sponsors_message(cq.from_user, cq.message.chat.id, clean=True)
    if not ok:
        await cq.message.answer(err, reply_markup=get_menu(uid))

@dp.callback_query(F.data == "balance_inline")
async def cb_balance_inline(cq: types.CallbackQuery):
    u = await get_user(cq.from_user.id)
    bal = u["balance"] if u else 0
    min_wd = await get_min_withdraw()
    await cq.answer()
    await cq.message.answer(
        f"💰 Баланс: <b>{fmt_money(bal)} монет</b>\n"
        f"📤 Минимум: <b>{fmt_money(min_wd)} монет</b>",
        reply_markup=get_menu(cq.from_user.id))

# ========== ТЕКСТОВЫЙ ХЕНДЛЕР ==========
@dp.message(F.text & ~F.text.startswith("/"))
async def text_handler(msg: types.Message):
    uid = msg.from_user.id
    state = _states.get(uid)

    if state == "await_promo":
        code = msg.text.strip()
        pool = await get_pool()
        async with pool.acquire() as db:
            promo = await db.fetchrow("SELECT * FROM promo_codes WHERE code=$1 AND active=TRUE", code)
            if not promo:
                await msg.answer("❌ Неверный промокод", reply_markup=get_menu(uid))
                _states.pop(uid, None); return
            if promo["used_count"] >= promo["max_uses"]:
                await msg.answer("❌ Промокод закончился", reply_markup=get_menu(uid))
                _states.pop(uid, None); return
            used = await db.fetchrow("SELECT 1 FROM promo_uses WHERE user_id=$1 AND code=$2", uid, code)
            if used:
                await msg.answer("❌ Ты уже активировал", reply_markup=get_menu(uid))
                _states.pop(uid, None); return
            await db.execute("UPDATE users SET balance = balance + $1 WHERE user_id=$2", promo["amount"], uid)
            await db.execute("UPDATE promo_codes SET used_count = used_count + 1 WHERE code=$1", code)
            await db.execute("INSERT INTO promo_uses (user_id, code, used_at) VALUES ($1,$2,$3)", uid, code, time.time())
        _states.pop(uid, None)
        await msg.answer(f"✅ Промокод: <b>+{fmt_money(promo['amount'])} монет</b>", reply_markup=get_menu(uid))
        return

    if state == "await_withdraw":
        try: amount = float(msg.text.replace(",", "."))
        except Exception:
            await msg.answer("❌ Не понял сумму", reply_markup=get_menu(uid)); return
        min_wd = await get_min_withdraw()
        if amount < min_wd:
            await msg.answer(f"❌ Минимум {fmt_money(min_wd)} монет", reply_markup=get_menu(uid)); return
        u = await get_user(uid)
        bal = u["balance"] if u else 0
        if amount > bal:
            await msg.answer(f"❌ У тебя только {fmt_money(bal)} монет", reply_markup=get_menu(uid)); return
        _states.pop(uid, None)
        pool = await get_pool()
        async with pool.acquire() as db:
            res = await db.execute("UPDATE users SET balance = balance - $1 WHERE user_id=$2 AND balance >= $1", amount, uid)
            if res == "UPDATE 0":
                await msg.answer("❌ Недостаточно", reply_markup=get_menu(uid)); return
            row = await db.fetchrow("INSERT INTO withdrawals (user_id, amount, status, created_at) VALUES ($1,$2,'pending',$3) RETURNING id", uid, amount, time.time())
            wid = row["id"]
        await msg.answer(f"⏳ Вывод {fmt_money(amount)} монет обрабатывается…", reply_markup=get_menu(uid))
        resp = await bc_transfer(uid, amount)
        if resp.get("status") == "ok":
            pool = await get_pool()
            async with pool.acquire() as db:
                await db.execute("UPDATE withdrawals SET status='done', tx_id=$1 WHERE id=$2", resp.get("transaction_id"), wid)
            await msg.answer(
                f"✅ Выведено: <b>{fmt_money(amount)} монет</b>\n"
                f"TX: <code>{str(resp.get('transaction_id',''))[:16]}…</code>\n\n"
                f'💎 <b>Проверь свой баланс: <a href="https://t.me/byteappbot">@byteappbot</a></b>',
                reply_markup=get_menu(uid))
            user_str = await get_user_display(uid)
            await notify_admin(f"💸 Выплата\n👤 {user_str}\n💰 {fmt_money(amount)} монет\nTX: <code>{resp.get('transaction_id','')}</code>")
        else:
            pool = await get_pool()
            async with pool.acquire() as db:
                await db.execute("UPDATE users SET balance = balance + $1 WHERE user_id=$2", amount, uid)
                await db.execute("UPDATE withdrawals SET status='failed', error=$1 WHERE id=$2", resp.get("error", ""), wid)
            await msg.answer(f"❌ Ошибка: {resp.get('error','unknown')}\nСредства возвращены.", reply_markup=get_menu(uid))
        return

    # ... (блоки adm_reward, adm_max, adm_minw, adm_refb, adm_broadcast, adm_promo_create, adm_find, adm_give остаются такими же, как в твоем коде)    # ===== АДМИН: НАГРАДА =====
    if state == "adm_reward":
        try:
            val = float(msg.text.replace(",", "."))
            if val <= 0: await msg.answer("❌ > 0"); return
        except Exception: await msg.answer("❌ Число"); return
        await set_setting("reward", val)
        _states.pop(uid, None)
        await msg.answer(f"✅ Награда: <b>{fmt_money(val)} монет</b>", reply_markup=admin_menu())
        return

    # ===== АДМИН: МАКС СПОНСОРОВ =====
    if state == "adm_max":
        try: val = int(msg.text)
        except Exception: await msg.answer("❌ Целое"); return
        if val > MAX_MAX_SPONSORS or val < 1:
            await msg.answer(f"❌ От 1 до {MAX_MAX_SPONSORS}"); return
        await set_setting("max_sponsors", val)
        _states.pop(uid, None)
        await msg.answer(f"✅ Макс: <b>{val}</b>", reply_markup=admin_menu())
        return

    # ===== АДМИН: МИН ВЫВОД =====
    if state == "adm_minw":
        try:
            val = float(msg.text.replace(",", "."))
            if val <= 0: await msg.answer("❌ > 0"); return
        except Exception: await msg.answer("❌ Число"); return
        await set_setting("min_withdraw", val)
        _states.pop(uid, None)
        await msg.answer(f"✅ Мин. вывод: <b>{fmt_money(val)} монет</b>", reply_markup=admin_menu())
        return

    # ===== АДМИН: РЕФ-БОНУС =====
    if state == "adm_refb":
        try:
            val = float(msg.text.replace(",", "."))
            if val < 0: await msg.answer("❌ ≥ 0"); return
        except Exception: await msg.answer("❌ Число"); return
        await set_setting("ref_bonus", val)
        _states.pop(uid, None)
        await msg.answer(f"✅ Реф-бонус: <b>{fmt_money(val)} монет</b>", reply_markup=admin_menu())
        return

    # ===== АДМИН: РАССЫЛКА =====
    if state == "adm_broadcast":
        _states.pop(uid, None)
        await msg.answer("📢 Рассылка…")
        pool = await get_pool()
        async with pool.acquire() as db:
            rows = await db.fetch("SELECT user_id FROM users WHERE blocked=FALSE")
        ok = 0; fail = 0
        for r in rows:
            try:
                await msg.copy_to(r["user_id"])
                ok += 1
            except Exception:
                fail += 1
            await asyncio.sleep(0.05)
        await msg.answer(f"✅ Отправлено: <b>{ok}</b>\n❌ Ошибок: <b>{fail}</b>", reply_markup=admin_menu())
        return

    # ===== АДМИН: СОЗДАТЬ ПРОМОКОД =====
    if state == "adm_promo_create":
        parts = [p.strip() for p in msg.text.split("|")]
        if len(parts) != 3:
            await msg.answer("❌ Формат: <code>КОД | СУММА | АКТИВАЦИЙ</code>"); return
        code, amount_s, uses_s = parts
        try:
            amount = float(amount_s); uses = int(uses_s)
        except Exception:
            await msg.answer("❌ Числа"); return
        pool = await get_pool()
        async with pool.acquire() as db:
            try:
                await db.execute("INSERT INTO promo_codes (code, amount, max_uses, created_at) VALUES ($1,$2,$3,$4)", code, amount, uses, time.time())
            except Exception:
                await msg.answer("❌ Такой код уже есть"); return
        _states.pop(uid, None)
        await msg.answer(f"✅ Промокод <code>{code}</code>: {fmt_money(amount)} × {uses}", reply_markup=admin_menu())
        return

    # ===== АДМИН: НАЙТИ ЮЗЕРА =====
    if state == "adm_find":
        query = msg.text.strip()
        _states.pop(uid, None)
        target = await find_user(query)
        if not target:
            await msg.answer(f"❌ Юзер <code>{escape_html(query)}</code> не найден", reply_markup=admin_menu()); return
        u = await get_user(target)
        if not u:
            await msg.answer(f"❌ Юзер не найден в БД", reply_markup=admin_menu()); return
        pool = await get_pool()
        async with pool.acquire() as db:
            refs = (await db.fetchrow("SELECT COUNT(*) as c FROM users WHERE referrer_id=$1", target))["c"]
            withdrawn = (await db.fetchrow("SELECT COALESCE(SUM(amount),0) as s FROM withdrawals WHERE user_id=$1 AND status='done'", target))["s"]
            subs_total = (await db.fetchrow("SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status='subscribed'", target))["c"]
            subs_by_service = await db.fetch("""SELECT service, COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status='subscribed' GROUP BY service ORDER BY c DESC""", target)

        user_str = user_display(target, u["username"])
        ref_str = "—"
        if u["referrer_id"]: ref_str = await get_user_display(u["referrer_id"])

        svc_lines = ""
        if subs_by_service:
            svc_lines = "\n" + "\n".join(f"    • <b>{r['service']}</b> — {r['c']}" for r in subs_by_service)
        else:
            svc_lines = "\n    <i>нет подписок</i>"

        text = (
            f"🔎 <b>Информация о юзере</b>\n\n👤 {user_str}\n\n"
            f"💰 Баланс: <b>{fmt_money(u['balance'])} монет</b>\n"
            f"📊 Всего подписок: <b>{subs_total}</b>{svc_lines}\n"
            f"👥 Рефералов: <b>{refs}</b>\n🔗 Реферер: {ref_str}\n"
            f"💸 Выведено всего: <b>{fmt_money(withdrawn)} монет</b>\n"
            f"🚫 Бан: {'Да' if u['blocked'] else 'Нет'}\n\n"
            f"Команды:\n<code>/ban {target}</code>\n<code>/unban {target}</code>\n<code>/give {target} 500</code>"
        )
        await msg.answer(text, reply_markup=admin_menu())
        return

    # ===== АДМИН: НАЧИСЛИТЬ =====
    if state == "adm_give":
        parts = msg.text.strip().split()
        if len(parts) != 2:
            await msg.answer("❌ Формат: <code>ID_или_@username СУММА</code>"); return
        query, amount_s = parts
        target = await find_user(query)
        if not target:
            await msg.answer(f"❌ Юзер <code>{escape_html(query)}</code> не найден"); return
        try: amount = float(amount_s.replace(",", "."))
        except Exception: await msg.answer("❌ Неверная сумма"); return
        await add_balance(target, amount)
        _states.pop(uid, None)
        user_str = await get_user_display(target)
        await msg.answer(f"✅ Начислено <b>{fmt_money(amount)} монет</b> → {user_str}", reply_markup=admin_menu())
        try: await bot.send_message(target, f"🎁 Тебе начислено: <b>+{fmt_money(amount)} монет</b>")
        except Exception: pass
        return

# ========== КОМАНДЫ АДМИНА ==========
@dp.message(Command("ban"))
async def cmd_ban(msg: types.Message):
    if msg.from_user.id != ADMIN_ID: return
    try: query = msg.text.split()[1]
    except Exception: await msg.answer("Формат: <code>/ban ID_или_@username</code>"); return
    target = await find_user(query)
    if not target: await msg.answer("❌ Юзер не найден"); return
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE users SET blocked=TRUE WHERE user_id=$1", target)
    user_str = await get_user_display(target)
    await msg.answer(f"🚫 Забанен: {user_str}", reply_markup=admin_menu())

@dp.message(Command("unban"))
async def cmd_unban(msg: types.Message):
    if msg.from_user.id != ADMIN_ID: return
    try: query = msg.text.split()[1]
    except Exception: await msg.answer("Формат: <code>/unban ID_или_@username</code>"); return
    target = await find_user(query)
    if not target: await msg.answer("❌ Юзер не найден"); return
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE users SET blocked=FALSE WHERE user_id=$1", target)
    user_str = await get_user_display(target)
    await msg.answer(f"✅ Разбанен: {user_str}", reply_markup=admin_menu())

@dp.message(Command("give"))
async def cmd_give(msg: types.Message):
    if msg.from_user.id != ADMIN_ID: return
    try:
        parts = msg.text.split()
        query = parts[1]
        amount = float(parts[2].replace(",", "."))
    except Exception: await msg.answer("Формат: <code>/give ID_или_@username СУММА</code>"); return
    target = await find_user(query)
    if not target: await msg.answer("❌ Юзер не найден"); return
    await add_balance(target, amount)
    user_str = await get_user_display(target)
    await msg.answer(f"✅ Начислено <b>{fmt_money(amount)} монет</b> → {user_str}", reply_markup=admin_menu())
    try: await bot.send_message(target, f"🎁 Тебе начислено: <b>+{fmt_money(amount)} монет</b>")
    except Exception: pass

@dp.message(Command("errors"))
async def cmd_errors(msg: types.Message):
    if msg.from_user.id != ADMIN_ID: return
    try: rows = await get_recent_errors(hours=3)
    except Exception as e: await msg.answer(f"❌ Ошибка БД: {e}"); return
    if not rows:
        await msg.answer("✅ <b>Ошибок за 3 часа нет!</b>"); return
    text = f"⚠️ <b>Ошибки за 3ч:</b> ({len(rows)})\n\n"
    for r in rows[:15]:
        ts = time.strftime("%d.%m %H:%M", time.localtime(r["created_at"]))
        text += f"<b>{escape_html(r['service'])}</b> · {escape_html(r['error_type'])}\n"
        text += f"<code>{escape_html((r['message'] or '')[:100])}</code>\n<i>{ts}</i>\n\n"
    if len(text) > 4000: text = text[:4000] + "…"
    await msg.answer(text)

# ========== АДМИН CALLBACKS ==========
@dp.callback_query(F.data == "adm_reward")
async def adm_reward(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_reward"
    await cq.answer()
    try: await cq.message.edit_text(f"💰 Текущая награда: <b>{fmt_money(await get_reward())} монет</b>\n\nОтправь новое значение:")
    except Exception: pass

@dp.callback_query(F.data == "adm_max")
async def adm_max(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_max"
    await cq.answer()
    try: await cq.message.edit_text(f"👥 Текущий: <b>{await get_max_sponsors()}</b>\n\nОтправь число 1-{MAX_MAX_SPONSORS}:")
    except Exception: pass

@dp.callback_query(F.data == "adm_minw")
async def adm_minw(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_minw"
    await cq.answer()
    try: await cq.message.edit_text(f"💸 Мин. вывод: <b>{fmt_money(await get_min_withdraw())} монет</b>\n\nОтправь новое значение:")
    except Exception: pass

@dp.callback_query(F.data == "adm_refb")
async def adm_refb(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_refb"
    await cq.answer()
    try: await cq.message.edit_text(f"👥 Реф-бонус: <b>{fmt_money(await get_ref_bonus())} монет</b>\n\nОтправь новое значение:")
    except Exception: pass

@dp.callback_query(F.data == "adm_stats")
async def adm_stats(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
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
            f"📊 <b>Статистика</b>\n\n━━━━━━━━━━━━━━━\n"
            f"👥 Юзеров: <b>{total}</b> (🚫 {blocked})\n"
            f"💰 Балансы: <b>{fmt_money(balances)} монет</b>\n"
            f"✅ Подписок: <b>{subs}</b>\n"
            f"💸 Выплачено: <b>{fmt_money(paid)} монет</b>\n"
            f"📤 Pending: <b>{pend}</b>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"⚠️ Ошибок за 24ч: <b>{errs}</b>", reply_markup=admin_menu())
    except Exception: pass

@dp.callback_query(F.data == "adm_find")
async def adm_find(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_find"
    await cq.answer()
    try: await cq.message.edit_text("🔎 <b>Найти юзера</b>\n\nОтправь <b>ID</b> или <b>@username</b>:\nПример: <code>123456789</code> или <code>@vasya</code>")
    except Exception: pass

@dp.callback_query(F.data == "adm_give")
async def adm_give(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_give"
    await cq.answer()
    try: await cq.message.edit_text("💎 <b>Начислить</b>\n\nФормат: <code>ID_или_@username СУММА</code>\nПример: <code>123456789 500</code> или <code>@vasya 500</code>")
    except Exception: pass

@dp.callback_query(F.data == "adm_broadcast")
async def adm_broadcast(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_broadcast"
    await cq.answer()
    try: await cq.message.edit_text("📢 Отправь сообщение для рассылки (всем активным):")
    except Exception: pass

@dp.callback_query(F.data == "adm_promo_create")
async def adm_promo_create(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_promo_create"
    await cq.answer()
    try: await cq.message.edit_text("Формат: <code>КОД | СУММА | АКТИВАЦИЙ</code>\nПример: <code>SALE | 500 | 100</code>")
    except Exception: pass

@dp.callback_query(F.data == "adm_withdraws")
async def adm_withdraws(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch("""SELECT w.id, w.user_id, w.amount, w.status, w.tx_id, u.username
            FROM withdrawals w LEFT JOIN users u ON u.user_id = w.user_id ORDER BY w.id DESC LIMIT 20""")
    if not rows:
        await cq.answer("Нет заявок", show_alert=True); return
    text = "💸 <b>Последние выводы:</b>\n\n"
    for r in rows:
        emoji = {"done": "✅", "pending": "⏳", "failed": "❌"}.get(r["status"], "•")
        user_str = user_display(r["user_id"], r["username"])
        text += f"{emoji} #{r['id']} · {user_str} · <b>{fmt_money(r['amount'])}</b>\n"
    await cq.answer()
    try: await cq.message.edit_text(text, reply_markup=admin_menu())
    except Exception: pass

# ========== ДИАГНОСТИКА (кнопки) ==========
@dp.callback_query(F.data == "adm_diag")
async def adm_diag(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    await cq.answer()
    text = "📊 <b>Диагностика</b>\n\nВыбери, что проверить:\n• <b>Проверить сервисы</b> — живой опрос API\n• <b>Ошибки за 3ч</b> — последние ошибки"
    try: await cq.message.edit_text(text, reply_markup=diag_kb())
    except Exception: await cq.message.answer(text, reply_markup=diag_kb())

@dp.callback_query(F.data == "diag_back")
async def cb_diag_back(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    await cq.answer()
    try: await cq.message.edit_text("🛠 <b>Админ-панель</b>\n\nВыбери действие:", reply_markup=admin_menu())
    except Exception: pass

@dp.callback_query(F.data == "diag_services")
async def cb_diag_services(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    await cq.answer("Проверяю сервисы…")
    try: await cq.message.edit_text("🔎 <b>Проверяю сервисы…</b>")
    except Exception: pass
    try: report = await run_diagnostics()
    except Exception as e: report = f"❌ Ошибка диагностики: {e}"
    if len(report) > 4000: report = report[:4000] + "…"

    kb = InlineKeyboardBuilder()
    kb.button(text="🔄 Повторить", callback_data="diag_services")
    kb.button(text="⚠️ Ошибки за 3ч", callback_data="diag_errors")
    kb.button(text="⬅️ Назад", callback_data="diag_back")
    kb.adjust(2, 1)

    try: await cq.message.edit_text(report, reply_markup=kb.as_markup())
    except Exception:
        try: await cq.message.answer(report, reply_markup=kb.as_markup())
        except Exception: pass

@dp.callback_query(F.data == "diag_errors")
async def cb_diag_errors(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    await cq.answer()
    try: rows = await get_recent_errors(hours=3)
    except Exception as e:
        await cq.answer(f"Ошибка БД: {e}", show_alert=True); return

    if not rows:
        text = "✅ <b>Ошибок нет!</b>\n\nЗа последние <b>3 часа</b> не зафиксировано ни одной ошибки."
    else:
        text = f"⚠️ <b>Ошибки за 3 часа:</b> ({len(rows)} шт)\n\n"
        for r in rows[:15]:
            ts = time.strftime("%d.%m %H:%M", time.localtime(r["created_at"]))
            text += f"<b>{escape_html(r['service'])}</b> · {escape_html(r['error_type'])}\n"
            text += f"<code>{escape_html((r['message'] or '')[:100])}</code>\n<i>{ts}</i>\n\n"
        if len(rows) > 15: text += f"\n<i>…и ещё {len(rows) - 15}</i>"
    if len(text) > 4000: text = text[:4000] + "…"

    kb = InlineKeyboardBuilder()
    kb.button(text="🔄 Обновить", callback_data="diag_errors")
    kb.button(text="🔍 Сервисы", callback_data="diag_services")
    kb.button(text="⬅️ Назад", callback_data="diag_back")
    kb.adjust(2, 1)

    try: await cq.message.edit_text(text, reply_markup=kb.as_markup())
    except Exception:
        try: await cq.message.answer(text, reply_markup=kb.as_markup())
        except Exception: pass

# ========== ТОП ==========
@dp.callback_query(F.data == "adm_top")
async def adm_top(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    await cq.answer()
    text = "🏆 <b>Топ игроков</b>\n\nВыбери категорию:"
    try: await cq.message.edit_text(text, reply_markup=top_kb())
    except Exception: await cq.message.answer(text, reply_markup=top_kb())

@dp.callback_query(F.data == "top_back")
async def cb_top_back(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    await cq.answer()
    try: await cq.message.edit_text("🛠 <b>Админ-панель</b>\n\nВыбери действие:", reply_markup=admin_menu())
    except Exception: pass

@dp.callback_query(F.data == "top_balance")
async def cb_top_balance(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    await cq.answer()
    rows = await get_top_by_balance(15)
    if not rows: text = "🏆 <b>Топ по балансу</b>\n\n📭 Пусто."
    else:
        text = "🏆 <b>Топ-15 по балансу</b>\n\n"
        for i, r in enumerate(rows, 1):
            user_str = user_display(r["user_id"], r["username"])
            text += f"{i}. {user_str} — <b>{fmt_money(r['balance'])}</b> монет\n"

    kb = InlineKeyboardBuilder()
    kb.button(text="👥 По рефералам", callback_data="top_refs")
    kb.button(text="🔄 Обновить", callback_data="top_balance")
    kb.button(text="⬅️ Назад", callback_data="top_back")
    kb.adjust(2, 1)
    if len(text) > 4000: text = text[:4000] + "…"
    try: await cq.message.edit_text(text, reply_markup=kb.as_markup())
    except Exception:
        try: await cq.message.answer(text, reply_markup=kb.as_markup())
        except Exception: pass

@dp.callback_query(F.data == "top_refs")
async def cb_top_refs(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    await cq.answer()
    rows = await get_top_by_refs(15)
    if not rows: text = "🏆 <b>Топ по рефералам</b>\n\n📭 Пусто."
    else:
        text = "🏆 <b>Топ-15 по рефералам</b>\n\n"
        for i, r in enumerate(rows, 1):
            user_str = user_display(r["user_id"], r["username"])
            cnt = r["refs_count"] or 0
            text += f"{i}. {user_str} — <b>{cnt}</b> реф.\n"

    kb = InlineKeyboardBuilder()
    kb.button(text="💰 По балансу", callback_data="top_balance")
    kb.button(text="🔄 Обновить", callback_data="top_refs")
    kb.button(text="⬅️ Назад", callback_data="top_back")
    kb.adjust(2, 1)
    if len(text) > 4000: text = text[:4000] + "…"
    try: await cq.message.edit_text(text, reply_markup=kb.as_markup())
    except Exception:
        try: await cq.message.answer(text, reply_markup=kb.as_markup())
        except Exception: pass

# ========== WEB + MAIN ==========
async def health(_):
    return web.Response(text="ok")

async def start_web():
    app = web.Application()
    app.router.add_get("/", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    logging.info(f"Web on :{PORT}")

async def on_shutdown(*args, **kwargs):
    global _http
    if _http and not _http.closed:
        await _http.close()
        logging.info("HTTP session closed")

async def main():
    global darkboost, BOT_ID
    await init_db()
    await start_web()

    # ⚡ ИНИЦИАЛИЗАЦИЯ DARKBOOST
    me = await bot.get_me()
    BOT_ID = me.id
    if DARKBOOST_API_KEY:
        darkboost = DarkBoostOP(await http(), await get_pool(), BOT_ID)
        logging.info("✅ DarkBoost initialized")

    asyncio.create_task(autocheck_worker())
    asyncio.create_task(lazy_worker())
    dp.shutdown.register(on_shutdown)
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
