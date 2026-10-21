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
TRAFSLY_API_KEY  = os.getenv("TRAFSLY_API_KEY", "")

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
    except Exception as e: logging.error(f"log_error failed: {e}")

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
            lines = [f"🎯 <b>{service}</b> ({cnt} шт, +{fmt_money(sm)})"]
            for aid, reward, link, tt in items:
                short_link = str(link) if link else str(aid)
                if len(short_link) > 60: short_link = short_link[:57] + "…"
                lines.append(f"  • <a href=\"{short_link}\">{short_link}</a>")
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
        if ref_id: ref_line = f"\n🔗 Реферер: {await get_user_display(ref_id)}"
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

LAZY_USER_MSGS = [
    "😡 <b>Эй, ты чего?!</b>\n\nТы пришёл заработать монеты, а сам ничего не сделал?\nТак дело не пойдёт.\n\n🔥 Жми «🎯 Заработать», подпишись на <b>ВСЕХ</b> спонсоров и нажми «✅ Я подписался — проверить». Это 2 минуты.\n\n⚠️ <i>Пока не подпишешься на 5+ каналов — ты НЕ заработаешь ничего.</i>",
    "😤 <b>Слушай, так не работает!</b>\n\nТы нажал /start и думаешь, что монеты упадут сами?\nНет, дружок. Так не бывает.\n\n💎 Зайди в «🎯 Заработать», подпишись на спонсоров, проверь подписки — и получишь свои первые монеты.\n\n🚀 <i>Хватит лениться, начни зарабатывать!</i>"
]

async def notify_lazy_user(user_id):
    try:
        await bot.send_message(user_id, random.choice(LAZY_USER_MSGS), disable_web_page_preview=True)
        return True
    except Exception: return False

async def notify_admin_lazy(user_id, subs, mins):
    try: await bot.send_message(ADMIN_ID, f"😴 <b>Ленивый юзер</b>\n\n👤 {await get_user_display(user_id)}\n📊 Подписок: <b>{subs}</b>/{LAZY_THRESHOLD}\n⏱ Прошло: {mins} мин", disable_web_page_preview=True)
    except Exception: pass

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
        await db.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users (LOWER(username))")
        await db.execute("""CREATE TABLE IF NOT EXISTS sponsor_tasks (
            id SERIAL PRIMARY KEY, user_id BIGINT, service TEXT, assignment_id TEXT, link TEXT,
            reward DOUBLE PRECISION DEFAULT 0, status TEXT DEFAULT 'unsubscribed',
            signature TEXT, created_at DOUBLE PRECISION, UNIQUE(user_id, service, assignment_id))""")
        await db.execute("ALTER TABLE sponsor_tasks ADD COLUMN IF NOT EXISTS task_type TEXT")
        await db.execute("""CREATE TABLE IF NOT EXISTS withdrawals (
            id SERIAL PRIMARY KEY, user_id BIGINT, amount DOUBLE PRECISION, status TEXT DEFAULT 'pending',
            tx_id TEXT, error TEXT, created_at DOUBLE PRECISION)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS promo_codes (
            code TEXT PRIMARY KEY, amount DOUBLE PRECISION, max_uses INT DEFAULT 1,
            used_count INT DEFAULT 0, active BOOLEAN DEFAULT TRUE, created_at DOUBLE PRECISION)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS promo_uses (
            user_id BIGINT, code TEXT, used_at DOUBLE PRECISION, PRIMARY KEY (user_id, code))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS sponsor_cache (
            cache_key TEXT PRIMARY KEY, payload TEXT NOT NULL, expires_at DOUBLE PRECISION NOT NULL)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS error_log (
            id SERIAL PRIMARY KEY, service TEXT, error_type TEXT, message TEXT, created_at DOUBLE PRECISION)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS lazy_watch (
            user_id BIGINT PRIMARY KEY, referrer_id BIGINT, created_at DOUBLE PRECISION,
            last_notified DOUBLE PRECISION DEFAULT 0, admin_notified BOOLEAN DEFAULT FALSE)""")
        await db.execute("ALTER TABLE lazy_watch ADD COLUMN IF NOT EXISTS admin_notified BOOLEAN DEFAULT FALSE")
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

async def get_reward(): return float(await get_setting("reward", DEFAULT_REWARD))
async def get_ref_bonus(): return float(await get_setting("ref_bonus", DEFAULT_REF_BONUS))
async def get_min_withdraw(): return float(await get_setting("min_withdraw", DEFAULT_MIN_WITHDRAW))
async def get_max_sponsors(): return int(await get_setting("max_sponsors", DEFAULT_MAX_SPONSORS))

async def register_user(user_id, referrer_id=None, username=None):
    pool = await get_pool()
    async with pool.acquire() as db:
        exists = await db.fetchrow("SELECT user_id FROM users WHERE user_id=$1", user_id)
        if exists:
            if username: await db.execute("UPDATE users SET username=$1 WHERE user_id=$2", username, user_id)
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
        async with pool.acquire() as db: await db.execute("UPDATE users SET username=$1 WHERE user_id=$2", username, user_id)
    except Exception: pass

async def get_user(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        return await db.fetchrow("SELECT balance, referrer_id, referred_earned, blocked, username FROM users WHERE user_id=$1", user_id)

async def find_user(query):
    query = query.strip()
    if query.isdigit():
        pool = await get_pool()
        async with pool.acquire() as db: row = await db.fetchrow("SELECT user_id FROM users WHERE user_id=$1", int(query))
        return row["user_id"] if row else None
    clean = query.lstrip("@").lower()
    if not clean: return None
    pool = await get_pool()
    async with pool.acquire() as db: row = await db.fetchrow("SELECT user_id FROM users WHERE LOWER(username)=$1 LIMIT 1", clean)
    return row["user_id"] if row else None

async def add_balance(user_id, amount):
    pool = await get_pool()
    async with pool.acquire() as db: await db.execute("UPDATE users SET balance = balance + $1 WHERE user_id=$2", amount, user_id)

async def add_referred_earned(user_id, amount):
    pool = await get_pool()
    async with pool.acquire() as db: await db.execute("UPDATE users SET referred_earned = referred_earned + $1 WHERE user_id=$2", amount, user_id)

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
    async with pool.acquire() as db: await db.execute("UPDATE sponsor_tasks SET status='subscribed' WHERE user_id=$1 AND service=$2 AND assignment_id=$3", user_id, service, aid)

async def clear_pending_tasks(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("DELETE FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed'", user_id)
        await db.execute("DELETE FROM sponsor_cache WHERE cache_key=$1", f"all_sponsors_{user_id}")

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
        rows = await db.fetch("SELECT link FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed' ORDER BY id ASC", user_id)
        return [r["link"] for r in rows]

async def pending_by_service(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch("SELECT service, COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed' GROUP BY service", user_id)
        return {r["service"]: r["c"] for r in rows}

async def count_total_subs(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status='subscribed'", user_id)
        return row["c"]

async def add_lazy_watch(user_id, referrer_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("INSERT INTO lazy_watch (user_id, referrer_id, created_at, last_notified, admin_notified) VALUES ($1,$2,$3,0,FALSE) ON CONFLICT (user_id) DO NOTHING", user_id, referrer_id, time.time())

async def remove_lazy_watch(user_id):
    pool = await get_pool()
    async with pool.acquire() as db: await db.execute("DELETE FROM lazy_watch WHERE user_id=$1", user_id)

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
        await db.execute("INSERT INTO sponsor_cache (cache_key, payload, expires_at) VALUES ($1,$2,$3) ON CONFLICT (cache_key) DO UPDATE SET payload=$2, expires_at=$3", f"all_sponsors_{user_id}", json.dumps(links), time.time() + CACHE_TTL)

# ========== API СПОНСОРОВ ==========
async def get_piarflow(user_id):
    if not PIARFLOW_API_KEY: return []
    s = await http()
    try:
        async with s.post("https://piarflow.com/v1/sponsors", json={"user_id": user_id, "chat_id": user_id, "max_sponsors": await get_max_sponsors()}, headers={"Authorization": f"Bearer {PIARFLOW_API_KEY}"}) as r:
            d = await r.json()
            if d.get("status") == "ok": return d.get("sponsors", [])
    except Exception as e: await log_error("piarflow", "fetch", str(e))
    return []

async def check_piarflow(user_id, links):
    if not PIARFLOW_API_KEY or not links: return []
    s = await http()
    try:
        async with s.post("https://piarflow.com/v1/sponsors/check", json={"user_id": user_id, "links": links}, headers={"Authorization": f"Bearer {PIARFLOW_API_KEY}"}) as r:
            d = await r.json()
            if d.get("status") == "ok": return d.get("sponsors", [])
    except Exception as e: await log_error("piarflow", "check", str(e))
    return []

async def get_axionna(user_id):
    if not AXIONNA_API_KEY: return []
    s = await http()
    try:
        async with s.post("https://axionna.org/api/sponsors", json={"user_id": user_id, "max_sponsors": 20}, headers={"Authorization": f"Bearer {AXIONNA_API_KEY}", "Content-Type": "application/json"}) as r:
            if r.status != 200: return []
            d = await r.json()
            if d.get("status") == "ok": return d.get("sponsors", [])
    except Exception as e: await log_error("axionna", "fetch", str(e))
    return []

async def check_axionna(user_id, task_ids):
    if not AXIONNA_API_KEY or not task_ids: return {}
    s = await http()
    try:
        async with s.post("https://axionna.org/api/check", json={"user_id": user_id, "task_ids": task_ids}, headers={"Authorization": f"Bearer {AXIONNA_API_KEY}", "Content-Type": "application/json"}) as r:
            if r.status != 200: return {}
            d = await r.json()
            if d.get("status") == "ok":
                result = {}
                for x in d.get("results", []):
                    tid = str(x.get("id"))
                    ok = (x.get("subscribed") is True) and (x.get("credited") is True)
                    result[tid] = ok
                return result
    except Exception as e: await log_error("axionna", "check", str(e))
    return {}

async def _flowly_request(user_id, lang="ru", is_premium=False):
    if not FLOWLY_API_KEY: return None
    try:
        timeout = aiohttp.ClientTimeout(total=FLOWLY_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.post("https://flowly.guru/api/v1/offers", headers={"Auth": FLOWLY_API_KEY}, json={"tg_user_id": user_id, "lang": lang or "ru", "is_premium": bool(is_premium)}) as r:
                if r.status != 200: return None
                return await r.json()
    except Exception: return None

async def get_flowly(user_id, lang="ru"):
    d = await _flowly_request(user_id, lang)
    if not d: return []
    if d.get("status") == "ok": return []
    out = []
    for item in d.get("offers", []):
        if item.get("subscribed"): continue
        link = item.get("link")
        if link: out.append({"id": link, "link": link, "name": item.get("name", "")})
    return out

async def check_flowly(user_id, lang="ru"):
    d = await _flowly_request(user_id, lang)
    if not d: return None, []
    return d.get("status"), d.get("offers", [])

async def fetch_flyer(user_id, lang="ru"):
    if not flyer: return []
    try:
        tasks = await flyer.get_tasks(user_id=user_id, language_code=lang or "ru", limit=await get_max_sponsors())
        if not tasks: return []
        incomplete = [t for t in tasks if t.get('status') in INCOMPLETE_STATUSES]
        out = []
        for t in incomplete:
            links = t.get('links', [])
            if links: out.append({"id": t.get('signature'), "link": links[0], "signature": t.get('signature'), "task": t.get('task', '')})
        return out
    except Exception as e: await log_error("flyer", "get_tasks", str(e))
    return []

async def check_flyer(signature):
    if not flyer or not signature: return None
    try: return await flyer.check_task(signature=signature)
    except Exception as e: await log_error("flyer", "check", str(e))
    return None

async def get_traffy(user_id, first_name="", username="", language_code="ru"):
    if not TRAFFY_API_KEY: return []
    s = await http()
    try:
        payload = {"telegram_id": int(user_id), "limit": min(await get_max_sponsors(), 10), "language_code": language_code or "ru"}
        if first_name: payload["first_name"] = first_name
        if username: payload["username"] = username
        async with s.post("https://api.traffy.ai/publisher/tasks", json=payload, headers={"x-publisher-api-key": TRAFFY_API_KEY, "Content-Type": "application/json"}) as r:
            if r.status not in (200, 201): return []
            d = await r.json()
            if d.get("ok") is True: return d.get("tasks", [])
    except Exception as e: await log_error("traffy", "fetch", str(e))
    return []

async def check_traffy(user_id, assignment_ids):
    if not TRAFFY_API_KEY or not assignment_ids: return {}
    s = await http()
    try:
        async with s.post("https://api.traffy.ai/publisher/tasks/check", json={"telegram_id": int(user_id), "assignment_ids": assignment_ids}, headers={"x-publisher-api-key": TRAFFY_API_KEY, "Content-Type": "application/json"}) as r:
            if r.status not in (200, 201): return {}
            d = await r.json()
            if d.get("ok") is True:
                result = {}
                for x in d.get("results", []):
                    aid = str(x.get("assignment_id"))
                    result[aid] = (x.get("status") == "completed")
                return result
    except Exception as e: await log_error("traffy", "check", str(e))
    return {}

# ========== TRAFSLY (ФИКС КЛОДА) ==========
async def get_trafsly(user_id, username):
    if not TRAFSLY_API_KEY: return []
    payload = {"user_id": user_id, "max_sponsors": await get_max_sponsors(), "language_code": "ru", "is_premium": False}
    if username: payload["username"] = username
    s = await http()
    try:
        async with s.post("https://api.trafsly.com/api/v1/get-sponsors", json=payload, headers={"Auth": TRAFSLY_API_KEY}) as r:
            if r.status != 200:
                await log_error("trafsly", "fetch", f"HTTP {r.status}")
                return []
            d = await r.json()
            if d.get("status") == "success":
                return d.get("sponsors", [])
            else:
                await log_error("trafsly", "fetch", f"status={d.get('status')}")
                return []
    except Exception as e:
        await log_error("trafsly", "fetch", str(e))
        return []

async def check_trafsly(user_id, ads_ids):
    if not TRAFSLY_API_KEY or not ads_ids: return []
    s = await http()
    res = []
    for aid in ads_ids:
        try:
            async with s.post("https://api.trafsly.com/api/v1/confirm-subscription", json={"user_id": user_id, "ads_id": int(aid)}, headers={"Auth": TRAFSLY_API_KEY}) as r:
                if r.status != 200:
                    await log_error("trafsly", "check", f"HTTP {r.status} for ads_id {aid}")
                    continue
                d = await r.json()
                if "subscribed" not in d:
                    await log_error("trafsly", "check", f"Invalid response: {d}")
                    continue
                if d.get("subscribed") is True:
                    res.append({"ads_id": aid, "status": "subscribed"})
                else:
                    res.append({"ads_id": aid, "status": "unsubscribed"})
        except Exception as e:
            await log_error("trafsly", "check", f"ads_id {aid}: {str(e)}")
    return res

# ========== СБОР СПОНСОРОВ ==========
async def collect_sponsors(user):
    uid, un, fn, lang = user.id, user.username or "", getattr(user, "first_name", "") or "", user.language_code or "ru"
    reward = await get_reward()

    for x in await get_piarflow(uid):
        if x.get("link"): await save_sponsor(uid, "piarflow", x["link"], x["link"], reward)

    for x in await get_flowly(uid, lang):
        if x.get("link"): await save_sponsor(uid, "flowly", x["link"], x["link"], reward, task_type=x.get("name"))

    for x in await fetch_flyer(uid, lang):
        if x.get("link") and x.get("id"): await save_sponsor(uid, "flyer", str(x["id"]), x["link"], reward, signature=x.get("signature"), task_type=x.get("task"))

    for x in await get_traffy(uid, fn, un, lang):
        if x.get("assignment_id") and x.get("target_link"): await save_sponsor(uid, "traffy", str(x["assignment_id"]), x["target_link"], reward, task_type=x.get("button_text"))

    for x in await get_axionna(uid):
        if x.get("link") and x.get("id"): await save_sponsor(uid, "axionna", str(x["id"]), x["link"], reward)

    for x in await get_trafsly(uid, un):
        link = x.get("link")
        aid = x.get("ads_id")
        if link: await save_sponsor(uid, "trafsly", str(aid) if aid else link, link, reward)

async def _task_reward(user_id, service, aid):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT reward FROM sponsor_tasks WHERE user_id=$1 AND service=$2 AND assignment_id=$3", user_id, service, aid)
        return row["reward"] if row else 0.0

async def check_all(user_id):
    tasks = await pending_tasks(user_id)
    if not tasks: return 0, 0.0, 0

    pf, ax, fl, tr, ts, flowly_pending = [], [], [], [], [], []
    link_map, task_type_map = {}, {}
    for service, aid, link, reward, sig, task_type in tasks:
        link_map[aid] = link
        task_type_map[aid] = task_type
        if service == "piarflow" and link: pf.append(link)
        elif service == "axionna" and aid: ax.append(aid)
        elif service == "flyer" and sig: fl.append((aid, sig))
        elif service == "traffy" and aid: tr.append(aid)
        elif service == "trafsly" and aid: ts.append(aid)
        elif service == "flowly": flowly_pending.append((service, aid, link, reward, sig, task_type))

    done_count, done_sum, skipped_count, new_subs = 0, 0.0, 0, []

    if pf:
        for r in await check_piarflow(user_id, pf):
            if r.get("status") == "subscribed":
                await mark_subscribed(user_id, "piarflow", r.get("link"))
                rw = await _task_reward(user_id, "piarflow", r.get("link"))
                await add_balance(user_id, rw); done_count += 1; done_sum += rw
                new_subs.append(("piarflow", r.get("link"), rw, r.get("link"), None))
            elif r.get("status") == "not_counted": skipped_count += 1

    if ax:
        for aid, ok in (await check_axionna(user_id, ax)).items():
            if ok:
                await mark_subscribed(user_id, "axionna", aid)
                rw = await _task_reward(user_id, "axionna", aid)
                await add_balance(user_id, rw); done_count += 1; done_sum += rw
                new_subs.append(("axionna", aid, rw, link_map.get(aid), None))
            else: skipped_count += 1

    for aid, sig in fl:
        if await check_flyer(sig) == "complete":
            await mark_subscribed(user_id, "flyer", aid)
            rw = await _task_reward(user_id, "flyer", aid)
            await add_balance(user_id, rw); done_count += 1; done_sum += rw
            new_subs.append(("flyer", aid, rw, link_map.get(aid), task_type_map.get(aid)))

    if tr:
        for aid, ok in (await check_traffy(user_id, tr)).items():
            if ok:
                await mark_subscribed(user_id, "traffy", aid)
                rw = await _task_reward(user_id, "traffy", aid)
                await add_balance(user_id, rw); done_count += 1; done_sum += rw
                new_subs.append(("traffy", aid, rw, link_map.get(aid), task_type_map.get(aid)))
            else: skipped_count += 1

    if ts:
        for r in await check_trafsly(user_id, ts):
            if r.get("status") == "subscribed":
                aid = str(r.get("ads_id"))
                await mark_subscribed(user_id, "trafsly", aid)
                rw = await _task_reward(user_id, "trafsly", aid)
                if rw > 0:
                    await add_balance(user_id, rw)
                    done_count += 1; done_sum += rw
                    new_subs.append(("trafsly", aid, rw, link_map.get(aid), task_type_map.get(aid)))
            else: skipped_count += 1

    if flowly_pending:
        status, _ = await check_flowly(user_id)
        if status == "ok":
            total_rw = 0.0
            for service, aid, link, reward, sig, tt in flowly_pending:
                await mark_subscribed(user_id, "flowly", aid)
                rw = await _task_reward(user_id, "flowly", aid)
                total_rw += rw; done_count += 1; done_sum += rw
                new_subs.append(("flowly", aid, rw, link, tt))
            if total_rw > 0: await add_balance(user_id, total_rw)

    if done_count > 0:
        pool = await get_pool()
        async with pool.acquire() as db:
            row = await db.fetchrow("SELECT referrer_id, username FROM users WHERE user_id=$1", user_id)
        if row and row["referrer_id"] and row["referrer_id"] != user_id:
            ref_total = (await get_ref_bonus()) * done_count
            if ref_total > 0:
                await add_balance(row["referrer_id"], ref_total)
                await add_referred_earned(row["referrer_id"], ref_total)
                await notify_referrer(row["referrer_id"], row["username"] or "", done_count, ref_total)

    if new_subs: await notify_admin_new_subs(user_id, new_subs)
    return done_count, done_sum, skipped_count

# ========== ВОРКЕРЫ И ДИАГНОСТИКА ==========
async def autocheck_worker():
    await asyncio.sleep(300)
    while True:
        try:
            if await get_setting("autocheck", "true") == "true":
                pool = await get_pool()
                async with pool.acquire() as db:
                    rows = await db.fetch("SELECT user_id, username FROM users WHERE blocked = FALSE AND created_at > $1 LIMIT 500", time.time() - 86400)
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
                            try: await bot.send_message(uid, f"🔔 <b>Новые задания ждут тебя!</b>\n\n🎯 Доступно: <b>{after - before}</b>\nНажми «🎯 Заработать», чтобы забрать награду.")
                            except Exception: pass
                    except Exception as e: logging.error(f"autocheck {uid}: {e}")
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
                        await remove_lazy_watch(uid); continue
                    if await pending_count(uid) == 0: continue
                    if time.time() - (r["last_notified"] or 0) < LAZY_INTERVAL: continue
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
                except Exception as e: logging.error(f"lazy check {uid}: {e}")
                await asyncio.sleep(0.3)
            await asyncio.sleep(LAZY_INTERVAL)
        except Exception as e:
            logging.error(f"lazy_worker: {e}")
            await asyncio.sleep(120)

async def diagnose_service(name, key, checker):
    if not key: return f"❌ <b>{name}</b> — ключ не задан"
    t0 = time.time()
    try:
        res = await checker()
        dt = int((time.time() - t0) * 1000)
        if res.get("error"): return f"❌ <b>{name}</b> — {res['error']} ({dt}мс)"
        cnt, extra = res.get("count", 0), res.get("extra", "")
        if dt > 3000: extra += f" ⚠️ медленно"
        if cnt > 0: return f"✅ <b>{name}</b> — {cnt} заданий ({dt}мс){extra}"
        return f"🟡 <b>{name}</b> — 0 заданий ({dt}мс){extra}"
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
        if r.status != 200: return {"error": f"HTTP {r.status}", "count": 0}
        d = json.loads(body)
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
                return {"count": sum(1 for o in d.get("offers", []) if not o.get("subscribed"))}
    except asyncio.TimeoutError: return {"error": "timeout", "count": 0}

async def _diag_flyer():
    if not flyer: return {"error": "flyerapi не подключён", "count": 0}
    tasks = await flyer.get_tasks(user_id=ADMIN_ID, language_code="ru", limit=5)
    if not tasks: return {"count": 0}
    return {"count": len([t for t in tasks if t.get('status') in INCOMPLETE_STATUSES])}

async def _diag_traffy():
    s = await http()
    async with s.get("https://api.traffy.ai/publisher/me", headers={"x-publisher-api-key": TRAFFY_API_KEY}) as r:
        if r.status == 401: return {"error": "HTTP 401", "count": 0}
        d = await r.json()
        if d.get("resource", {}).get("api_enabled"): return {"count": 1, "extra": " (api активен)"}
        return {"error": "api отключен", "count": 0}

async def _diag_trafsly():
    if not TRAFSLY_API_KEY: return {"error": "ключ не задан", "count": 0}
    s = await http()
    try:
        async with s.post("https://api.trafsly.com/api/v1/get-sponsors", json={"user_id": ADMIN_ID, "max_sponsors": 5, "language_code": "ru", "is_premium": False}, headers={"Auth": TRAFSLY_API_KEY}) as r:
            if r.status != 200: return {"error": f"HTTP {r.status}", "count": 0}
            d = await r.json()
            if d.get("status") == "success":
                return {"count": len(d.get("sponsors", []))}
            return {"error": f"status={d.get('status')}", "count": 0}
    except Exception as e: return {"error": str(e)[:100], "count": 0}

async def run_diagnostics():
    results = await asyncio.gather(
        diagnose_service("Piarflow", PIARFLOW_API_KEY, _diag_piarflow),
        diagnose_service("Flowly", FLOWLY_API_KEY, _diag_flowly),
        diagnose_service("Flyer", FLYER_API_KEY, _diag_flyer),
        diagnose_service("Traffy", TRAFFY_API_KEY, _diag_traffy),
        diagnose_service("Axionna", AXIONNA_API_KEY, _diag_axionna),
        diagnose_service("Trafsly", TRAFSLY_API_KEY, _diag_trafsly),
    )
    return f"🔎 <b>Диагностика сервисов</b>\n🆔 user_id: <code>{ADMIN_ID}</code>\n\n" + "\n".join(results)

async def get_recent_errors(hours=3):
    pool = await get_pool()
    async with pool.acquire() as db:
        return await db.fetch("SELECT service, error_type, message, created_at FROM error_log WHERE created_at > $1 ORDER BY created_at DESC LIMIT 30", time.time() - hours * 3600)

async def get_top_by_balance(limit=15):
    pool = await get_pool()
    async with pool.acquire() as db:
        return await db.fetch("SELECT user_id, username, balance FROM users WHERE blocked = FALSE ORDER BY balance DESC LIMIT $1", limit)

async def get_top_by_refs(limit=15):
    pool = await get_pool()
    async with pool.acquire() as db:
        return await db.fetch("SELECT u.user_id, u.username, (SELECT COUNT(*) FROM users r WHERE r.referrer_id = u.user_id) AS refs_count FROM users u WHERE u.blocked = FALSE ORDER BY refs_count DESC, u.user_id ASC LIMIT $1", limit)# ========== КЛАВИАТУРЫ ==========
def user_menu():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🎯 Заработать"), KeyboardButton(text="💰 Баланс")],
        [KeyboardButton(text="💸 Вывести"), KeyboardButton(text="👥 Рефералы")],
        [KeyboardButton(text="🎁 Промокод")],
    ], resize_keyboard=True, is_persistent=True)

def admin_reply_menu():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🎯 Заработать"), KeyboardButton(text="💰 Баланс")],
        [KeyboardButton(text="💸 Вывести"), KeyboardButton(text="👥 Рефералы")],
        [KeyboardButton(text="🛠 Админка")],
    ], resize_keyboard=True, is_persistent=True)

def get_menu(user_id): return admin_reply_menu() if user_id == ADMIN_ID else user_menu()

def sponsors_kb(links):
    kb = InlineKeyboardBuilder()
    for i, link in enumerate(links, 1): kb.button(text=f"🔗 {i}", url=link)
    kb.button(text="✅ Я подписался — проверить", callback_data="check_subs")
    kb.button(text="🔄 Обновить список", callback_data="rebuild_subs")
    rows = [2] * (len(links) // 2)
    if len(links) % 2: rows.append(1)
    rows.append(2); kb.adjust(*rows)
    return kb.as_markup()

def promo_kb():
    kb = InlineKeyboardBuilder(); kb.button(text="⬅️ Назад", callback_data="promo_cancel"); return kb.as_markup()

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
    kb.adjust(3, 3, 3, 3); return kb.as_markup()

def diag_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="🔍 Проверить сервисы", callback_data="diag_services")
    kb.button(text="⚠️ Ошибки за 3ч", callback_data="diag_errors")
    kb.button(text="⬅️ Назад", callback_data="diag_back")
    kb.adjust(2, 1); return kb.as_markup()

def top_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="💰 По балансу", callback_data="top_balance")
    kb.button(text="👥 По рефералам", callback_data="top_refs")
    kb.button(text="⬅️ Назад", callback_data="top_back")
    kb.adjust(2, 1); return kb.as_markup()

# ========== ВЫДАЧА ==========
async def issue_sponsors_message(user, chat_id, delete_msg_id=None, clean=True):
    uid = user.id
    if clean: await clear_pending_tasks(uid)
    cached = await get_cached_sponsors(uid)
    if cached: all_links = cached
    else:
        await collect_sponsors(user)
        all_links = (await pending_links(uid))[:await get_max_sponsors()]
        if all_links: await set_cached_sponsors(uid, all_links)

    if not all_links:
        return False, "😕 <b>Заданий пока нет, но ты молодец!</b>\n\nТы уже забрал всё, что было. Загляни через 15-20 минут — мы постоянно добавляем новых спонсоров 💎"

    reward = await get_reward(); total_reward = reward * len(all_links); min_wd = await get_min_withdraw()
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
        f"💡 <i>Чем больше спонсоров — тем больше монет!</i>"
    )
    if delete_msg_id:
        try: await bot.delete_message(chat_id, delete_msg_id)
        except Exception: pass
    try:
        await bot.send_message(chat_id, text, reply_markup=sponsors_kb(all_links), disable_web_page_preview=True)
    except Exception as e:
        await log_error("bot", "send_sponsors", str(e)); return False, "❌ Ошибка отправки. Попробуй позже."
    return True, None

# ========== ХЭНДЛЕРЫ ==========
@dp.message(CommandStart())
async def start_cmd(msg: types.Message):
    args = msg.text.split(); ref_id = None
    if len(args) > 1 and args[1].startswith("ref"):
        try: ref_id = int(args[1].replace("ref", ""))
        except Exception: pass
    un = msg.from_user.username or ""
    is_new = await register_user(msg.from_user.id, ref_id, un)
    u_check = await get_user(msg.from_user.id)
    if u_check and u_check["blocked"]:
        await msg.answer("🚫 <b>Ты заблокирован.</b>"); return
    if is_new and ref_id: await add_lazy_watch(msg.from_user.id, ref_id)

    try: loading = await msg.answer("⏳ <b>Собираю спонсоров…</b>\n\n<i>Это займёт 5-10 секунд</i>")
    except Exception: loading = None
    ok, err = await issue_sponsors_message(msg.from_user, msg.chat.id, delete_msg_id=loading.message_id if loading else None)

    if is_new:
        try:
            services_map = await pending_by_service(msg.from_user.id)
            await notify_admin_new_user(msg.from_user.id, f"@{un}" if un else "", msg.from_user.full_name or "", sum(services_map.values()), services_map, ref_id)
        except Exception as e: logging.error(f"notify new user failed: {e}")

    if not ok: await msg.answer(err, reply_markup=get_menu(msg.from_user.id))

@dp.message(Command("admin"))
async def cmd_admin(msg: types.Message):
    if msg.from_user.id != ADMIN_ID: return
    await msg.answer("🛠 <b>Админ-панель</b>", reply_markup=admin_menu())

@dp.message(F.text == "🛠 Админка")
async def msg_admin_panel(msg: types.Message):
    if msg.from_user.id != ADMIN_ID: return
    await msg.answer("🛠 <b>Админ-панель</b>", reply_markup=admin_menu())

@dp.message(F.text == "🎯 Заработать")
async def msg_earn(msg: types.Message):
    await update_username(msg.from_user.id, msg.from_user.username)
    try: loading = await msg.answer("⏳ <b>Собираю спонсоров…</b>")
    except Exception: loading = None
    ok, err = await issue_sponsors_message(msg.from_user, msg.chat.id, delete_msg_id=loading.message_id if loading else None)
    if not ok: await msg.answer(err, reply_markup=get_menu(msg.from_user.id))

@dp.message(F.text == "💰 Баланс")
async def msg_balance(msg: types.Message):
    u = await get_user(msg.from_user.id); bal = u["balance"] if u else 0
    min_wd = await get_min_withdraw()
    await msg.answer(f"💰 <b>Твой баланс</b>\n\n━━━━━━━━━━━━━━━\n💎 Доступно: <b>{fmt_money(bal)} монет</b>\n📤 Минимум вывода: <b>{fmt_money(min_wd)} монет</b>\n━━━━━━━━━━━━━━━", reply_markup=get_menu(msg.from_user.id))

@dp.message(F.text == "💸 Вывести")
async def msg_withdraw(msg: types.Message):
    u = await get_user(msg.from_user.id); bal = u["balance"] if u else 0
    min_wd = await get_min_withdraw()
    if bal < min_wd:
        await msg.answer(f"❌ <b>Недостаточно монет</b>\n\n📤 Минимум: <b>{fmt_money(min_wd)}</b>\n💰 У тебя: <b>{fmt_money(bal)}</b>", reply_markup=get_menu(msg.from_user.id)); return
    _states[msg.from_user.id] = "await_withdraw"
    await msg.answer(f"💸 <b>Вывод в Bytecoin</b>\n\n💰 Доступно: <b>{fmt_money(bal)} монет</b>\n\nОтправь сумму одним сообщением:\nНапример: <code>1500</code>", reply_markup=get_menu(msg.from_user.id))

@dp.message(F.text == "👥 Рефералы")
async def msg_refs(msg: types.Message):
    me = await bot.get_me(); ref_link = f"https://t.me/{me.username}?start=ref{msg.from_user.id}"
    pool = await get_pool()
    async with pool.acquire() as db:
        refs_count = (await db.fetchrow("SELECT COUNT(*) as c FROM users WHERE referrer_id=$1", msg.from_user.id))["c"]
        earned = (await db.fetchrow("SELECT COALESCE(referred_earned, 0) as s FROM users WHERE user_id=$1", msg.from_user.id))["s"]
    ref_bonus = await get_ref_bonus()
    text = f"👥 <b>Реферальная система</b>\n\n━━━━━━━━━━━━━━━\n💎 Бонус: <b>{fmt_money(ref_bonus)} монет</b> за каждого спонсора реферала\n━━━━━━━━━━━━━━━\n\n🔗 Твоя ссылка:\n<code>{ref_link}</code>\n\n👤 Приглашено: <b>{refs_count}</b>\n💰 Заработано: <b>{fmt_money(earned)} монет</b>"
    await msg.answer(text, reply_markup=get_menu(msg.from_user.id))

@dp.message(F.text == "🎁 Промокод")
async def msg_promo(msg: types.Message):
    if msg.from_user.id == ADMIN_ID: return
    _states[msg.from_user.id] = "await_promo"
    await msg.answer("🎁 <b>Активация промокода</b>\n\nОтправь код одним сообщением:", reply_markup=promo_kb())

@dp.callback_query(F.data == "promo_cancel")
async def cb_promo_cancel(cq: types.CallbackQuery):
    _states.pop(cq.from_user.id, None)
    u = await get_user(cq.from_user.id); bal = u["balance"] if u else 0
    await cq.answer("Отменено")
    try: await cq.message.edit_text("Отменено.")
    except Exception: pass
    await cq.message.answer(f"💰 Баланс: <b>{fmt_money(bal)} монет</b>", reply_markup=get_menu(cq.from_user.id))

# ========== ПРОВЕРКА ==========
@dp.callback_query(F.data == "check_subs")
async def cb_check(cq: types.CallbackQuery):
    uid = cq.from_user.id
    if time.time() - _last.get(uid, 0) < CHECK_COOLDOWN:
        await cq.answer("⏱ Подожди пару секунд"); return
    _last[uid] = time.time(); await cq.answer("Проверяю…")
    cnt, sm, skipped = await check_all(uid); left = await pending_count(uid)
    pool = await get_pool()
    async with pool.acquire() as db:
        total_done = (await db.fetchrow("SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status='subscribed'", uid))["c"]
        total_all = (await db.fetchrow("SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1", uid))["c"]
    u = await get_user(uid); bal = u["balance"] if u else 0
    min_wd = await get_min_withdraw(); reward = await get_reward()

    if left == 0 and total_all > 0:
        text = f"🎉 <b>Ты просто космос!</b>\n\nВсе <b>{total_done}</b> заданий выполнены 💎\n\n━━━━━━━━━━━━━━━\n💰 Баланс: <b>{fmt_money(bal)} монет</b>\n📤 Минимум: <b>{fmt_money(min_wd)} монет</b>\n━━━━━━━━━━━━━━━\n\n<i>Заглядывай позже!</i>"
        try: await cq.message.edit_text(text)
        except Exception: await cq.message.answer(text)
        await cq.message.answer("Меню 👇", reply_markup=get_menu(uid)); return

    if cnt > 0:
        text = f"✅ <b>Засчитано: {cnt}</b>\n\n💰 Начислено: <b>+{fmt_money(sm)} монет</b>\n💎 Баланс: <b>{fmt_money(bal)} монет</b>\n\n━━━━━━━━━━━━━━━\n📊 Прогресс: <b>{total_done} / {total_all}</b>\n━━━━━━━━━━━━━━━\n\n⚠️ Осталось: <b>{left}</b>\n💰 За каждого ещё: <b>{fmt_money(reward)} монет</b>"
    else:
        text = f"❌ <b>Новых подписок не найдено</b>\n\n📊 Прогресс: <b>{total_done} / {total_all}</b>\n\n⚠️ Осталось: <b>{left}</b> спонсоров\n\n<i>💡 Подпишись на ВСЕ кнопки и подожди 5-10 минут.</i>"

    kb = InlineKeyboardBuilder()
    kb.button(text="🔄 Обновить список", callback_data="rebuild_subs")
    kb.button(text="💰 Баланс", callback_data="balance_inline")
    kb.adjust(1)
    try: await cq.message.edit_text(text, reply_markup=kb.as_markup())
    except Exception: await cq.message.answer(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "rebuild_subs")
async def cb_rebuild(cq: types.CallbackQuery):
    uid = cq.from_user.id; await cq.answer("Обновляю…")
    await check_all(uid)
    try: await cq.message.delete()
    except Exception: pass
    ok, err = await issue_sponsors_message(cq.from_user, cq.message.chat.id, clean=True)
    if not ok: await cq.message.answer(err, reply_markup=get_menu(uid))

@dp.callback_query(F.data == "balance_inline")
async def cb_balance_inline(cq: types.CallbackQuery):
    u = await get_user(cq.from_user.id); bal = u["balance"] if u else 0
    await cq.answer()
    await cq.message.answer(f"💰 Баланс: <b>{fmt_money(bal)} монет</b>\n📤 Минимум: <b>{fmt_money(await get_min_withdraw())} монет</b>", reply_markup=get_menu(cq.from_user.id))

# ========== ТЕКСТОВЫЙ ХЭНДЛЕР ==========
@dp.message(F.text & ~F.text.startswith("/"))
async def text_handler(msg: types.Message):
    uid = msg.from_user.id; state = _states.get(uid)

    if state == "await_promo":
        code = msg.text.strip(); pool = await get_pool()
        async with pool.acquire() as db:
            promo = await db.fetchrow("SELECT * FROM promo_codes WHERE code=$1 AND active=TRUE", code)
            if not promo: await msg.answer("❌ Неверный промокод", reply_markup=get_menu(uid)); _states.pop(uid, None); return
            if promo["used_count"] >= promo["max_uses"]: await msg.answer("❌ Промокод закончился", reply_markup=get_menu(uid)); _states.pop(uid, None); return
            if await db.fetchrow("SELECT 1 FROM promo_uses WHERE user_id=$1 AND code=$2", uid, code): await msg.answer("❌ Ты уже активировал", reply_markup=get_menu(uid)); _states.pop(uid, None); return
            await db.execute("UPDATE users SET balance = balance + $1 WHERE user_id=$2", promo["amount"], uid)
            await db.execute("UPDATE promo_codes SET used_count = used_count + 1 WHERE code=$1", code)
            await db.execute("INSERT INTO promo_uses (user_id, code, used_at) VALUES ($1,$2,$3)", uid, code, time.time())
        _states.pop(uid, None)
        await msg.answer(f"✅ Промокод: <b>+{fmt_money(promo['amount'])} монет</b>", reply_markup=get_menu(uid)); return

    if state == "await_withdraw":
        try: amount = float(msg.text.replace(",", "."))
        except Exception: await msg.answer("❌ Не понял сумму", reply_markup=get_menu(uid)); return
        min_wd = await get_min_withdraw(); u = await get_user(uid); bal = u["balance"] if u else 0
        if amount < min_wd: await msg.answer(f"❌ Минимум {fmt_money(min_wd)} монет", reply_markup=get_menu(uid)); return
        if amount > bal: await msg.answer(f"❌ У тебя только {fmt_money(bal)} монет", reply_markup=get_menu(uid)); return
        _states.pop(uid, None); pool = await get_pool()
        async with pool.acquire() as db:
            if await db.execute("UPDATE users SET balance = balance - $1 WHERE user_id=$2 AND balance >= $1", amount, uid) == "UPDATE 0":
                await msg.answer("❌ Недостаточно", reply_markup=get_menu(uid)); return
            wid = (await db.fetchrow("INSERT INTO withdrawals (user_id, amount, status, created_at) VALUES ($1,$2,'pending',$3) RETURNING id", uid, amount, time.time()))["id"]
        await msg.answer(f"⏳ Вывод {fmt_money(amount)} монет обрабатывается…", reply_markup=get_menu(uid))
        resp = await bc_transfer(uid, amount)
        if resp.get("status") == "ok":
            pool = await get_pool()
            async with pool.acquire() as db: await db.execute("UPDATE withdrawals SET status='done', tx_id=$1 WHERE id=$2", resp.get("transaction_id"), wid)
            await msg.answer(f"✅ Выведено: <b>{fmt_money(amount)} монет</b>\nTX: <code>{str(resp.get('transaction_id',''))[:16]}…</code>\n\n💎 <a href=\"https://t.me/byteappbot\">@byteappbot</a>", reply_markup=get_menu(uid))
            await notify_admin(f"💸 Выплата\n👤 {await get_user_display(uid)}\n💰 {fmt_money(amount)} монет\nTX: <code>{resp.get('transaction_id','')}</code>")
        else:
            pool = await get_pool()
            async with pool.acquire() as db:
                await db.execute("UPDATE users SET balance = balance + $1 WHERE user_id=$2", amount, uid)
                await db.execute("UPDATE withdrawals SET status='failed', error=$1 WHERE id=$2", resp.get("error", ""), wid)
            await msg.answer(f"❌ Ошибка: {resp.get('error','unknown')}\nСредства возвращены.", reply_markup=get_menu(uid))
        return

    # ===== АДМИН =====
    if state == "adm_reward":
        try:
            val = float(msg.text.replace(",", "."))
            if val <= 0: await msg.answer("❌ > 0"); return
        except Exception: await msg.answer("❌ Число"); return
        await set_setting("reward", val); _states.pop(uid, None)
        await msg.answer(f"✅ Награда: <b>{fmt_money(val)} монет</b>", reply_markup=admin_menu()); return

    if state == "adm_max":
        try: val = int(msg.text)
        except Exception: await msg.answer("❌ Целое"); return
        if val > MAX_MAX_SPONSORS or val < 1: await msg.answer(f"❌ От 1 до {MAX_MAX_SPONSORS}"); return
        await set_setting("max_sponsors", val); _states.pop(uid, None)
        await msg.answer(f"✅ Макс: <b>{val}</b>", reply_markup=admin_menu()); return

    if state == "adm_minw":
        try:
            val = float(msg.text.replace(",", "."))
            if val <= 0: await msg.answer("❌ > 0"); return
        except Exception: await msg.answer("❌ Число"); return
        await set_setting("min_withdraw", val); _states.pop(uid, None)
        await msg.answer(f"✅ Мин. вывод: <b>{fmt_money(val)} монет</b>", reply_markup=admin_menu()); return

    if state == "adm_refb":
        try:
            val = float(msg.text.replace(",", "."))
            if val < 0: await msg.answer("❌ ≥ 0"); return
        except Exception: await msg.answer("❌ Число"); return
        await set_setting("ref_bonus", val); _states.pop(uid, None)
        await msg.answer(f"✅ Реф-бонус: <b>{fmt_money(val)} монет</b>", reply_markup=admin_menu()); return

    if state == "adm_broadcast":
        _states.pop(uid, None); await msg.answer("📢 Рассылка…")
        pool = await get_pool()
        async with pool.acquire() as db: rows = await db.fetch("SELECT user_id FROM users WHERE blocked=FALSE")
        ok = 0; fail = 0
        for r in rows:
            try: await msg.copy_to(r["user_id"]); ok += 1
            except Exception: fail += 1
            await asyncio.sleep(0.05)
        await msg.answer(f"✅ Отправлено: <b>{ok}</b>\n❌ Ошибок: <b>{fail}</b>", reply_markup=admin_menu()); return

    if state == "adm_promo_create":
        parts = [p.strip() for p in msg.text.split("|")]
        if len(parts) != 3: await msg.answer("❌ Формат: <code>КОД | СУММА | АКТИВАЦИЙ</code>"); return
        code, amount_s, uses_s = parts
        try: amount = float(amount_s); uses = int(uses_s)
        except Exception: await msg.answer("❌ Числа"); return
        pool = await get_pool()
        async with pool.acquire() as db:
            try: await db.execute("INSERT INTO promo_codes (code, amount, max_uses, created_at) VALUES ($1,$2,$3,$4)", code, amount, uses, time.time())
            except Exception: await msg.answer("❌ Такой код уже есть"); return
        _states.pop(uid, None)
        await msg.answer(f"✅ Промокод <code>{code}</code>: {fmt_money(amount)} × {uses}", reply_markup=admin_menu()); return

    if state == "adm_find":
        query = msg.text.strip(); _states.pop(uid, None)
        target = await find_user(query)
        if not target: await msg.answer(f"❌ Юзер <code>{escape_html(query)}</code> не найден", reply_markup=admin_menu()); return
        u = await get_user(target)
        if not u: await msg.answer(f"❌ Юзер не найден", reply_markup=admin_menu()); return
        pool = await get_pool()
        async with pool.acquire() as db:
            refs = (await db.fetchrow("SELECT COUNT(*) as c FROM users WHERE referrer_id=$1", target))["c"]
            withdrawn = (await db.fetchrow("SELECT COALESCE(SUM(amount),0) as s FROM withdrawals WHERE user_id=$1 AND status='done'", target))["s"]
            subs_total = (await db.fetchrow("SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status='subscribed'", target))["c"]
        user_str = user_display(target, u["username"]); ref_str = await get_user_display(u["referrer_id"]) if u["referrer_id"] else "—"
        text = f"🔎 <b>Инфо о юзере</b>\n\n👤 {user_str}\n\n💰 Баланс: <b>{fmt_money(u['balance'])} монет</b>\n📊 Всего подписок: <b>{subs_total}</b>\n👥 Рефералов: <b>{refs}</b>\n🔗 Реферер: {ref_str}\n💸 Выведено: <b>{fmt_money(withdrawn)} монет</b>\n🚫 Бан: {'Да' if u['blocked'] else 'Нет'}\n\nКоманды:\n<code>/ban {target}</code>\n<code>/unban {target}</code>\n<code>/give {target} 500</code>"
        await msg.answer(text, reply_markup=admin_menu()); return

    if state == "adm_give":
        parts = msg.text.strip().split()
        if len(parts) != 2: await msg.answer("❌ Формат: <code>ID_или_@username СУММА</code>"); return
        query, amount_s = parts; target = await find_user(query)
        if not target: await msg.answer(f"❌ Юзер <code>{escape_html(query)}</code> не найден"); return
        try: amount = float(amount_s.replace(",", "."))
        except Exception: await msg.answer("❌ Неверная сумма"); return
        await add_balance(target, amount); _states.pop(uid, None)
        await msg.answer(f"✅ Начислено <b>{fmt_money(amount)} монет</b> → {await get_user_display(target)}", reply_markup=admin_menu())
        try: await bot.send_message(target, f"🎁 Тебе начислено: <b>+{fmt_money(amount)} монет</b>")
        except Exception: pass
        return

@dp.message(Command("ban"))
async def cmd_ban(msg: types.Message):
    if msg.from_user.id != ADMIN_ID: return
    try: query = msg.text.split()[1]
    except Exception: await msg.answer("Формат: <code>/ban ID</code>"); return
    target = await find_user(query)
    if not target: await msg.answer("❌ Не найден"); return
    pool = await get_pool()
    async with pool.acquire() as db: await db.execute("UPDATE users SET blocked=TRUE WHERE user_id=$1", target)
    await msg.answer(f"🚫 Забанен: {await get_user_display(target)}", reply_markup=admin_menu())

@dp.message(Command("unban"))
async def cmd_unban(msg: types.Message):
    if msg.from_user.id != ADMIN_ID: return
    try: query = msg.text.split()[1]
    except Exception: await msg.answer("Формат: <code>/unban ID</code>"); return
    target = await find_user(query)
    if not target: await msg.answer("❌ Не найден"); return
    pool = await get_pool()
    async with pool.acquire() as db: await db.execute("UPDATE users SET blocked=FALSE WHERE user_id=$1", target)
    await msg.answer(f"✅ Разбанен: {await get_user_display(target)}", reply_markup=admin_menu())

@dp.message(Command("give"))
async def cmd_give(msg: types.Message):
    if msg.from_user.id != ADMIN_ID: return
    try:
        parts = msg.text.split(); query = parts[1]; amount = float(parts[2].replace(",", "."))
    except Exception: await msg.answer("Формат: <code>/give ID СУММА</code>"); return
    target = await find_user(query)
    if not target: await msg.answer("❌ Не найден"); return
    await add_balance(target, amount)
    await msg.answer(f"✅ Начислено <b>{fmt_money(amount)}</b> → {await get_user_display(target)}", reply_markup=admin_menu())
    try: await bot.send_message(target, f"🎁 Тебе начислено: <b>+{fmt_money(amount)} монет</b>")
    except Exception: pass

# ========== МЕДИА РАССЫЛКА ==========
@dp.message((F.photo | F.video | F.document | F.animation | F.sticker | F.voice) & F.from_user.id == ADMIN_ID)
async def media_broadcast_handler(msg: types.Message):
    uid = msg.from_user.id
    if _states.get(uid) != "adm_broadcast": return
    _states.pop(uid, None)
    await msg.answer("📢 Начинаю рассылку медиа…")
    pool = await get_pool()
    async with pool.acquire() as db: rows = await db.fetch("SELECT user_id FROM users WHERE blocked=FALSE")
    ok = 0; fail = 0
    for r in rows:
        try: await msg.copy_to(r["user_id"]); ok += 1
        except Exception: fail += 1
        await asyncio.sleep(0.05)
    await msg.answer(f"✅ Отправлено: <b>{ok}</b>\n❌ Ошибок: <b>{fail}</b>", reply_markup=admin_menu())

# ========== АДМИН CALLBACKS ==========
@dp.callback_query(F.data == "adm_reward")
async def adm_reward(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_reward"; await cq.answer()
    try: await cq.message.edit_text(f"💰 Текущая: <b>{fmt_money(await get_reward())} монет</b>\n\nНовое значение:")
    except Exception: pass

@dp.callback_query(F.data == "adm_max")
async def adm_max(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_max"; await cq.answer()
    try: await cq.message.edit_text(f"👥 Текущий: <b>{await get_max_sponsors()}</b>\n\nОтправь число 1-{MAX_MAX_SPONSORS}:")
    except Exception: pass

@dp.callback_query(F.data == "adm_minw")
async def adm_minw(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_minw"; await cq.answer()
    try: await cq.message.edit_text(f"💸 Мин. вывод: <b>{fmt_money(await get_min_withdraw())} монет</b>\n\nНовое значение:")
    except Exception: pass

@dp.callback_query(F.data == "adm_refb")
async def adm_refb(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_refb"; await cq.answer()
    try: await cq.message.edit_text(f"👥 Реф-бонус: <b>{fmt_money(await get_ref_bonus())} монет</b>\n\nНовое значение:")
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
    try: await cq.message.edit_text(f"📊 <b>Статистика</b>\n\n━━━━━━━━━━━━━━━\n👥 Юзеров: <b>{total}</b> (🚫 {blocked})\n💰 Балансы: <b>{fmt_money(balances)} монет</b>\n✅ Подписок: <b>{subs}</b>\n💸 Выплачено: <b>{fmt_money(paid)} монет</b>\n📤 Pending: <b>{pend}</b>\n━━━━━━━━━━━━━━━\n⚠️ Ошибок за 24ч: <b>{errs}</b>", reply_markup=admin_menu())
    except Exception: pass

@dp.callback_query(F.data == "adm_find")
async def adm_find(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_find"; await cq.answer()
    try: await cq.message.edit_text("🔎 <b>Найти юзера</b>\n\nОтправь <b>ID</b> или <b>@username</b>:")
    except Exception: pass

@dp.callback_query(F.data == "adm_give")
async def adm_give(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_give"; await cq.answer()
    try: await cq.message.edit_text("💎 <b>Начислить</b>\n\nФормат: <code>ID_или_@username СУММА</code>")
    except Exception: pass

@dp.callback_query(F.data == "adm_broadcast")
async def adm_broadcast(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_broadcast"; await cq.answer()
    try: await cq.message.edit_text("📢 Отправь сообщение или фото для рассылки:")
    except Exception: pass

@dp.callback_query(F.data == "adm_promo_create")
async def adm_promo_create(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_promo_create"; await cq.answer()
    try: await cq.message.edit_text("Формат: <code>КОД | СУММА | АКТИВАЦИЙ</code>\nПример: <code>SALE | 500 | 100</code>")
    except Exception: pass

@dp.callback_query(F.data == "adm_withdraws")
async def adm_withdraws(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch("SELECT w.id, w.user_id, w.amount, w.status, w.tx_id, u.username FROM withdrawals w LEFT JOIN users u ON u.user_id = w.user_id ORDER BY w.id DESC LIMIT 20")
    if not rows: await cq.answer("Нет заявок", show_alert=True); return
    text = "💸 <b>Последние выводы:</b>\n\n"
    for r in rows:
        emoji = {"done": "✅", "pending": "⏳", "failed": "❌"}.get(r["status"], "•")
        text += f"{emoji} #{r['id']} · {user_display(r['user_id'], r['username'])} · <b>{fmt_money(r['amount'])}</b>\n"
    await cq.answer()
    try: await cq.message.edit_text(text, reply_markup=admin_menu())
    except Exception: pass

@dp.callback_query(F.data == "adm_diag")
async def adm_diag(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    await cq.answer()
    try: await cq.message.edit_text("📊 <b>Диагностика</b>\n\nВыбери, что проверить:", reply_markup=diag_kb())
    except Exception: await cq.message.answer("📊 <b>Диагностика</b>", reply_markup=diag_kb())

@dp.callback_query(F.data == "diag_back")
async def cb_diag_back(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    await cq.answer()
    try: await cq.message.edit_text("🛠 <b>Админ-панель</b>", reply_markup=admin_menu())
    except Exception: pass

@dp.callback_query(F.data == "diag_services")
async def cb_diag_services(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    await cq.answer("Проверяю…")
    try: await cq.message.edit_text("🔎 <b>Проверяю сервисы…</b>")
    except Exception: pass
    try: report = await run_diagnostics()
    except Exception as e: report = f"❌ Ошибка: {e}"
    if len(report) > 4000: report = report[:4000] + "…"
    kb = InlineKeyboardBuilder()
    kb.button(text="🔄 Повторить", callback_data="diag_services")
    kb.button(text="⚠️ Ошибки", callback_data="diag_errors")
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
    except Exception as e: await cq.answer(f"Ошибка БД: {e}", show_alert=True); return
    if not rows: text = "✅ <b>Ошибок нет!</b>"
    else:
        text = f"⚠️ <b>Ошибки за 3 часа:</b> ({len(rows)})\n\n"
        for r in rows[:15]:
            ts = time.strftime("%d.%m %H:%M", time.localtime(r["created_at"]))
            text += f"<b>{escape_html(r['service'])}</b> · {escape_html(r['error_type'])}\n<code>{escape_html((r['message'] or '')[:100])}</code>\n<i>{ts}</i>\n\n"
    kb = InlineKeyboardBuilder()
    kb.button(text="🔄 Обновить", callback_data="diag_errors")
    kb.button(text="🔍 Сервисы", callback_data="diag_services")
    kb.button(text="⬅️ Назад", callback_data="diag_back")
    kb.adjust(2, 1)
    try: await cq.message.edit_text(text, reply_markup=kb.as_markup())
    except Exception:
        try: await cq.message.answer(text, reply_markup=kb.as_markup())
        except Exception: pass

@dp.callback_query(F.data == "adm_top")
async def adm_top(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    await cq.answer()
    try: await cq.message.edit_text("🏆 <b>Топ игроков</b>", reply_markup=top_kb())
    except Exception: await cq.message.answer("🏆 <b>Топ</b>", reply_markup=top_kb())

@dp.callback_query(F.data == "top_back")
async def cb_top_back(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    await cq.answer()
    try: await cq.message.edit_text("🛠 <b>Админ-панель</b>", reply_markup=admin_menu())
    except Exception: pass

@dp.callback_query(F.data == "top_balance")
async def cb_top_balance(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    await cq.answer()
    rows = await get_top_by_balance(15)
    text = "🏆 <b>Топ-15 по балансу</b>\n\n" + "".join(f"{i}. {user_display(r['user_id'], r['username'])} — <b>{fmt_money(r['balance'])}</b>\n" for i, r in enumerate(rows, 1)) if rows else "🏆 <b>Топ</b>\n\n📭 Пусто."
    kb = InlineKeyboardBuilder()
    kb.button(text="👥 По рефералам", callback_data="top_refs")
    kb.button(text="🔄 Обновить", callback_data="top_balance")
    kb.button(text="⬅️ Назад", callback_data="top_back")
    kb.adjust(2, 1)
    try: await cq.message.edit_text(text[:4000], reply_markup=kb.as_markup())
    except Exception: pass

@dp.callback_query(F.data == "top_refs")
async def cb_top_refs(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    await cq.answer()
    rows = await get_top_by_refs(15)
    text = "🏆 <b>Топ-15 по рефералам</b>\n\n" + "".join(f"{i}. {user_display(r['user_id'], r['username'])} — <b>{r['refs_count'] or 0}</b> реф.\n" for i, r in enumerate(rows, 1)) if rows else "🏆 <b>Топ</b>\n\n📭 Пусто."
    kb = InlineKeyboardBuilder()
    kb.button(text="💰 По балансу", callback_data="top_balance")
    kb.button(text="🔄 Обновить", callback_data="top_refs")
    kb.button(text="⬅️ Назад", callback_data="top_back")
    kb.adjust(2, 1)
    try: await cq.message.edit_text(text[:4000], reply_markup=kb.as_markup())
    except Exception: pass

# ========== WEB + MAIN ==========
async def health(_): return web.Response(text="ok")

async def start_web():
    app = web.Application()
    app.router.add_get("/", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    logging.info(f"Web on :{PORT}")

async def on_shutdown(*args, **kwargs):
    global _http
    if _http and not _http.closed: await _http.close()

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
