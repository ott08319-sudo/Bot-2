import os
import time
import uuid
import asyncio
import logging
import json
import aiohttp
import asyncpg
from decimal import Decimal
from aiohttp import web
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

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
SELF_URL        = os.getenv("SELF_URL", "")

BC_API_KEY      = os.getenv("BYTECOIN_API_KEY", "")
BC_ENABLED      = os.getenv("BC_ENABLED", "true").lower() == "true"
BC_BASE         = "https://bytecoin.space/api/public/v1"
BC_MIN_TRANSFER = Decimal("0.0000001")

PIARFLOW_API_KEY = os.getenv("PIARFLOW_API_KEY", "")
TGRASS_API_KEY   = os.getenv("TGRASS_API_KEY", "")
TRAFSLY_API_KEY  = os.getenv("TRAFSLY_API_KEY", "")
AXIONNA_API_KEY  = os.getenv("AXIONNA_API_KEY", "")
FLYER_API_KEY    = os.getenv("FLYER_API_KEY", "")

flyer = Flyer(FLYER_API_KEY) if (FLYER_AVAILABLE and FLYER_API_KEY) else None

CHECK_COOLDOWN       = 5
DEFAULT_REWARD       = 300.0
DEFAULT_MAX_SPONSORS = 25
MAX_MAX_SPONSORS     = 60
MIN_WITHDRAW         = 1000.0
REF_PERCENT          = 50
HOLD_HOURS           = 48
HOLD_SECONDS         = HOLD_HOURS * 3600
CACHE_TTL            = 300
AUTOCHECK_INTERVAL   = 3600
MAX_RETRIES          = 3

INCOMPLETE_STATUSES = ('incomplete', 'abort')

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


# ========== РЕТРАИ ==========
async def retry_request(func, *args, **kwargs):
    for attempt in range(MAX_RETRIES):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logging.warning(f"retry {attempt+1}/{MAX_RETRIES}: {e}")
            if attempt == MAX_RETRIES - 1:
                await log_error("retry", "failed", str(e))
                return None
            await asyncio.sleep(2 ** attempt)
    return None


async def log_error(service, error_type, message):
    try:
        pool = await get_pool()
        async with pool.acquire() as db:
            await db.execute("""
                INSERT INTO error_log (service, error_type, message, created_at)
                VALUES ($1, $2, $3, $4)
            """, service, error_type, message[:500], time.time())
    except Exception as e:
        logging.error(f"log_error failed: {e}")


async def notify_admin(text):
    try:
        await bot.send_message(ADMIN_ID, text)
    except Exception:
        pass


# ========== BYTECOIN ==========
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
    if not BC_ENABLED:
        return {"status": "error", "code": "DISABLED", "error": "disabled"}
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


# ========== БД ==========
async def init_db():
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            balance DOUBLE PRECISION DEFAULT 0,
            held DOUBLE PRECISION DEFAULT 0,
            referrer_id BIGINT,
            referred_earned DOUBLE PRECISION DEFAULT 0,
            blocked BOOLEAN DEFAULT FALSE,
            created_at DOUBLE PRECISION)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS sponsor_tasks (
            id SERIAL PRIMARY KEY,
            user_id BIGINT, service TEXT, assignment_id TEXT, link TEXT,
            reward DOUBLE PRECISION DEFAULT 0, status TEXT DEFAULT 'unsubscribed',
            signature TEXT, created_at DOUBLE PRECISION,
            UNIQUE(user_id, service, assignment_id))""")
        await db.execute("ALTER TABLE sponsor_tasks ADD COLUMN IF NOT EXISTS session_id TEXT")
        await db.execute("ALTER TABLE sponsor_tasks ADD COLUMN IF NOT EXISTS task_type TEXT")
        await db.execute("""CREATE TABLE IF NOT EXISTS reward_holds (
            id SERIAL PRIMARY KEY,
            user_id BIGINT, service TEXT, assignment_id TEXT, amount DOUBLE PRECISION,
            unlock_at DOUBLE PRECISION, status TEXT DEFAULT 'holding',
            created_at DOUBLE PRECISION,
            UNIQUE(user_id, service, assignment_id))""")
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
        await db.execute("INSERT INTO settings (key,value) VALUES ('reward',$1) ON CONFLICT (key) DO NOTHING", str(DEFAULT_REWARD))
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


async def get_max_sponsors():
    return int(await get_setting("max_sponsors", DEFAULT_MAX_SPONSORS))


async def register_user(user_id, referrer_id=None):
    pool = await get_pool()
    async with pool.acquire() as db:
        exists = await db.fetchrow("SELECT user_id FROM users WHERE user_id=$1", user_id)
        if exists: return
        if referrer_id == user_id: referrer_id = None
        if referrer_id:
            r = await db.fetchrow("SELECT user_id FROM users WHERE user_id=$1", referrer_id)
            if not r: referrer_id = None
        await db.execute("INSERT INTO users (user_id, referrer_id, created_at) VALUES ($1,$2,$3)", user_id, referrer_id, time.time())


async def get_user(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        return await db.fetchrow("SELECT balance, held, referrer_id, referred_earned, blocked FROM users WHERE user_id=$1", user_id)


async def add_balance(user_id, amount):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE users SET balance = balance + $1 WHERE user_id=$2", amount, user_id)


async def add_hold(user_id, amount):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE users SET held = held + $1 WHERE user_id=$2", amount, user_id)


async def remove_hold(user_id, amount):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE users SET held = GREATEST(held - $1, 0) WHERE user_id=$2", amount, user_id)


async def save_sponsor(user_id, service, aid, link, reward, signature=None, session_id=None, task_type=None):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("""INSERT INTO sponsor_tasks
            (user_id, service, assignment_id, link, reward, signature, session_id, task_type, status, created_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'unsubscribed',$9)
            ON CONFLICT (user_id, service, assignment_id) DO NOTHING""",
            user_id, service, str(aid), link, reward,
            str(signature) if signature is not None else None,
            str(session_id) if session_id is not None else None,
            task_type, time.time())


async def mark_subscribed(user_id, service, aid):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE sponsor_tasks SET status='subscribed' WHERE user_id=$1 AND service=$2 AND assignment_id=$3", user_id, service, aid)


async def clear_pending_tasks(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("DELETE FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed'", user_id)
        await db.execute("DELETE FROM sponsor_cache WHERE cache_key=$1", f"all_sponsors_{user_id}")


async def pending_tasks(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch("""SELECT service, assignment_id, link, reward, signature, session_id, task_type
            FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed'""", user_id)
        return [(r["service"], r["assignment_id"], r["link"], r["reward"],
                 r["signature"], r["session_id"], r["task_type"]) for r in rows]


async def pending_count(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed'", user_id)
        return row["c"]


async def pending_links(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch("SELECT link FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed'", user_id)
        return [r["link"] for r in rows]


# ========== КЭШ ==========
async def get_cached_sponsors(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT payload, expires_at FROM sponsor_cache WHERE cache_key=$1", f"all_sponsors_{user_id}")
        if row and row["expires_at"] > time.time():
            try:
                return json.loads(row["payload"])
            except Exception:
                return None
    return None


async def set_cached_sponsors(user_id, links):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("""INSERT INTO sponsor_cache (cache_key, payload, expires_at)
            VALUES ($1,$2,$3) ON CONFLICT (cache_key) DO UPDATE SET payload=$2, expires_at=$3""",
            f"all_sponsors_{user_id}", json.dumps(links), time.time() + CACHE_TTL)


# ========== PIARFLOW ==========
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


# ========== TGRASS ==========
async def get_tgrass(user_id, username):
    if not TGRASS_API_KEY: return []
    s = await http()
    try:
        async with s.post("https://tgrass.space/offers",
            json={"tg_user_id": user_id, "tg_login": username or "", "lang": "ru", "is_premium": False},
            headers={"Auth": TGRASS_API_KEY}) as r:
            d = await r.json()
            if d.get("status") == "not_ok": return d.get("offers", [])
    except Exception as e:
        await log_error("tgrass", "fetch", str(e))
    return []


async def check_tgrass(user_id):
    if not TGRASS_API_KEY: return False
    s = await http()
    try:
        async with s.post("https://tgrass.space/offers",
            json={"tg_user_id": user_id}, headers={"Auth": TGRASS_API_KEY}) as r:
            d = await r.json()
            return d.get("status") == "ok"
    except Exception as e:
        await log_error("tgrass", "check", str(e))
    return False


# ========== TRAFSLY ==========
async def get_trafsly(user_id, username):
    if not TRAFSLY_API_KEY: return []
    payload = {"user_id": user_id, "max_sponsors": await get_max_sponsors(),
               "language_code": "ru", "is_premium": False}
    if username: payload["username"] = username
    s = await http()
    try:
        async with s.post("https://api.trafsly.com/api/v1/get-sponsors",
            json=payload, headers={"Auth": TRAFSLY_API_KEY}) as r:
            d = await r.json()
            if d.get("status") == "warning": return d.get("sponsors", [])
    except Exception as e:
        await log_error("trafsly", "fetch", str(e))
    return []


async def check_trafsly(user_id, ads_ids):
    if not TRAFSLY_API_KEY or not ads_ids: return []
    s = await http()
    res = []
    for aid in ads_ids:
        try:
            async with s.post("https://api.trafsly.com/api/v1/confirm-subscription",
                json={"user_id": user_id, "ads_id": int(aid)},
                headers={"Auth": TRAFSLY_API_KEY}) as r:
                d = await r.json()
                if d.get("subscribed") is True:
                    res.append({"ads_id": aid, "status": "subscribed"})
                elif d.get("subscribed") is False:
                    if d.get("status") == "error" or "not verified" in str(d.get("message", "")).lower():
                        res.append({"ads_id": aid, "status": "subscribed"})
                    else:
                        res.append({"ads_id": aid, "status": "unsubscribed"})
        except Exception as e:
            await log_error("trafsly", f"check_{aid}", str(e))
    return res


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
            if d.get("status") == "ok":
                return d.get("sponsors", [])
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
                return {str(x.get("id")): (x.get("status") == "ok") for x in d.get("results", [])}
    except Exception as e:
        await log_error("axionna", "check", str(e))
    return {}


# ========== FLYER ==========
async def fetch_flyer(user_id, lang="ru"):
    if not flyer: return []
    try:
        tasks = await flyer.get_tasks(user_id=user_id, language_code=lang or "ru", limit=await get_max_sponsors())
        logging.info(f"FLYER get_tasks: {len(tasks) if tasks else 0}")
        if not tasks: return []
        incomplete = [t for t in tasks if t.get('status') in INCOMPLETE_STATUSES]
        out = []
        for t in incomplete:
            links = t.get('links', [])
            if not links: continue
            out.append({
                "service": "flyer",
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


# ========== СБОР СПОНСОРОВ ==========
async def collect_sponsors(user):
    uid = user.id
    un = user.username or ""
    lang = user.language_code or "ru"
    reward = await get_reward()
    links = []

    for x in await get_piarflow(uid):
        link = x.get("link")
        if link:
            await save_sponsor(uid, "piarflow", link, link, reward)
            links.append(link)

    for x in await get_tgrass(uid, un):
        link = x.get("link")
        if link:
            await save_sponsor(uid, "tgrass", str(x.get("offer_id")), link, reward)
            links.append(link)

    for x in await get_trafsly(uid, un):
        link = x.get("link")
        aid = x.get("ads_id")
        if link:
            await save_sponsor(uid, "trafsly", str(aid) if aid else link, link, reward)
            links.append(link)

    for x in await get_axionna(uid):
        link = x.get("link")
        aid = x.get("id")
        if link and aid:
            await save_sponsor(uid, "axionna", str(aid), link, reward)
            links.append(link)

    # Flyer
    for x in await fetch_flyer(uid, lang):
        link = x.get("link")
        aid = x.get("id")
        if link and aid:
            await save_sponsor(uid, "flyer", str(aid), link, reward,
                               signature=x.get("signature"),
                               task_type=x.get("task"))
            links.append(link)

    return links


# ========== ПРОВЕРКА + ХОЛД ==========
async def _task_reward(user_id, service, aid):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT reward FROM sponsor_tasks WHERE user_id=$1 AND service=$2 AND assignment_id=$3", user_id, service, aid)
        return row["reward"] if row else 0.0


async def _create_hold(user_id, service, aid, amount):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("""INSERT INTO reward_holds (user_id, service, assignment_id, amount, unlock_at, status, created_at)
            VALUES ($1,$2,$3,$4,$5,'holding',$6)
            ON CONFLICT (user_id, service, assignment_id) DO NOTHING""",
            user_id, service, aid, amount, time.time() + HOLD_SECONDS, time.time())
    await add_hold(user_id, amount)


async def check_all(user_id):
    tasks = await pending_tasks(user_id)
    if not tasks: return 0, 0.0

    pf, ts, ax, fl = [], [], [], []
    for service, aid, link, reward, sig, session_id, task_type in tasks:
        if service == "piarflow" and link: pf.append(link)
        elif service == "trafsly" and aid: ts.append(aid)
        elif service == "axionna" and aid: ax.append(aid)
        elif service == "flyer" and sig: fl.append((aid, sig))

    done_count = 0
    done_sum = 0.0

    if pf:
        for r in await check_piarflow(user_id, pf):
            if r.get("status") in ("subscribed", "not_counted"):
                await mark_subscribed(user_id, "piarflow", r.get("link"))
                rw = await _task_reward(user_id, "piarflow", r.get("link"))
                await _create_hold(user_id, "piarflow", r.get("link"), rw)
                done_count += 1; done_sum += rw

    if ts:
        for r in await check_trafsly(user_id, ts):
            if r.get("status") == "subscribed":
                aid = str(r.get("ads_id"))
                await mark_subscribed(user_id, "trafsly", aid)
                rw = await _task_reward(user_id, "trafsly", aid)
                await _create_hold(user_id, "trafsly", aid, rw)
                done_count += 1; done_sum += rw

    if await check_tgrass(user_id):
        pool = await get_pool()
        async with pool.acquire() as db:
            rows = await db.fetch("SELECT assignment_id, reward FROM sponsor_tasks WHERE user_id=$1 AND service='tgrass' AND status!='subscribed'", user_id)
        for r in rows:
            await mark_subscribed(user_id, "tgrass", r["assignment_id"])
            await _create_hold(user_id, "tgrass", r["assignment_id"], r["reward"])
            done_count += 1; done_sum += r["reward"]

    if ax:
        res = await check_axionna(user_id, ax)
        for aid, ok in res.items():
            if ok:
                await mark_subscribed(user_id, "axionna", aid)
                rw = await _task_reward(user_id, "axionna", aid)
                await _create_hold(user_id, "axionna", aid, rw)
                done_count += 1; done_sum += rw

    for aid, sig in fl:
        status = await check_flyer(sig)
        if status == "complete":
            await mark_subscribed(user_id, "flyer", aid)
            rw = await _task_reward(user_id, "flyer", aid)
            await _create_hold(user_id, "flyer", aid, rw)
            done_count += 1; done_sum += rw

    return done_count, done_sum


# ========== АВТОПРОВЕРКА НОВЫХ ЗАДАНИЙ ==========
async def autocheck_worker():
    await asyncio.sleep(300)
    while True:
        try:
            enabled = await get_setting("autocheck", "true")
            if enabled == "true":
                pool = await get_pool()
                async with pool.acquire() as db:
                    rows = await db.fetch("""
                        SELECT user_id FROM users
                        WHERE blocked = FALSE AND created_at > $1
                        LIMIT 500
                    """, time.time() - 86400)

                for r in rows:
                    uid = r["user_id"]
                    try:
                        before = await pending_count(uid)
                        user = types.User(id=uid, is_bot=False, first_name="", language_code="ru")
                        await collect_sponsors(user)
                        after = await pending_count(uid)
                        if after > before:
                            new_count = after - before
                            try:
                                await bot.send_message(uid,
                                    f"🔔 <b>Новые задания ждут тебя!</b>\n\n"
                                    f"🎯 Доступно: <b>{new_count}</b>\n"
                                    f"Нажми «🎯 Заработать», чтобы забрать награду.",
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


# ========== ХОЛД-ВОРКЕР ==========
async def hold_worker():
    while True:
        await asyncio.sleep(600)
        try:
            pool = await get_pool()
            async with pool.acquire() as db:
                holds = await db.fetch("SELECT id, user_id, service, assignment_id, amount FROM reward_holds WHERE status='holding' AND unlock_at <= $1", time.time())
            for h in holds:
                still = True
                uid, service, aid = h["user_id"], h["service"], h["assignment_id"]
                try:
                    if service == "piarflow":
                        r = await check_piarflow(uid, [aid])
                        still = any(x.get("status") in ("subscribed", "not_counted") for x in r)
                    elif service == "tgrass":
                        still = await check_tgrass(uid)
                    elif service == "trafsly":
                        r = await check_trafsly(uid, [aid])
                        still = any(x.get("status") == "subscribed" for x in r)
                    elif service == "axionna":
                        res = await check_axionna(uid, [aid])
                        still = res.get(aid, False)
                    elif service == "flyer":
                        pool = await get_pool()
                        async with pool.acquire() as db:
                            sig_row = await db.fetchrow("SELECT signature FROM sponsor_tasks WHERE user_id=$1 AND service='flyer' AND assignment_id=$2", uid, aid)
                        sig = sig_row["signature"] if sig_row else None
                        if sig:
                            status = await check_flyer(sig)
                            still = (status == "complete")
                except Exception as e:
                    logging.error(f"hold {h['id']}: {e}")
                    still = True

                pool = await get_pool()
                async with pool.acquire() as db:
                    if still:
                        await db.execute("UPDATE reward_holds SET status='released' WHERE id=$1", h["id"])
                        await db.execute("UPDATE users SET balance = balance + $1 WHERE user_id=$2", h["amount"], uid)
                    else:
                        await db.execute("UPDATE reward_holds SET status='cancelled' WHERE id=$1", h["id"])
                await remove_hold(uid, h["amount"])

                if still:
                    u = await get_user(uid)
                    if u and u["referrer_id"]:
                        ref_bonus = h["amount"] * REF_PERCENT / 100
                        await add_balance(u["referrer_id"], ref_bonus)
                        try:
                            await bot.send_message(u["referrer_id"], f"💸 +{ref_bonus:.2f} монет с реферала")
                        except Exception:
                            pass
                    try:
                        await bot.send_message(uid, f"✅ Холд разблокирован: +{h['amount']:.2f} монет")
                    except Exception:
                        pass
                else:
                    try:
                        await bot.send_message(uid, f"❌ Холд отменён (отписка): {h['amount']:.2f} монет сгорели")
                    except Exception:
                        pass
        except Exception as e:
            logging.error(f"hold_worker: {e}")


# ========== КЛАВИАТУРЫ ==========
def user_menu():
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🎯 Заработать"), KeyboardButton(text="💰 Баланс")],
            [KeyboardButton(text="💸 Вывести"), KeyboardButton(text="👥 Рефералы")],
            [KeyboardButton(text="🎁 Промокод")],
        ],
        resize_keyboard=True, is_persistent=True
    )
    return kb


def admin_reply_menu():
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🎯 Заработать"), KeyboardButton(text="💰 Баланс")],
            [KeyboardButton(text="💸 Вывести"), KeyboardButton(text="👥 Рефералы")],
            [KeyboardButton(text="🛠 Админка")],
        ],
        resize_keyboard=True, is_persistent=True
    )
    return kb


def get_menu(user_id):
    return admin_reply_menu() if user_id == ADMIN_ID else user_menu()


# Эмодзи-циферки для кнопок спонсоров
DIGIT_EMOJI = ["1️⃣","2️⃣","3️⃣","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]

def number_label(i):
    if 1 <= i <= 10:
        return DIGIT_EMOJI[i - 1]
    s = str(i)
    out = []
    for d in s:
        if d == "0":
            out.append("0️⃣")
        else:
            out.append(DIGIT_EMOJI[int(d) - 1])
    return "".join(out)


def sponsors_kb(links):
    kb = InlineKeyboardBuilder()
    for i, link in enumerate(links, 1):
        kb.button(text=number_label(i), url=link)
    kb.button(text="✅ Я подписался — проверить", callback_data="check_subs")
    kb.button(text="🔄 Обновить список", callback_data="rebuild_subs")

    rows = [2] * (len(links) // 2)
    if len(links) % 2:
        rows.append(1)
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
    kb.button(text="🔎 Найти юзера", callback_data="adm_find")
    kb.button(text="💎 Начислить", callback_data="adm_give")
    kb.button(text="🎁 Создать промокод", callback_data="adm_promo_create")
    kb.button(text="📢 Рассылка", callback_data="adm_broadcast")
    kb.button(text="⚠️ Ошибки API", callback_data="adm_errors")
    kb.adjust(3, 3, 3)
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
        return False, (
            "😕 <b>Заданий пока нет, но ты молодец!</b>\n\n"
            "Ты уже забрал всё, что было. Загляни через 15-20 минут — "
            "мы постоянно добавляем новых спонсоров 💎"
        )

    reward = await get_reward()
    total_reward = reward * len(all_links)

    text = (
        f"🎯 <b>Тебе доступно {len(all_links)} заданий!</b>\n\n"
        f"💎 Это твой шанс заработать <b>{total_reward:.2f} монет</b> за 1 минуту!\n\n"
        f"━━━━━━━━━━━━━━━\n"
        f"💵 За каждого спонсора: <b>{reward:.2f} монет</b>\n"
        f"⏳ Разблокировка: через <b>{HOLD_HOURS}ч</b>\n"
        f"━━━━━━━━━━━━━━━\n\n"
        f"🚀 <b>Как забрать деньги:</b>\n"
        f"1️⃣ Жми на <b>каждую кнопку</b> ниже и подписывайся\n"
        f"2️⃣ Вернись и нажми <b>«✅ Я подписался — проверить»</b>\n"
        f"3️⃣ Получи монеты в холд — и они твои через {HOLD_HOURS}ч 🎉\n\n"
        f"💡 <i>Чем больше спонсоров — тем больше монет! "
        f"Подпишись на ВСЕХ, не пропускай ни одного 👇</i>"
    )

    if delete_msg_id:
        try:
            await bot.delete_message(chat_id, delete_msg_id)
        except Exception:
            pass

    try:
        await bot.send_message(
            chat_id, text,
            reply_markup=sponsors_kb(all_links),
            disable_web_page_preview=True
        )
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
        try:
            ref_id = int(args[1].replace("ref", ""))
        except Exception:
            pass

    await register_user(msg.from_user.id, ref_id)

    u_check = await get_user(msg.from_user.id)
    if u_check and u_check["blocked"]:
        await msg.answer("🚫 <b>Ты заблокирован.</b>\nОбратись к админу.")
        return

    ok, err = await issue_sponsors_message(msg.from_user, msg.chat.id)
    if not ok:
        u = await get_user(msg.from_user.id)
        bal = u["balance"] if u else 0
        held = u["held"] if u else 0
        await msg.answer(
            f"{err}\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"💰 Доступно: <b>{bal:.2f} монет</b>\n"
            f"⏳ В холде: <b>{held:.2f} монет</b>",
            reply_markup=get_menu(msg.from_user.id)
        )


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
    ok, err = await issue_sponsors_message(msg.from_user, msg.chat.id)
    if not ok:
        await msg.answer(err, reply_markup=get_menu(msg.from_user.id))


@dp.message(F.text == "💰 Баланс")
async def msg_balance(msg: types.Message):
    u = await get_user(msg.from_user.id)
    bal = u["balance"] if u else 0
    held = u["held"] if u else 0
    await msg.answer(
        f"💰 <b>Твой баланс</b>\n\n"
        f"━━━━━━━━━━━━━━━\n"
        f"💎 Доступно: <b>{bal:.2f} монет</b>\n"
        f"⏳ В холде: <b>{held:.2f} монет</b>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"📤 Минимум вывода: <b>{MIN_WITHDRAW:.0f} монет</b>",
        reply_markup=get_menu(msg.from_user.id))


@dp.message(F.text == "💸 Вывести")
async def msg_withdraw(msg: types.Message):
    u = await get_user(msg.from_user.id)
    bal = u["balance"] if u else 0
    if bal < MIN_WITHDRAW:
        await msg.answer(
            f"❌ <b>Недостаточно монет</b>\n\n"
            f"📤 Минимум: <b>{MIN_WITHDRAW:.0f}</b>\n"
            f"💰 У тебя: <b>{bal:.2f}</b>",
            reply_markup=get_menu(msg.from_user.id))
        return
    _states[msg.from_user.id] = "await_withdraw"
    await msg.answer(
        f"💸 <b>Вывод в Bytecoin</b>\n\n"
        f"💰 Доступно: <b>{bal:.2f} монет</b>\n\n"
        f"Отправь сумму одним сообщением:\n"
        f"Например: <code>1500</code>",
        reply_markup=get_menu(msg.from_user.id))


@dp.message(F.text == "👥 Рефералы")
async def msg_refs(msg: types.Message):
    me = await bot.get_me()
    ref_link = f"https://t.me/{me.username}?start=ref{msg.from_user.id}"

    pool = await get_pool()
    async with pool.acquire() as db:
        refs_count = (await db.fetchrow(
            "SELECT COUNT(*) as c FROM users WHERE referrer_id=$1",
            msg.from_user.id
        ))["c"]

        earned = (await db.fetchrow("""
            SELECT COALESCE(SUM(amount), 0) AS s FROM reward_holds
            WHERE user_id IN (SELECT user_id FROM users WHERE referrer_id=$1)
            AND status='released'
        """, msg.from_user.id))["s"]

    text = (
        f"👥 <b>Реферальная система</b>\n\n"
        f"━━━━━━━━━━━━━━━\n"
        f"💸 Бонус: <b>{REF_PERCENT}%</b> с холда рефералов\n"
        f"━━━━━━━━━━━━━━━\n\n"
        f"🔗 Твоя ссылка:\n"
        f"<code>{ref_link}</code>\n\n"
        f"👤 Приглашено: <b>{refs_count}</b>\n"
        f"💵 Заработано: <b>{float(earned) * REF_PERCENT / 100:.2f} монет</b>"
    )
    await msg.answer(text, reply_markup=get_menu(msg.from_user.id))


@dp.message(F.text == "🎁 Промокод")
async def msg_promo(msg: types.Message):
    if msg.from_user.id == ADMIN_ID: return
    _states[msg.from_user.id] = "await_promo"
    await msg.answer(
        "🎁 <b>Активация промокода</b>\n\n"
        "Отправь код одним сообщением:",
        reply_markup=promo_kb()
    )


@dp.callback_query(F.data == "promo_cancel")
async def cb_promo_cancel(cq: types.CallbackQuery):
    _states.pop(cq.from_user.id, None)
    u = await get_user(cq.from_user.id)
    bal = u["balance"] if u else 0
    held = u["held"] if u else 0
    await cq.answer("Отменено")
    try:
        await cq.message.edit_text("Отменено.")
    except Exception:
        pass
    await cq.message.answer(
        f"💰 Доступно: <b>{bal:.2f} монет</b>\n⏳ В холде: <b>{held:.2f} монет</b>",
        reply_markup=get_menu(cq.from_user.id))


# ========== ПРОВЕРКА ПОДПИСОК ==========
@dp.callback_query(F.data == "check_subs")
async def cb_check(cq: types.CallbackQuery):
    uid = cq.from_user.id
    if time.time() - _last.get(uid, 0) < CHECK_COOLDOWN:
        await cq.answer("⏱ Подожди пару секунд")
        return
    _last[uid] = time.time()
    await cq.answer("Проверяю…")

    cnt, sm = await check_all(uid)
    left = await pending_count(uid)

    pool = await get_pool()
    async with pool.acquire() as db:
        total_done = (await db.fetchrow("SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status='subscribed'", uid))["c"]
        total_all = (await db.fetchrow("SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1", uid))["c"]

    u = await get_user(uid)
    bal = u["balance"] if u else 0
    held = u["held"] if u else 0
    reward = await get_reward()

    if left == 0 and total_all > 0:
        text = (
            f"🎉 <b>Ты просто космос!</b>\n\n"
            f"Все <b>{total_done}</b> заданий выполнены — ты легенда 💎\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"💰 Доступно: <b>{bal:.2f} монет</b>\n"
            f"⏳ В холде: <b>{held:.2f} монет</b>\n"
            f"━━━━━━━━━━━━━━━\n\n"
            f"<i>Награды придут через {HOLD_HOURS}ч, если не отпишешься. "
            f"Заглядывай позже — мы добавляем новых спонсоров! 🚀</i>"
        )
        try:
            await cq.message.edit_text(text)
        except Exception:
            await cq.message.answer(text)
        await cq.message.answer("Меню 👇", reply_markup=get_menu(uid))
        return

    if cnt > 0:
        text = (
            f"✅ <b>Красавчик! Засчитано: {cnt}</b>\n\n"
            f"💵 В холд: <b>+{sm:.2f} монет</b>\n"
            f"⏳ Через {HOLD_HOURS}ч → на баланс\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"📊 Прогресс: <b>{total_done} / {total_all}</b>\n"
            f"━━━━━━━━━━━━━━━\n\n"
            f"⚠️ Осталось: <b>{left}</b>\n"
            f"💵 За каждого ещё: <b>{reward:.2f} монет</b>\n\n"
            f"🔥 <i>Не останавливайся! Ты почти у цели — "
            f"добери ещё {left} и получишь всё.</i>"
        )
    else:
        text = (
            f"❌ <b>Новых подписок не найдено</b>\n\n"
            f"📊 Прогресс: <b>{total_done} / {total_all}</b>\n\n"
            f"⚠️ Осталось: <b>{left}</b> спонсоров\n\n"
            f"<i>💡 Подпишись на ВСЕ кнопки, что были выше, потом жми проверку. "
            f"Если подписался — подожди 5-10 минут.</i>"
        )

    kb = InlineKeyboardBuilder()
    kb.button(text="🔄 Обновить список", callback_data="rebuild_subs")
    kb.button(text="💰 Баланс", callback_data="balance_inline")
    kb.adjust(1)

    try:
        await cq.message.edit_text(text, reply_markup=kb.as_markup())
    except Exception:
        await cq.message.answer(text, reply_markup=kb.as_markup())


@dp.callback_query(F.data == "rebuild_subs")
async def cb_rebuild(cq: types.CallbackQuery):
    uid = cq.from_user.id
    await cq.answer("Проверяю и пересобираю…")

    await check_all(uid)

    try:
        await cq.message.delete()
    except Exception:
        pass

    ok, err = await issue_sponsors_message(
        cq.from_user, cq.message.chat.id, clean=True
    )
    if not ok:
        await cq.message.answer(err, reply_markup=get_menu(uid))


@dp.callback_query(F.data == "balance_inline")
async def cb_balance_inline(cq: types.CallbackQuery):
    u = await get_user(cq.from_user.id)
    bal = u["balance"] if u else 0
    held = u["held"] if u else 0
    await cq.answer()
    await cq.message.answer(
        f"💰 Доступно: <b>{bal:.2f} монет</b>\n"
        f"⏳ В холде: <b>{held:.2f} монет</b>\n"
        f"📤 Минимум: <b>{MIN_WITHDRAW:.0f} монет</b>",
        reply_markup=get_menu(cq.from_user.id)
    )


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
        await msg.answer(f"✅ Промокод: <b>+{promo['amount']:.2f} монет</b>", reply_markup=get_menu(uid))
        return

    if state == "await_withdraw":
        try:
            amount = float(msg.text.replace(",", "."))
        except Exception:
            await msg.answer("❌ Не понял сумму", reply_markup=get_menu(uid)); return
        if amount < MIN_WITHDRAW:
            await msg.answer(f"❌ Минимум {MIN_WITHDRAW:.0f} монет", reply_markup=get_menu(uid)); return
        u = await get_user(uid)
        bal = u["balance"] if u else 0
        if amount > bal:
            await msg.answer(f"❌ У тебя только {bal:.2f} монет", reply_markup=get_menu(uid)); return
        _states.pop(uid, None)
        pool = await get_pool()
        async with pool.acquire() as db:
            res = await db.execute("UPDATE users SET balance = balance - $1 WHERE user_id=$2 AND balance >= $1", amount, uid)
            if res == "UPDATE 0":
                await msg.answer("❌ Недостаточно", reply_markup=get_menu(uid)); return
            row = await db.fetchrow("INSERT INTO withdrawals (user_id, amount, status, created_at) VALUES ($1,$2,'pending',$3) RETURNING id", uid, amount, time.time())
            wid = row["id"]
        await msg.answer(f"⏳ Вывод {amount:.2f} монет обрабатывается…", reply_markup=get_menu(uid))
        resp = await bc_transfer(uid, amount)
        if resp.get("status") == "ok":
            pool = await get_pool()
            async with pool.acquire() as db:
                await db.execute("UPDATE withdrawals SET status='done', tx_id=$1 WHERE id=$2", resp.get("transaction_id"), wid)
            await msg.answer(f"✅ Выведено: <b>{amount:.2f} монет</b>\nTX: <code>{str(resp.get('transaction_id',''))[:16]}…</code>", reply_markup=get_menu(uid))
            await notify_admin(f"💸 Выплата\n👤 <code>{uid}</code>\n💰 {amount:.2f} монет\nTX: <code>{resp.get('transaction_id','')}</code>")
        else:
            pool = await get_pool()
            async with pool.acquire() as db:
                await db.execute("UPDATE users SET balance = balance + $1 WHERE user_id=$2", amount, uid)
                await db.execute("UPDATE withdrawals SET status='failed', error=$1 WHERE id=$2", resp.get("error", ""), wid)
            await msg.answer(f"❌ Ошибка: {resp.get('error','unknown')}\nСредства возвращены.", reply_markup=get_menu(uid))
        return

    if state == "adm_reward":
        try:
            val = float(msg.text.replace(",", "."))
        except Exception:
            await msg.answer("❌ Число"); return
        await set_setting("reward", val)
        _states.pop(uid, None)
        await msg.answer(f"✅ Награда: <b>{val:.2f} монет</b>", reply_markup=admin_menu())
        return

    if state == "adm_max":
        try:
            val = int(msg.text)
        except Exception:
            await msg.answer("❌ Целое"); return
        if val > MAX_MAX_SPONSORS or val < 1:
            await msg.answer(f"❌ От 1 до {MAX_MAX_SPONSORS}"); return
        await set_setting("max_sponsors", val)
        _states.pop(uid, None)
        await msg.answer(f"✅ Макс: <b>{val}</b>", reply_markup=admin_menu())
        return

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
        await msg.answer(f"✅ Промокод <code>{code}</code>: {amount:.2f} × {uses}", reply_markup=admin_menu())
        return

    if state == "adm_find":
        try:
            target = int(msg.text.strip())
        except Exception:
            await msg.answer("❌ Введи ID (число)"); return
        _states.pop(uid, None)
        u = await get_user(target)
        if not u:
            await msg.answer(f"❌ Юзер <code>{target}</code> не найден", reply_markup=admin_menu()); return
        pool = await get_pool()
        async with pool.acquire() as db:
            holds = (await db.fetchrow("SELECT COUNT(*) as c, COALESCE(SUM(amount),0) as s FROM reward_holds WHERE user_id=$1 AND status='holding'", target))
            refs = (await db.fetchrow("SELECT COUNT(*) as c FROM users WHERE referrer_id=$1", target))["c"]
            withdrawn = (await db.fetchrow("SELECT COALESCE(SUM(amount),0) as s FROM withdrawals WHERE user_id=$1 AND status='done'", target))["s"]

        text = (
            f"🔎 <b>Юзер</b> <code>{target}</code>\n\n"
            f"💰 Доступно: <b>{u['balance']:.2f}</b>\n"
            f"⏳ В холде: <b>{u['held']:.2f}</b>\n"
            f"🔒 Активных холдов: <b>{holds['c']}</b> ({holds['s']:.2f})\n"
            f"👥 Рефералов: <b>{refs}</b>\n"
            f"💸 Выведено всего: <b>{withdrawn:.2f}</b>\n"
            f"🚫 Бан: {'Да' if u['blocked'] else 'Нет'}\n\n"
            f"Команды:\n"
            f"<code>/ban {target}</code>\n"
            f"<code>/unban {target}</code>\n"
            f"<code>/give {target} 500</code>"
        )
        await msg.answer(text, reply_markup=admin_menu())
        return

    if state == "adm_give":
        parts = msg.text.strip().split()
        if len(parts) != 2:
            await msg.answer("❌ Формат: <code>ID СУММА</code>"); return
        try:
            target = int(parts[0]); amount = float(parts[1])
        except Exception:
            await msg.answer("❌ Числа неверные"); return
        u = await get_user(target)
        if not u:
            await msg.answer("❌ Юзер не найден"); return
        await add_balance(target, amount)
        _states.pop(uid, None)
        await msg.answer(f"✅ Начислено <b>{amount:.2f}</b> → <code>{target}</code>", reply_markup=admin_menu())
        try:
            await bot.send_message(target, f"🎁 Тебе начислено: <b>+{amount:.2f} монет</b>")
        except Exception:
            pass
        return


# ========== КОМАНДЫ АДМИНА ==========
@dp.message(Command("ban"))
async def cmd_ban(msg: types.Message):
    if msg.from_user.id != ADMIN_ID: return
    try:
        target = int(msg.text.split()[1])
    except Exception:
        await msg.answer("Формат: <code>/ban ID</code>"); return
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE users SET blocked=TRUE WHERE user_id=$1", target)
    await msg.answer(f"🚫 Забанен: <code>{target}</code>", reply_markup=admin_menu())


@dp.message(Command("unban"))
async def cmd_unban(msg: types.Message):
    if msg.from_user.id != ADMIN_ID: return
    try:
        target = int(msg.text.split()[1])
    except Exception:
        await msg.answer("Формат: <code>/unban ID</code>"); return
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE users SET blocked=FALSE WHERE user_id=$1", target)
    await msg.answer(f"✅ Разбанен: <code>{target}</code>", reply_markup=admin_menu())


@dp.message(Command("give"))
async def cmd_give(msg: types.Message):
    if msg.from_user.id != ADMIN_ID: return
    try:
        parts = msg.text.split()
        target = int(parts[1]); amount = float(parts[2])
    except Exception:
        await msg.answer("Формат: <code>/give ID СУММА</code>"); return
    u = await get_user(target)
    if not u:
        await msg.answer("❌ Юзер не найден"); return
    await add_balance(target, amount)
    await msg.answer(f"✅ Начислено <b>{amount:.2f}</b> → <code>{target}</code>", reply_markup=admin_menu())
    try:
        await bot.send_message(target, f"🎁 Тебе начислено: <b>+{amount:.2f} монет</b>")
    except Exception:
        pass


# ========== АДМИН CALLBACKS ==========
@dp.callback_query(F.data == "adm_reward")
async def adm_reward(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_reward"
    await cq.answer()
    try:
        await cq.message.edit_text(f"💰 Текущая: <b>{await get_reward():.2f} монет</b>\n\nОтправь новое значение:")
    except Exception:
        pass


@dp.callback_query(F.data == "adm_max")
async def adm_max(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_max"
    await cq.answer()
    try:
        await cq.message.edit_text(f"👥 Текущий: <b>{await get_max_sponsors()}</b>\n\nОтправь число 1-{MAX_MAX_SPONSORS}:")
    except Exception:
        pass


@dp.callback_query(F.data == "adm_stats")
async def adm_stats(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    pool = await get_pool()
    async with pool.acquire() as db:
        total = (await db.fetchrow("SELECT COUNT(*) as c FROM users"))["c"]
        blocked = (await db.fetchrow("SELECT COUNT(*) as c FROM users WHERE blocked=TRUE"))["c"]
        balances = (await db.fetchrow("SELECT COALESCE(SUM(balance),0) as s FROM users"))["s"]
        held = (await db.fetchrow("SELECT COALESCE(SUM(held),0) as s FROM users"))["s"]
        subs = (await db.fetchrow("SELECT COUNT(*) as c FROM reward_holds WHERE status='released'"))["c"]
        holding = (await db.fetchrow("SELECT COUNT(*) as c FROM reward_holds WHERE status='holding'"))["c"]
        paid = (await db.fetchrow("SELECT COALESCE(SUM(amount),0) as s FROM withdrawals WHERE status='done'"))["s"]
        pend = (await db.fetchrow("SELECT COUNT(*) as c FROM withdrawals WHERE status='pending'"))["c"]
        errs = (await db.fetchrow("SELECT COUNT(*) as c FROM error_log WHERE created_at > $1", time.time() - 86400))["c"]

    await cq.answer()
    try:
        await cq.message.edit_text(
            f"📊 <b>Статистика</b>\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"👥 Юзеров: <b>{total}</b> (🚫 {blocked})\n"
            f"💰 Балансы: <b>{balances:.2f}</b>\n"
            f"⏳ В холде: <b>{held:.2f}</b>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"✅ Подписок: <b>{subs}</b>\n"
            f"⏳ Холдов: <b>{holding}</b>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"💸 Выплачено: <b>{paid:.2f}</b>\n"
            f"📤 Pending: <b>{pend}</b>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"⚠️ Ошибок за 24ч: <b>{errs}</b>",
            reply_markup=admin_menu())
    except Exception:
        pass


@dp.callback_query(F.data == "adm_find")
async def adm_find(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_find"
    await cq.answer()
    try:
        await cq.message.edit_text("🔎 Отправь Telegram ID юзера:")
    except Exception:
        pass


@dp.callback_query(F.data == "adm_give")
async def adm_give(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_give"
    await cq.answer()
    try:
        await cq.message.edit_text("💎 Формат: <code>ID СУММА</code>\nПример: <code>123456789 500</code>")
    except Exception:
        pass


@dp.callback_query(F.data == "adm_errors")
async def adm_errors(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch("""
            SELECT service, error_type, message, created_at
            FROM error_log ORDER BY created_at DESC LIMIT 20
        """)
    if not rows:
        await cq.answer("Ошибок нет 🎉", show_alert=True); return

    text = "⚠️ <b>Последние ошибки:</b>\n\n"
    for r in rows:
        ts = time.strftime("%d.%m %H:%M", time.localtime(r["created_at"]))
        text += f"<b>{r['service']}</b> · {r['error_type']}\n"
        text += f"<code>{r['message'][:120]}</code>\n"
        text += f"<i>{ts}</i>\n\n"

    await cq.answer()
    try:
        await cq.message.edit_text(text, reply_markup=admin_menu())
    except Exception:
        pass


@dp.callback_query(F.data == "adm_broadcast")
async def adm_broadcast(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_broadcast"
    await cq.answer()
    try:
        await cq.message.edit_text("📢 Отправь сообщение для рассылки (всем активным):")
    except Exception:
        pass


@dp.callback_query(F.data == "adm_promo_create")
async def adm_promo_create(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_promo_create"
    await cq.answer()
    try:
        await cq.message.edit_text("Формат: <code>КОД | СУММА | АКТИВАЦИЙ</code>\nПример: <code>SALE | 500 | 100</code>")
    except Exception:
        pass


@dp.callback_query(F.data == "adm_withdraws")
async def adm_withdraws(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch("SELECT id, user_id, amount, status, tx_id FROM withdrawals ORDER BY id DESC LIMIT 20")
    if not rows:
        await cq.answer("Нет заявок", show_alert=True); return
    text = "💸 <b>Последние выводы:</b>\n\n"
    for r in rows:
        emoji = {"done": "✅", "pending": "⏳", "failed": "❌"}.get(r["status"], "•")
        text += f"{emoji} #{r['id']} · <code>{r['user_id']}</code> · {r['amount']:.2f}\n"
    await cq.answer()
    try:
        await cq.message.edit_text(text, reply_markup=admin_menu())
    except Exception:
        pass


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
    await init_db()
    await start_web()
    asyncio.create_task(hold_worker())
    asyncio.create_task(autocheck_worker())
    dp.shutdown.register(on_shutdown)
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
