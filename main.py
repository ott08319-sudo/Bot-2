import os
import time
import uuid
import asyncio
import logging
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

logging.basicConfig(level=logging.INFO)

BOT_TOKEN       = os.getenv("BOT_TOKEN")
ADMIN_ID        = int(os.getenv("ADMIN_ID", "0") or "0")
PORT            = int(os.getenv("PORT", "10000"))
DATABASE_URL    = os.getenv("DATABASE_URL", "")
SELF_URL        = os.getenv("SELF_URL", "")

BC_API_KEY      = os.getenv("BYTECOIN_API_KEY", "")
BC_ENABLED      = os.getenv("BYTECOIN_ENABLED", "true").lower() == "true"
BC_BASE         = "https://bytecoin.space/api/public/v1"
BC_MIN_TRANSFER = Decimal("0.0000001")

PIARFLOW_API_KEY = os.getenv("PIARFLOW_API_KEY", "")
TGRASS_API_KEY   = os.getenv("TGRASS_API_KEY", "")
TRAFSLY_API_KEY  = os.getenv("TRAFSLY_API_KEY", "")
BOTOHUB_API_KEY  = os.getenv("BOTOHUB_API_KEY", "")

CHECK_COOLDOWN       = 5
DEFAULT_REWARD       = 200.0
DEFAULT_MAX_SPONSORS = 15
MIN_WITHDRAW         = 1000.0
REF_PERCENT          = 50
HOLD_HOURS           = 48
HOLD_SECONDS         = HOLD_HOURS * 3600

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


def clean_db_url(url: str) -> str:
    if not url:
        return url
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
            user_id BIGINT,
            service TEXT,
            assignment_id TEXT,
            link TEXT,
            reward DOUBLE PRECISION DEFAULT 0,
            status TEXT DEFAULT 'unsubscribed',
            signature TEXT,
            created_at DOUBLE PRECISION,
            UNIQUE(user_id, service, assignment_id))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS reward_holds (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            service TEXT,
            assignment_id TEXT,
            amount DOUBLE PRECISION,
            unlock_at DOUBLE PRECISION,
            status TEXT DEFAULT 'holding',
            created_at DOUBLE PRECISION,
            UNIQUE(user_id, service, assignment_id))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS withdrawals (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            amount DOUBLE PRECISION,
            status TEXT DEFAULT 'pending',
            tx_id TEXT,
            error TEXT,
            created_at DOUBLE PRECISION)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS promo_codes (
            code TEXT PRIMARY KEY,
            amount DOUBLE PRECISION,
            max_uses INT DEFAULT 1,
            used_count INT DEFAULT 0,
            active BOOLEAN DEFAULT TRUE,
            created_at DOUBLE PRECISION)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS promo_uses (
            user_id BIGINT,
            code TEXT,
            used_at DOUBLE PRECISION,
            PRIMARY KEY (user_id, code))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS required_channels (
            id SERIAL PRIMARY KEY,
            chat_id TEXT UNIQUE,
            title TEXT,
            url TEXT,
            type TEXT,
            active BOOLEAN DEFAULT TRUE,
            created_at DOUBLE PRECISION)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT)""")
        await db.execute("INSERT INTO settings (key,value) VALUES ('reward',$1) ON CONFLICT (key) DO NOTHING", str(DEFAULT_REWARD))
        await db.execute("INSERT INTO settings (key,value) VALUES ('max_sponsors',$1) ON CONFLICT (key) DO NOTHING", str(DEFAULT_MAX_SPONSORS))

        await db.execute("""INSERT INTO required_channels (chat_id, title, url, type, active, created_at)
            VALUES ('bytecoin_app', 'Bytecoin', 'https://t.me/byteappbot?start=r-FdLJFAyOe73', 'app', TRUE, $1)
            ON CONFLICT (chat_id) DO NOTHING""", time.time())


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
        if exists:
            return
        if referrer_id == user_id:
            referrer_id = None
        if referrer_id:
            ref_exists = await db.fetchrow("SELECT user_id FROM users WHERE user_id=$1", referrer_id)
            if not ref_exists:
                referrer_id = None
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


async def save_sponsor(user_id, service, aid, link, reward, signature=None):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("""INSERT INTO sponsor_tasks (user_id, service, assignment_id, link, reward, signature, status, created_at)
            VALUES ($1,$2,$3,$4,$5,$6,'unsubscribed',$7) ON CONFLICT (user_id, service, assignment_id) DO NOTHING""",
            user_id, service, aid, link, reward, signature, time.time())


async def mark_subscribed(user_id, service, aid):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE sponsor_tasks SET status='subscribed' WHERE user_id=$1 AND service=$2 AND assignment_id=$3", user_id, service, aid)


async def pending_tasks(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch("SELECT service, assignment_id, link, reward, signature FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed'", user_id)
        return [(r["service"], r["assignment_id"], r["link"], r["reward"], r["signature"]) for r in rows]


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


# ========== SPONSOR APIs ==========
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
        logging.error(f"Piarflow: {e}")
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
        logging.error(f"Piarflow check: {e}")
    return []


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
        logging.error(f"TGrass: {e}")
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
        logging.error(f"TGrass check: {e}")
    return False


async def get_botohub(user_id):
    if not BOTOHUB_API_KEY: return []
    s = await http()
    try:
        async with s.post("https://botohub.me/get-tasks",
            json={"chat_id": user_id}, headers={"Auth": BOTOHUB_API_KEY}) as r:
            d = await r.json()
            if not d.get("skip") and not d.get("completed"):
                return d.get("tasks", [])
    except Exception as e:
        logging.error(f"Botohub: {e}")
    return []


async def check_botohub(user_id):
    if not BOTOHUB_API_KEY: return False
    s = await http()
    try:
        async with s.post("https://botohub.me/get-tasks",
            json={"chat_id": user_id}, headers={"Auth": BOTOHUB_API_KEY}) as r:
            d = await r.json()
            return bool(d.get("completed"))
    except Exception as e:
        logging.error(f"Botohub check: {e}")
    return False


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
        logging.error(f"Trafsly: {e}")
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
            logging.error(f"Trafsly {aid}: {e}")
    return res


async def collect_sponsors(user):
    uid = user.id
    un = user.username or ""
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

    for link in await get_botohub(uid):
        if link:
            await save_sponsor(uid, "botohub", link, link, reward)
            links.append(link)

    for x in await get_trafsly(uid, un):
        link = x.get("link")
        aid = x.get("ads_id")
        if link:
            await save_sponsor(uid, "trafsly", str(aid) if aid else link, link, reward)
            links.append(link)

    return links


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

    pf, ts = [], []
    for service, aid, link, reward, sig in tasks:
        if service == "piarflow" and link: pf.append(link)
        elif service == "trafsly" and aid: ts.append(aid)

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

    if await check_botohub(user_id):
        pool = await get_pool()
        async with pool.acquire() as db:
            rows = await db.fetch("SELECT assignment_id, reward FROM sponsor_tasks WHERE user_id=$1 AND service='botohub' AND status!='subscribed'", user_id)
        for r in rows:
            await mark_subscribed(user_id, "botohub", r["assignment_id"])
            await _create_hold(user_id, "botohub", r["assignment_id"], r["reward"])
            done_count += 1; done_sum += r["reward"]

    return done_count, done_sum


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
                    elif service == "botohub":
                        still = await check_botohub(uid)
                    elif service == "trafsly":
                        r = await check_trafsly(uid, [aid])
                        still = any(x.get("status") == "subscribed" for x in r)
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
                        pool = await get_pool()
                        async with pool.acquire() as db:
                            await db.execute("UPDATE users SET referred_earned = referred_earned + $1 WHERE user_id=$2", ref_bonus, u["referrer_id"])
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


async def check_required(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch("SELECT id, chat_id, title, url, type FROM required_channels WHERE active=TRUE")
    not_done = []
    for r in rows:
        if r["type"] == "app":
            continue
        try:
            member = await bot.get_chat_member(r["chat_id"], user_id)
            if member.status not in ("member", "administrator", "creator"):
                not_done.append(r)
        except Exception as e:
            logging.error(f"req {r['chat_id']}: {e}")
            not_done.append(r)
    return not_done


def required_kb(missing):
    kb = InlineKeyboardBuilder()
    for r in missing:
        emoji = "🚀" if r["type"] == "app" else "📢"
        kb.button(text=f"{emoji} {r['title']}", url=r["url"])
    kb.button(text="✅ Я подписался — проверить", callback_data="check_required")
    kb.adjust(1)
    return kb.as_markup()


# ========== КЛАВИАТУРЫ ==========
def user_menu():
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🎯 Заработать"), KeyboardButton(text="💰 Баланс")],
            [KeyboardButton(text="💸 Вывести"), KeyboardButton(text="👥 Рефералы")],
            [KeyboardButton(text="🎁 Промокод")],
        ],
        resize_keyboard=True,
        is_persistent=True
    )
    return kb


def admin_reply_menu():
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🎯 Заработать"), KeyboardButton(text="💰 Баланс")],
            [KeyboardButton(text="💸 Вывести"), KeyboardButton(text="👥 Рефералы")],
            [KeyboardButton(text="🛠 Админка")],
        ],
        resize_keyboard=True,
        is_persistent=True
    )
    return kb


def get_menu(user_id):
    return admin_reply_menu() if user_id == ADMIN_ID else user_menu()


def sponsors_kb(links):
    kb = InlineKeyboardBuilder()
    for i, link in enumerate(links, 1):
        kb.button(text=f"🔗 {i}", url=link)
    kb.button(text="✅ Проверить подписки", callback_data="check_subs")
    rows = [2] * (len(links) // 2)
    if len(links) % 2:
        rows.append(1)
    rows.append(1)
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
    kb.button(text="📊 Статистика", callback_data="adm_stats")
    kb.button(text="📡 Проверка API", callback_data="adm_check_api")
    kb.button(text="📢 Рассылка", callback_data="adm_broadcast")
    kb.button(text="➕ Добавить ОП", callback_data="adm_add_req")
    kb.button(text="📋 Список ОП", callback_data="adm_list_req")
    kb.button(text="🎁 Создать промокод", callback_data="adm_promo_create")
    kb.button(text="📋 Промокоды", callback_data="adm_promos")
    kb.button(text="💸 Заявки", callback_data="adm_withdraws")
    kb.adjust(1)
    return kb.as_markup()


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

    missing = await check_required(msg.from_user.id)
    if missing:
        await msg.answer("🔒 <b>Для доступа подпишись:</b>", reply_markup=required_kb(missing))
        return

    u = await get_user(msg.from_user.id)
    bal = u["balance"] if u else 0
    held = u["held"] if u else 0
    reward = await get_reward()

    text = (
        f"🎉 <b>Sub3Byte — зарабатывай монетки!</b>\n\n"
        f"💰 Доступно: <b>{bal:.2f} монет</b>\n"
        f"⏳ В холде: <b>{held:.2f} монет</b>\n\n"
        f"🎯 За спонсора: <b>{reward:.2f} монет</b>\n"
        f"👥 С реферала: <b>{REF_PERCENT}%</b>\n"
        f"💸 Вывод от: <b>{MIN_WITHDRAW:.0f} монет</b>\n"
        f"⏰ Холд: <b>{HOLD_HOURS}ч</b>\n\n"
        f"Выбирай действие внизу 👇"
    )
    await msg.answer(text, reply_markup=get_menu(msg.from_user.id))


@dp.callback_query(F.data == "check_required")
async def cb_check_required(cq: types.CallbackQuery):
    missing = await check_required(cq.from_user.id)
    if missing:
        await cq.answer("❌ Ещё не всё", show_alert=True)
        try:
            await cq.message.edit_text("🔒 <b>Не все подписки:</b>", reply_markup=required_kb(missing))
        except Exception:
            pass
        return

    await cq.answer("✅ Готово!")
    try:
        await cq.message.delete()
    except Exception:
        pass

    u = await get_user(cq.from_user.id)
    bal = u["balance"] if u else 0
    held = u["held"] if u else 0
    reward = await get_reward()

    text = (
        f"🎉 <b>Добро пожаловать в Sub3Byte!</b>\n\n"
        f"Теперь ты можешь зарабатывать <b>монетки</b> — "
        f"выполняй простые задания и получай награду.\n\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"🎯 <b>Заработать</b> — подписывайся на каналы и получай "
        f"<b>{reward:.2f} монет</b> за каждого. Награда приходит через {HOLD_HOURS}ч.\n\n"
        f"👥 <b>Рефералы</b> — приглашай друзей и получай "
        f"<b>{REF_PERCENT}%</b> с их заработка пожизненно.\n\n"
        f"💸 <b>Вывод</b> — минимум <b>{MIN_WITHDRAW:.0f} монет</b> "
        f"автоматически на твой баланс.\n\n"
        f"🎁 <b>Промокоды</b> — следи за новостями, "
        f"иногда раздаём бонусы.\n\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"💰 Твой баланс: <b>{bal:.2f} монет</b>\n"
        f"⏳ В холде: <b>{held:.2f} монет</b>\n\n"
        f"<b>Жми «🎯 Заработать» — и вперёд!</b> 🚀"
    )
    await cq.message.answer(text, reply_markup=get_menu(cq.from_user.id))


@dp.message(Command("admin"))
async def cmd_admin(msg: types.Message):
    if msg.from_user.id != ADMIN_ID: return
    await msg.answer("🛠 <b>Админ-панель</b>", reply_markup=admin_menu())


@dp.message(F.text == "🛠 Админка")
async def msg_admin_panel(msg: types.Message):
    if msg.from_user.id != ADMIN_ID:
        return
    await msg.answer("🛠 <b>Админ-панель</b>", reply_markup=admin_menu())


@dp.message(Command("cancel"))
async def cmd_cancel(msg: types.Message):
    if msg.from_user.id != ADMIN_ID:
        return
    _states.pop(msg.from_user.id, None)
    _states.pop(f"{msg.from_user.id}_req", None)
    await msg.answer("❌ Отменено", reply_markup=admin_menu())


# ========== REPLY BUTTONS ==========
@dp.message(F.text == "🎯 Заработать")
async def msg_earn(msg: types.Message):
    missing = await check_required(msg.from_user.id)
    if missing:
        await msg.answer("❌ Сначала подпишись", reply_markup=required_kb(missing))
        return

    uid = msg.from_user.id
    new_links = await collect_sponsors(msg.from_user)
    pending = await pending_links(uid)
    all_links = list(dict.fromkeys(pending + new_links))[:await get_max_sponsors()]

    if not all_links:
        await msg.answer("😕 Заданий нет, зайди позже.", reply_markup=get_menu(uid))
        return

    reward = await get_reward()
    await msg.answer(
        f"🎯 <b>Спонсоров: {len(all_links)}</b>\n\n"
        f"Подпишись на все каналы (кнопки ниже).\n"
        f"💵 За каждого: <b>{reward:.2f} монет</b>\n"
        f"⏳ Награда через <b>{HOLD_HOURS}ч</b>\n\n"
        f"После подписки жми «✅ Проверить подписки».",
        reply_markup=sponsors_kb(all_links),
        disable_web_page_preview=True
    )


@dp.message(F.text == "💰 Баланс")
async def msg_balance(msg: types.Message):
    missing = await check_required(msg.from_user.id)
    if missing:
        await msg.answer("❌ Сначала подпишись", reply_markup=required_kb(missing))
        return
    u = await get_user(msg.from_user.id)
    bal = u["balance"] if u else 0
    held = u["held"] if u else 0
    await msg.answer(
        f"💰 Доступно: <b>{bal:.2f} монет</b>\n"
        f"⏳ В холде: <b>{held:.2f} монет</b>\n"
        f"📤 Минимум: <b>{MIN_WITHDRAW:.0f} монет</b>",
        reply_markup=get_menu(msg.from_user.id))


@dp.message(F.text == "💸 Вывести")
async def msg_withdraw(msg: types.Message):
    missing = await check_required(msg.from_user.id)
    if missing:
        await msg.answer("❌ Сначала подпишись", reply_markup=required_kb(missing))
        return
    u = await get_user(msg.from_user.id)
    bal = u["balance"] if u else 0
    if bal < MIN_WITHDRAW:
        await msg.answer(f"❌ Минимум {MIN_WITHDRAW:.0f} монет. У тебя {bal:.2f}", reply_markup=get_menu(msg.from_user.id))
        return
    _states[msg.from_user.id] = "await_withdraw"
    await msg.answer(
        f"💸 <b>Вывод</b>\n\n💰 Доступно: <b>{bal:.2f} монет</b>\n\n"
        f"Отправь сумму:\n<code>1500</code>",
        reply_markup=get_menu(msg.from_user.id))


@dp.message(F.text == "👥 Рефералы")
async def msg_refs(msg: types.Message):
    missing = await check_required(msg.from_user.id)
    if missing:
        await msg.answer("❌ Сначала подпишись", reply_markup=required_kb(missing))
        return
    me = await bot.get_me()
    ref_link = f"https://t.me/{me.username}?start=ref{msg.from_user.id}"
    u = await get_user(msg.from_user.id)
    earned = u["referred_earned"] if u else 0
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT COUNT(*) as c FROM users WHERE referrer_id=$1", msg.from_user.id)
        refs_count = row["c"]
        top_refs = await db.fetch("SELECT user_id, referred_earned FROM users WHERE referrer_id=$1 ORDER BY referred_earned DESC LIMIT 10", msg.from_user.id)
    text = (f"👥 <b>Рефералка</b>\n\n"
            f"Бонус: <b>{REF_PERCENT}%</b> от холда рефералов.\n\n"
            f"🔗 <code>{ref_link}</code>\n\n"
            f"👤 Приглашено: <b>{refs_count}</b>\n"
            f"💵 Заработано: <b>{earned:.2f} монет</b>")
    if top_refs:
        text += "\n\n🏆 <b>Топ-10:</b>\n"
        for i, r in enumerate(top_refs, 1):
            text += f"{i}. <code>{r['user_id']}</code> — {r['referred_earned']:.2f} монет\n"
    await msg.answer(text, reply_markup=get_menu(msg.from_user.id))


@dp.message(F.text == "🎁 Промокод")
async def msg_promo(msg: types.Message):
    if msg.from_user.id == ADMIN_ID:
        return
    missing = await check_required(msg.from_user.id)
    if missing:
        await msg.answer("❌ Сначала подпишись", reply_markup=required_kb(missing))
        return
    _states[msg.from_user.id] = "await_promo"
    await msg.answer("🎁 Отправь промокод или нажми «Назад»", reply_markup=promo_kb())


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
    missing_req = await check_required(cq.from_user.id)
    if missing_req:
        await cq.answer("❌ Сначала подпишись на ОП", show_alert=True)
        return

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
        total_done = (await db.fetchrow(
            "SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status='subscribed'", uid
        ))["c"]
        total_all = (await db.fetchrow(
            "SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1", uid
        ))["c"]

    u = await get_user(uid)
    bal = u["balance"] if u else 0
    held = u["held"] if u else 0
    reward = await get_reward()

    if left == 0 and total_all > 0:
        text = (
            f"🎉 <b>Красавчик!</b>\n\n"
            f"Ты подписан на <b>все {total_done}</b> каналов.\n\n"
            f"💰 Доступно: <b>{bal:.2f} монет</b>\n"
            f"⏳ В холде: <b>{held:.2f} монет</b>\n\n"
            f"<i>Награды придут через {HOLD_HOURS}ч, если не отпишешься.</i>"
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
            f"💵 Начислено в холд: <b>{sm:.2f} монет</b>\n"
            f"⏳ Разблокировка через {HOLD_HOURS}ч\n\n"
            f"📊 <b>Прогресс:</b> {total_done} / {total_all}\n\n"
            f"⚠️ Осталось подписаться: <b>{left}</b>\n"
            f"💵 За каждого дадут ещё: <b>{reward:.2f} монет</b>"
        )
    else:
        text = (
            f"❌ <b>Новых подписок не найдено</b>\n\n"
            f"📊 <b>Прогресс:</b> {total_done} / {total_all}\n\n"
            f"⚠️ Осталось: <b>{left}</b> спонсоров\n\n"
            f"<i>Убедись, что подписался на каналы из списка.</i>"
        )

    kb = InlineKeyboardBuilder()
    kb.button(text="🎯 Показать оставшихся", callback_data="earn_inline")
    kb.button(text="💰 Баланс", callback_data="balance_inline")
    kb.adjust(1)

    try:
        await cq.message.edit_text(text, reply_markup=kb.as_markup())
    except Exception:
        await cq.message.answer(text, reply_markup=kb.as_markup())


@dp.callback_query(F.data == "earn_inline")
async def cb_earn_inline(cq: types.CallbackQuery):
    uid = cq.from_user.id
    missing = await check_required(uid)
    if missing:
        await cq.answer("❌ Сначала ОП", show_alert=True)
        return

    new_links = await collect_sponsors(cq.from_user)
    pending = await pending_links(uid)
    all_links = list(dict.fromkeys(pending + new_links))[:await get_max_sponsors()]

    if not all_links:
        await cq.answer("😕 Оставшихся нет", show_alert=True)
        return

    reward = await get_reward()
    await cq.answer()
    try:
        await cq.message.edit_text(
            f"🎯 <b>Осталось подписаться: {len(all_links)}</b>\n\n"
            f"💵 За каждого: <b>{reward:.2f} монет</b>\n"
            f"⏳ Награда через <b>{HOLD_HOURS}ч</b>\n\n"
            f"Подпишись и жми «✅ Проверить подписки».",
            reply_markup=sponsors_kb(all_links),
            disable_web_page_preview=True
        )
    except Exception:
        pass


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


# ========== ТЕКСТ ==========
@dp.message(F.text & ~F.text.startswith("/"))
async def text_handler(msg: types.Message):
    uid = msg.from_user.id
    state = _states.get(uid)

    if state and state.startswith("req_step:"):
        await req_steps(msg)
        return

    if state == "await_promo":
        code = msg.text.strip()
        pool = await get_pool()
        async with pool.acquire() as db:
            promo = await db.fetchrow("SELECT * FROM promo_codes WHERE code=$1 AND active=TRUE", code)
            if not promo:
                await msg.answer("❌ Неверный промокод", reply_markup=get_menu(uid))
                _states.pop(uid, None)
                return
            if promo["used_count"] >= promo["max_uses"]:
                await msg.answer("❌ Промокод закончился", reply_markup=get_menu(uid))
                _states.pop(uid, None)
                return
            used = await db.fetchrow("SELECT 1 FROM promo_uses WHERE user_id=$1 AND code=$2", uid, code)
            if used:
                await msg.answer("❌ Ты уже активировал", reply_markup=get_menu(uid))
                _states.pop(uid, None)
                return
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
            await msg.answer("❌ Не понял сумму", reply_markup=get_menu(uid))
            return
        if amount < MIN_WITHDRAW:
            await msg.answer(f"❌ Минимум {MIN_WITHDRAW:.0f} монет", reply_markup=get_menu(uid))
            return
        u = await get_user(uid)
        bal = u["balance"] if u else 0
        if amount > bal:
            await msg.answer(f"❌ У тебя только {bal:.2f} монет", reply_markup=get_menu(uid))
            return
        _states.pop(uid, None)
        pool = await get_pool()
        async with pool.acquire() as db:
            res = await db.execute("UPDATE users SET balance = balance - $1 WHERE user_id=$2 AND balance >= $1", amount, uid)
            if res == "UPDATE 0":
                await msg.answer("❌ Недостаточно", reply_markup=get_menu(uid))
                return
            row = await db.fetchrow("INSERT INTO withdrawals (user_id, amount, status, created_at) VALUES ($1,$2,'pending',$3) RETURNING id", uid, amount, time.time())
            wid = row["id"]
        await msg.answer(f"⏳ Вывод {amount:.2f} монет обрабатывается…", reply_markup=get_menu(uid))
        resp = await bc_transfer(uid, amount)
        if resp.get("status") == "ok":
            pool = await get_pool()
            async with pool.acquire() as db:
                await db.execute("UPDATE withdrawals SET status='done', tx_id=$1 WHERE id=$2", resp.get("transaction_id"), wid)
            await msg.answer(f"✅ Выведено: <b>{amount:.2f} монет</b>\nTX: <code>{str(resp.get('transaction_id',''))[:16]}…</code>", reply_markup=get_menu(uid))
            try:
                await bot.send_message(ADMIN_ID, f"💸 Выплата\n👤 <code>{uid}</code>\n💰 {amount:.2f} монет\nTX: <code>{resp.get('transaction_id','')}</code>")
            except Exception:
                pass
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
            await msg.answer("❌ Число")
            return
        await set_setting("reward", val)
        _states.pop(uid, None)
        await msg.answer(f"✅ Награда: <b>{val:.2f} монет</b>", reply_markup=admin_menu())
        return

    if state == "adm_max":
        try:
            val = int(msg.text)
        except Exception:
            await msg.answer("❌ Целое")
            return
        await set_setting("max_sponsors", val)
        _states.pop(uid, None)
        await msg.answer(f"✅ Макс: <b>{val}</b>", reply_markup=admin_menu())
        return

    if state == "adm_broadcast":
        _states.pop(uid, None)
        await msg.answer("📢 Рассылка…")
        pool = await get_pool()
        async with pool.acquire() as db:
            rows = await db.fetch("SELECT user_id FROM users")
        ok = 0; fail = 0
        for r in rows:
            try:
                await msg.copy_to(r["user_id"])
                ok += 1
            except Exception:
                fail += 1
            await asyncio.sleep(0.05)
        await msg.answer(f"✅ {ok} / ❌ {fail}", reply_markup=admin_menu())
        return

    if state == "adm_promo_create":
        parts = [p.strip() for p in msg.text.split("|")]
        if len(parts) != 3:
            await msg.answer("❌ Формат: <code>КОД | СУММА | АКТИВАЦИЙ</code>")
            return
        code, amount_s, uses_s = parts
        try:
            amount = float(amount_s); uses = int(uses_s)
        except Exception:
            await msg.answer("❌ Числа")
            return
        pool = await get_pool()
        async with pool.acquire() as db:
            try:
                await db.execute("INSERT INTO promo_codes (code, amount, max_uses, created_at) VALUES ($1,$2,$3,$4)", code, amount, uses, time.time())
            except Exception:
                await msg.answer("❌ Такой код уже есть")
                return
        _states.pop(uid, None)
        await msg.answer(f"✅ Промокод <code>{code}</code>: {amount:.2f} монет × {uses}", reply_markup=admin_menu())
        return


# ========== ПОШАГОВОЕ ДОБАВЛЕНИЕ ОП ==========
@dp.callback_query(F.data == "adm_add_req")
async def adm_add_req(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    _states[cq.from_user.id] = "req_step:1"
    _states[f"{cq.from_user.id}_req"] = {}
    await cq.answer()
    try:
        await cq.message.edit_text(
            "📝 <b>Добавление ОП</b>\n\n"
            "<b>Шаг 1/4</b>\n\n"
            "Отправь <b>@username</b> канала или чата.\n\n"
            "Пример: <code>@my_channel</code>\n\n"
            "Для Bytecoin — отправь <code>bytecoin_app</code>\n\n"
            "Отмена: /cancel"
        )
    except Exception:
        pass


async def req_steps(msg: types.Message):
    uid = msg.from_user.id
    state = _states.get(uid, "")
    if not state.startswith("req_step:"):
        return

    step = int(state.split(":")[1])
    buf = _states.get(f"{uid}_req", {})
    text = msg.text.strip()

    if step == 1:
        if text == "bytecoin_app":
            buf["chat_id"] = "bytecoin_app"
            buf["title"] = "Bytecoin"
            buf["url"] = "https://t.me/byteappbot?start=r-FdLJFAyOe73"
            buf["type"] = "app"
            _states[f"{uid}_req"] = buf
            _states[uid] = "req_step:done"
            await save_required_and_finish(uid, buf)
            return

        try:
            chat = await bot.get_chat(text)
            buf["chat_id"] = str(chat.id)
            _states[f"{uid}_req"] = buf
            _states[uid] = "req_step:2"
            await msg.answer(
                f"✅ Нашёл: <b>{chat.title}</b>\n\n"
                f"<b>Шаг 2/4</b>\n\n"
                f"Отправь <b>ссылку</b> для кнопки.\n"
                f"Пример: <code>https://t.me/{text.replace('@','')}</code>\n\n"
                f"Отмена: /cancel"
            )
        except Exception as e:
            await msg.answer(
                f"⚠️ Не могу получить канал <code>{text}</code>\n\n"
                f"<b>Проверь:</b>\n"
                f"• Бот добавлен <b>админом</b> в канал\n"
                f"• Username правильный\n\n"
                f"Ошибка: <code>{e}</code>\n\n"
                f"Попробуй ещё раз или /cancel"
            )
        return

    if step == 2:
        if not text.startswith("http"):
            await msg.answer("❌ Ссылка должна начинаться с http\n\nПопробуй ещё раз:")
            return
        buf["url"] = text
        _states[f"{uid}_req"] = buf
        _states[uid] = "req_step:3"
        await msg.answer(
            f"<b>Шаг 3/4</b>\n\n"
            f"Отправь <b>название</b> для кнопки (что увидит юзер).\n"
            f"Пример: <code>📢 Новости</code>\n\n"
            f"Отмена: /cancel"
        )
        return

    if step == 3:
        buf["title"] = text
        _states[f"{uid}_req"] = buf
        _states[uid] = "req_step:4"
        kb = InlineKeyboardBuilder()
        kb.button(text="📢 Канал", callback_data="reqtype2:channel")
        kb.button(text="💬 Чат", callback_data="reqtype2:chat")
        kb.button(text="🚀 Приложение", callback_data="reqtype2:app")
        kb.adjust(1)
        await msg.answer(
            f"<b>Шаг 4/4</b>\n\n"
            f"Выбери <b>тип</b>:",
            reply_markup=kb.as_markup()
        )
        return


@dp.callback_query(F.data.startswith("reqtype2:"))
async def cb_reqtype2(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    rtype = cq.data.split(":")[1]
    uid = cq.from_user.id
    state = _states.get(uid, "")
    if state != "req_step:4":
        await cq.answer("Ошибка")
        return
    buf = _states.get(f"{uid}_req", {})
    buf["type"] = rtype
    _states[uid] = "req_step:done"
    await cq.answer("Добавляю…")
    try:
        await cq.message.edit_text("⏳ Добавляю…")
    except Exception:
        pass
    await save_required_and_finish(uid, buf)


async def save_required_and_finish(uid, buf):
    """ВАЖНО: передаём 5 аргументов: chat_id, title, url, type, created_at."""
    chat_id = buf.get("chat_id", "")
    title = buf.get("title", "")
    url = buf.get("url", "")
    rtype = buf.get("type", "channel")

    pool = await get_pool()
    async with pool.acquire() as db:
        try:
            await db.execute(
                """INSERT INTO required_channels 
                    (chat_id, title, url, type, active, created_at)
                   VALUES ($1, $2, $3, $4, TRUE, $5)
                   ON CONFLICT (chat_id) DO UPDATE 
                    SET title = EXCLUDED.title,
                        url = EXCLUDED.url,
                        type = EXCLUDED.type,
                        active = TRUE""",
                chat_id, title, url, rtype, time.time()
            )
        except Exception as e:
            logging.error(f"save_required error: {e}")
            await bot.send_message(uid, f"❌ Ошибка БД: <code>{e}</code>", reply_markup=admin_menu())
            _states.pop(uid, None)
            _states.pop(f"{uid}_req", None)
            return

    _states.pop(uid, None)
    _states.pop(f"{uid}_req", None)

    type_names = {"channel": "📢 Канал", "chat": "💬 Чат", "app": "🚀 Приложение"}
    await bot.send_message(
        uid,
        f"✅ <b>Добавлено!</b>\n\n"
        f"{type_names.get(rtype, rtype)} <b>{title}</b>\n"
        f"ID: <code>{chat_id}</code>\n"
        f"URL: {url}",
        reply_markup=admin_menu()
    )


# ========== АДМИН CALLBACKS ==========
@dp.callback_query(F.data == "adm_reward")
async def adm_reward(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_reward"
    await cq.answer()
    try:
        await cq.message.edit_text(f"💰 Текущая: <b>{await get_reward():.2f} монет</b>\n\nОтправь новое значение")
    except Exception:
        pass


@dp.callback_query(F.data == "adm_max")
async def adm_max(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_max"
    await cq.answer()
    try:
        await cq.message.edit_text(f"👥 Текущий: <b>{await get_max_sponsors()}</b>\n\nОтправь число")
    except Exception:
        pass


@dp.callback_query(F.data == "adm_stats")
async def adm_stats(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    pool = await get_pool()
    async with pool.acquire() as db:
        total = (await db.fetchrow("SELECT COUNT(*) as c FROM users"))["c"]
        balances = (await db.fetchrow("SELECT COALESCE(SUM(balance),0) as s FROM users"))["s"]
        held = (await db.fetchrow("SELECT COALESCE(SUM(held),0) as s FROM users"))["s"]
        subs = (await db.fetchrow("SELECT COUNT(*) as c FROM reward_holds WHERE status='released'"))["c"]
        holding = (await db.fetchrow("SELECT COUNT(*) as c FROM reward_holds WHERE status='holding'"))["c"]
        paid = (await db.fetchrow("SELECT COALESCE(SUM(amount),0) as s FROM withdrawals WHERE status='done'"))["s"]
        pend = (await db.fetchrow("SELECT COUNT(*) as c FROM withdrawals WHERE status='pending'"))["c"]
        reqs = (await db.fetchrow("SELECT COUNT(*) as c FROM required_channels WHERE active=TRUE"))["c"]
    await cq.answer()
    try:
        await cq.message.edit_text(
            f"📊 <b>Статистика</b>\n\n"
            f"👥 Юзеров: <b>{total}</b>\n"
            f"💰 Доступно у всех: <b>{balances:.2f}</b>\n"
            f"⏳ В холде: <b>{held:.2f}</b>\n"
            f"✅ Выплачено холдов: <b>{subs}</b>\n"
            f"⏳ Активных холдов: <b>{holding}</b>\n"
            f"💸 Выплачено монет: <b>{paid:.2f}</b>\n"
            f"📤 Заявок pending: <b>{pend}</b>\n"
            f"📢 ОП: <b>{reqs}</b>",
            reply_markup=admin_menu())
    except Exception:
        pass


@dp.callback_query(F.data == "adm_check_api")
async def adm_check_api(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    await cq.answer("Проверяю…")
    try:
        await cq.message.edit_text("⏳ Проверяю API спонсоров…")
    except Exception:
        pass

    results = []

    async def ping(name, url, method="POST", headers=None, json_data=None):
        s = await http()
        start = time.time()
        try:
            timeout = aiohttp.ClientTimeout(total=10)
            if method == "POST":
                async with s.post(url, headers=headers or {}, json=json_data or {}, timeout=timeout) as r:
                    ms = int((time.time() - start) * 1000)
                    return (name, r.status == 200, ms, f"HTTP {r.status}")
            else:
                async with s.get(url, headers=headers or {}, timeout=timeout) as r:
                    ms = int((time.time() - start) * 1000)
                    return (name, r.status == 200, ms, f"HTTP {r.status}")
        except asyncio.TimeoutError:
            ms = int((time.time() - start) * 1000)
            return (name, False, ms, "TIMEOUT")
        except Exception as e:
            ms = int((time.time() - start) * 1000)
            return (name, False, ms, str(e)[:60])

    if PIARFLOW_API_KEY:
        results.append(await ping("Piarflow", "https://piarflow.com/v1/sponsors",
            headers={"Authorization": f"Bearer {PIARFLOW_API_KEY}"},
            json_data={"user_id": 0, "chat_id": 0, "max_sponsors": 1}))
    else:
        results.append(("Piarflow", None, 0, "не настроен"))

    if TGRASS_API_KEY:
        results.append(await ping("TGrass", "https://tgrass.space/offers",
            headers={"Auth": TGRASS_API_KEY},
            json_data={"tg_user_id": 0, "tg_login": "", "lang": "ru", "is_premium": False}))
    else:
        results.append(("TGrass", None, 0, "не настроен"))

    if BOTOHUB_API_KEY:
        results.append(await ping("Botohub", "https://botohub.me/get-tasks",
            headers={"Auth": BOTOHUB_API_KEY}, json_data={"chat_id": 0}))
    else:
        results.append(("Botohub", None, 0, "не настроен"))

    if TRAFSLY_API_KEY:
        results.append(await ping("Trafsly", "https://api.trafsly.com/api/v1/get-sponsors",
            headers={"Auth": TRAFSLY_API_KEY},
            json_data={"user_id": 0, "max_sponsors": 1, "language_code": "ru", "is_premium": False}))
    else:
        results.append(("Trafsly", None, 0, "не настроен"))

    results.append(await ping("Bytecoin", f"{BC_BASE}/commerce/exchangeRate", method="GET"))

    text = "📡 <b>Проверка API</b>\n\n"
    for name, ok, ms, info in results:
        if ok is None:
            text += f"⚪ <b>{name}</b> — {info}\n"
        elif ok:
            text += f"✅ <b>{name}</b> — {ms} мс\n"
        else:
            text += f"❌ <b>{name}</b> — {ms} мс · <code>{info}</code>\n"

    text += f"\n<i>Обновлено: {time.strftime('%H:%M:%S')}</i>"

    kb = InlineKeyboardBuilder()
    kb.button(text="🔄 Обновить", callback_data="adm_check_api")
    kb.button(text="⬅️ Назад", callback_data="adm_back")
    kb.adjust(2)

    try:
        await cq.message.edit_text(text, reply_markup=kb.as_markup())
    except Exception:
        pass


@dp.callback_query(F.data == "adm_back")
async def adm_back(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    await cq.answer()
    try:
        await cq.message.edit_text("🛠 <b>Админ-панель</b>", reply_markup=admin_menu())
    except Exception:
        pass


@dp.callback_query(F.data == "adm_broadcast")
async def adm_broadcast(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_broadcast"
    await cq.answer()
    try:
        await cq.message.edit_text("📢 Отправь сообщение")
    except Exception:
        pass


@dp.callback_query(F.data == "adm_list_req")
async def adm_list_req(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch("SELECT id, chat_id, title, type FROM required_channels WHERE active=TRUE")
    if not rows:
        await cq.answer("Нет ОП", show_alert=True)
        return
    text = "📋 <b>ОП:</b>\n\n"
    for r in rows:
        text += f"#{r['id']} · {r['type']} · <b>{r['title']}</b>\n<code>{r['chat_id']}</code>\n\n"
    text += "\nУдалить: <code>/del_req ID</code>"
    await cq.answer()
    try:
        await cq.message.edit_text(text, reply_markup=admin_menu())
    except Exception:
        pass


@dp.message(Command("del_req"))
async def del_req(msg: types.Message):
    if msg.from_user.id != ADMIN_ID: return
    try:
        rid = int(msg.text.split()[1])
    except Exception:
        await msg.answer("Формат: <code>/del_req ID</code>")
        return
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE required_channels SET active=FALSE WHERE id=$1", rid)
    await msg.answer(f"✅ Удалено #{rid}", reply_markup=admin_menu())


@dp.callback_query(F.data == "adm_promo_create")
async def adm_promo_create(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_promo_create"
    await cq.answer()
    try:
        await cq.message.edit_text("Формат: <code>КОД | СУММА | АКТИВАЦИЙ</code>\nПример: <code>SALE | 500 | 100</code>")
    except Exception:
        pass


@dp.callback_query(F.data == "adm_promos")
async def adm_promos(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch("SELECT code, amount, max_uses, used_count, active FROM promo_codes ORDER BY created_at DESC LIMIT 30")
    if not rows:
        await cq.answer("Нет промокодов", show_alert=True)
        return
    text = "🎁 <b>Промокоды:</b>\n\n"
    for r in rows:
        status = "✅" if r["active"] else "❌"
        text += f"{status} <code>{r['code']}</code> — {r['amount']:.2f} ({r['used_count']}/{r['max_uses']})\n"
    await cq.answer()
    try:
        await cq.message.edit_text(text, reply_markup=admin_menu())
    except Exception:
        pass


@dp.callback_query(F.data == "adm_withdraws")
async def adm_withdraws(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch("SELECT id, user_id, amount, status, tx_id FROM withdrawals ORDER BY id DESC LIMIT 20")
    if not rows:
        await cq.answer("Нет заявок", show_alert=True)
        return
    text = "💸 <b>Выводы:</b>\n\n"
    for r in rows:
        text += f"#{r['id']} · <code>{r['user_id']}</code> · {r['amount']:.2f} · {r['status']}\n"
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
    dp.shutdown.register(on_shutdown)
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
