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


# ========== BOTOHUB ==========
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


# ========== СБОР ==========
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


# ========== ВОРКЕР ==========
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
                            await bot.send_message(u["referrer_id"], f"💸 +{ref_bonus:.2f} BC с реферала")
                        except Exception:
                            pass
                    try:
                        await bot.send_message(uid, f"✅ Холд разблокирован: +{h['amount']:.2f} BC")
                    except Exception:
                        pass
                else:
                    try:
                        await bot.send_message(uid, f"❌ Холд отменён (отписка): {h['amount']:.2f} BC сгорели")
                    except Exception:
                        pass
        except Exception as e:
            logging.error(f"hold_worker: {e}")


# ========== ОП ==========
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
def main_menu():
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


def sponsors_kb(links):
    kb = InlineKeyboardBuilder()
    for i, link in enumerate(links, 1):
        kb.button(text=f"🔗 Спонсор {i}", url=link)
    kb.button(text="✅ Проверить", callback_data="check_subs")
    kb.adjust(1)
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
    await msg.answer(
        f"👋 Привет, {msg.from_user.first_name}!\n\n"
        f"💰 Доступно: <b>{bal:.2f} BC</b>\n"
        f"⏳ В холде: <b>{held:.2f} BC</b>\n"
        f"💵 За спонсора: <b>{await get_reward():.2f} BC</b>\n"
        f"📤 Минимум вывода: <b>{MIN_WITHDRAW:.0f} BC</b>\n"
        f"👥 Реферальный бонус: <b>{REF_PERCENT}%</b>\n"
        f"⏰ Холд: <b>{HOLD_HOURS}ч</b>",
        reply_markup=main_menu())


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
    u = await get_user(cq.from_user.id)
    bal = u["balance"] if u else 0
    held = u["held"] if u else 0
    try:
        await cq.message.edit_text("✅ Доступ открыт!")
    except Exception:
        pass
    await cq.message.answer(
        f"💰 Доступно: <b>{bal:.2f} BC</b>\n⏳ В холде: <b>{held:.2f} BC</b>",
        reply_markup=main_menu())


@dp.message(Command("admin"))
async def cmd_admin(msg: types.Message):
    if msg.from_user.id != ADMIN_ID: return
    await msg.answer("🛠 <b>Админ-панель</b>", reply_markup=admin_menu())


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
        await msg.answer("😕 Заданий нет, позже.", reply_markup=main_menu())
        return
    await msg.answer(
        f"🎯 Подпишись на <b>{len(all_links)}</b> канал(ов).\n"
        f"💵 За каждого — <b>{await get_reward():.2f} BC</b>.\n"
        f"⏳ Награда через <b>{HOLD_HOURS}ч</b>.",
        reply_markup=sponsors_kb(all_links),
        disable_web_page_preview=True)


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
        f"💰 Доступно: <b>{bal:.2f} BC</b>\n"
        f"⏳ В холде: <b>{held:.2f} BC</b>\n"
        f"📤 Минимум: <b>{MIN_WITHDRAW:.0f} BC</b>",
        reply_markup=main_menu())


@dp.message(F.text == "💸 Вывести")
async def msg_withdraw(msg: types.Message):
    missing = await check_required(msg.from_user.id)
    if missing:
        await msg.answer("❌ Сначала подпишись", reply_markup=required_kb(missing))
        return
    u = await get_user(msg.from_user.id)
    bal = u["balance"] if u else 0
    if bal < MIN_WITHDRAW:
        await msg.answer(f"❌ Минимум {MIN_WITHDRAW:.0f} BC. У тебя {bal:.2f}", reply_markup=main_menu())
        return
    _states[msg.from_user.id] = "await_withdraw"
    await msg.answer(
        f"💸 <b>Вывод</b>\n\n💰 Доступно: <b>{bal:.2f} BC</b>\n\n"
        f"Отправь сумму:\n<code>1500</code>",
        reply_markup=main_menu())


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
            f"💵 Заработано: <b>{earned:.2f} BC</b>")
    if top_refs:
        text += "\n\n🏆 <b>Топ-10:</b>\n"
        for i, r in enumerate(top_refs, 1):
            text += f"{i}. <code>{r['user_id']}</code> — {r['referred_earned']:.2f} BC\n"
    await msg.answer(text, reply_markup=main_menu())


@dp.message(F.text == "🎁 Промокод")
async def msg_promo(msg: types.Message):
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
        f"💰 Доступно: <b>{bal:.2f} BC</b>\n⏳ В холде: <b>{held:.2f} BC</b>",
        reply_markup=main_menu())


# ========== ПРОВЕРКА СПОНСОРОВ ==========
@dp.callback_query(F.data == "check_subs")
async def cb_check(cq: types.CallbackQuery):
    missing = await check_required(cq.from_user.id)
    if missing:
        await cq.answer("❌ Сначала подпишись на ОП", show_alert=True)
        return
    uid = cq.from_user.id
    if time.time() - _last.get(uid, 0) < CHECK_COOLDOWN:
        await cq.answer("⏱ Подожди")
        return
    _last[uid] = time.time()
    await cq.answer("Проверяю…")
    cnt, sm = await check_all(uid)
    left = await pending_count(uid)
    if cnt > 0:
        u = await get_user(uid)
        text = (f"✅ Засчитано: <b>{cnt}</b>\n"
                f"⏳ В холд на {HOLD_HOURS}ч: <b>{sm:.2f} BC</b>\n"
                f"💰 Доступно: <b>{u['balance']:.2f} BC</b>\n"
                f"⏳ В холде: <b>{u['held']:.2f} BC</b>")
        if left > 0:
            text += f"\n\n⚠️ Осталось: <b>{left}</b>"
        try:
            await cq.message.edit_text(text)
        except Exception:
            pass
        await cq.message.answer("Меню 👇", reply_markup=main_menu())
    else:
        await cq.answer(f"❌ Ничего. Осталось: {left}", show_alert=True)


# ========== ТЕКСТ ==========
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
                await msg.answer("❌ Неверный промокод", reply_markup=main_menu())
                _states.pop(uid, None)
                return
            if promo["used_count"] >= promo["max_uses"]:
                await msg.answer("❌ Промокод закончился", reply_markup=main_menu())
                _states.pop(uid, None)
                return
            used = await db.fetchrow("SELECT 1 FROM promo_uses WHERE user_id=$1 AND code=$2", uid, code)
            if used:
                await msg.answer("❌ Ты уже активировал", reply_markup=main_menu())
                _states.pop(uid, None)
                return
            await db.execute("UPDATE users SET balance = balance + $1 WHERE user_id=$2", promo["amount"], uid)
            await db.execute("UPDATE promo_codes SET used_count = used_count + 1 WHERE code=$1", code)
            await db.execute("INSERT INTO promo_uses (user_id, code, used_at) VALUES ($1,$2,$3)", uid, code, time.time())
        _states.pop(uid, None)
        await msg.answer(f"✅ Промокод: <b>+{promo['amount']:.2f} BC</b>", reply_markup=main_menu())
        return

    if state == "await_withdraw":
        try:
            amount = float(msg.text.replace(",", "."))
        except Exception:
            await msg.answer("❌ Не понял сумму", reply_markup=main_menu())
            return
        if amount < MIN_WITHDRAW:
            await msg.answer(f"❌ Минимум {MIN_WITHDRAW:.0f} BC", reply_markup=main_menu())
            return
        u = await get_user(uid)
        bal = u["balance"] if u else 0
        if amount > bal:
            await msg.answer(f"❌ У тебя только {bal:.2f} BC", reply_markup=main_menu())
            return
        _states.pop(uid, None)
        pool = await get_pool()
        async with pool.acquire() as db:
            res = await db.execute("UPDATE users SET balance = balance - $1 WHERE user_id=$2 AND balance >= $1", amount, uid)
            if res == "UPDATE 0":
                await msg.answer("❌ Недостаточно", reply_markup=main_menu())
                return
            row = await db.fetchrow("INSERT INTO withdrawals (user_id, amount, status, created_at) VALUES ($1,$2,'pending',$3) RETURNING id", uid, amount, time.time())
            wid = row["id"]
        await msg.answer(f"⏳ Вывод {amount:.2f} BC обрабатывается…", reply_markup=main_menu())
        resp = await bc_transfer(uid, amount)
        if resp.get("status") == "ok":
            pool = await get_pool()
            async with pool.acquire() as db:
                await db.execute("UPDATE withdrawals SET status='done', tx_id=$1 WHERE id=$2", resp.get("transaction_id"), wid)
            await msg.answer(f"✅ Выведено: <b>{amount:.2f} BC</b>\nTX: <code>{str(resp.get('transaction_id',''))[:16]}…</code>", reply_markup=main_menu())
            try:
                await bot.send_message(ADMIN_ID, f"💸 Выплата\n👤 <code>{uid}</code>\n💰 {amount:.2f} BC\nTX: <code>{resp.get('transaction_id','')}</code>")
            except Exception:
                pass
        else:
            pool = await get_pool()
            async with pool.acquire() as db:
                await db.execute("UPDATE users SET balance = balance + $1 WHERE user_id=$2", amount, uid)
                await db.execute("UPDATE withdrawals SET status='failed', error=$1 WHERE id=$2", resp.get("error", ""), wid)
            await msg.answer(f"❌ Ошибка: {resp.get('error','unknown')}\nСредства возвращены.", reply_markup=main_menu())
        return

    if state == "adm_reward":
        try:
            val = float(msg.text.replace(",", "."))
        except Exception:
            await msg.answer("❌ Число")
            return
        await set_setting("reward", val)
        _states.pop(uid, None)
        await msg.answer(f"✅ Награда: <b>{val:.2f} BC</b>", reply_markup=admin_menu())
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
        await msg.answer(f"✅ Промокод <code>{code}</code>: {amount:.2f} BC × {uses}", reply_markup=admin_menu())
        return


# ========== ФОРВАРД ОП ==========
@dp.message()
async def forward_req(msg: types.Message):
    uid = msg.from_user.id
    logging.info(f"FORWARD_DEBUG: uid={uid} admin_id={ADMIN_ID} forward_origin={getattr(msg,'forward_origin',None)} state={_states.get(uid)} text={msg.text}")

    if uid != ADMIN_ID:
        return
    if _states.get(uid) != "adm_forward":
        return

    origin = getattr(msg, "forward_origin", None)
    if origin is None:
        await msg.answer("❌ Это не форвард. Перешли пост из канала.")
        return

    chat = None
    if hasattr(origin, "chat") and origin.chat:
        chat = origin.chat
    elif hasattr(origin, "sender_chat") and origin.sender_chat:
        chat = origin.sender_chat

    if chat is None:
        await msg.answer("❌ Не могу определить канал. Возможно защита контента.")
        return

    cid = str(chat.id)
    title = chat.title or "Канал"
    username = getattr(chat, "username", None)
    url = f"https://t.me/{username}" if username else f"https://t.me/c/{str(chat.id)[4:]}"

    _states[uid] = f"adm_req_type:{cid}|{title}|{url}"

    kb = InlineKeyboardBuilder()
    kb.button(text="📢 Канал", callback_data=f"reqtype:channel:{cid}")
    kb.button(text="💬 Чат", callback_data=f"reqtype:chat:{cid}")
    kb.button(text="🚀 App", callback_data=f"reqtype:app:{cid}")
    kb.adjust(1)

    await msg.answer(f"✅ Получено: <b>{title}</b>\n\nВыбери тип:", reply_markup=kb.as_markup())


@dp.callback_query(F.data.startswith("reqtype:"))
async def cb_reqtype(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    parts = cq.data.split(":", 2)
    rtype, cid = parts[1], parts[2]
    st = _states.get(cq.from_user.id, "")
    if not st.startswith("adm_req_type:"):
        await cq.answer("Ошибка")
        return
    payload = st.replace("adm_req_type:", "").split("|")
    if len(payload) != 3:
        await cq.answer("Ошибка")
        return
    _, title, url = payload
    pool = await get_pool()
    async with pool.acquire() as db:
        try:
            await db.execute("INSERT INTO required_channels (chat_id, title, url, type, created_at) VALUES ($1,$2,$3,$4,$5)",
                             cid, title, url, rtype, time.time())
        except Exception:
            await cq.answer("Уже есть")
            return
    _states.pop(cq.from_user.id, None)
    await cq.answer("✅ Добавлено")
    try:
        await cq.message.edit_text(f"✅ <b>{title}</b> добавлен как {rtype}", reply_markup=admin_menu())
    except Exception:
        pass


# ========== АДМИН CALLBACKS ==========
@dp.callback_query(F.data == "adm_reward")
async def adm_reward(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_reward"
    await cq.answer()
    try:
        await cq.message.edit_text(f"💰 Текущая: <b>{await get_reward():.2f} BC</b>\n\nОтправь новое значение")
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
            f"💰 Доступно у всех: <b>{balances:.2f} BC</b>\n"
            f"⏳ В холде: <b>{held:.2f} BC</b>\n"
            f"✅ Выплачено холдов: <b>{subs}</b>\n"
            f"⏳ Активных холдов: <b>{holding}</b>\n"
            f"💸 Выплачено BC: <b>{paid:.2f}</b>\n"
            f"📤 Заявок pending: <b>{pend}</b>\n"
            f"📢 ОП: <b>{reqs}</b>",
            reply_markup=admin_menu())
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


@dp.callback_query(F.data == "adm_add_req")
async def adm_add_req(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_forward"
    await cq.answer()
    try:
        await cq.message.edit_text("Перешли пост из канала/чата")
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
        text += f"{status} <code>{r['code']}</code> — {r['amount']:.2f} BC ({r['used_count']}/{r['max_uses']})\n"
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
        text += f"#{r['id']} · <code>{r['user_id']}</code> · {r['amount']:.2f} BC · {r['status']}\n"
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


async def main():
    await init_db()
    await start_web()
    asyncio.create_task(hold_worker())
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
