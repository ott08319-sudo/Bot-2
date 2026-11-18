import os
import re
import time
import json
import random
import asyncio
import logging
import html as html_lib

import asyncpg
import aiohttp
from aiohttp import web

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except Exception:
    pass

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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ========== КОНФИГУРАЦИЯ ==========
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
PORT = int(os.getenv("PORT", "8080"))

PIARFLOW_API_KEY = os.getenv("PIARFLOW_API_KEY", "")
FLOWLY_API_KEY = os.getenv("FLOWLY_API_KEY", "")
FLYER_API_KEY = os.getenv("FLYER_API_KEY", "")
MGRAM_API_KEY = os.getenv("MGRAM_API_KEY", "")
TRAFSLY_API_KEY = os.getenv("TRAFSLY_API_KEY", "")
BRIZAHUB_API_KEY = os.getenv("BRIZAHUB_API_KEY", os.getenv("BRIZA_API_KEY", ""))

DEFAULT_REWARD = 0.5
DEFAULT_MAX_SPONSORS = 10
DEFAULT_MIN_WITHDRAW = 15.0
DEFAULT_REF_BONUS = 0.25
MAX_MAX_SPONSORS = 30
CHECK_COOLDOWN = 3.0
FLOWLY_TIMEOUT = 8.0

LAZY_THRESHOLD = 3
LAZY_INTERVAL = 1800

REF_ACTIVE_MIN_SUBS = 3
REF_MILESTONES = {
    3: 2.0,
    5: 4.0,
    10: 10.0,
    25: 30.0,
    50: 75.0,
}

DAILY_REWARDS = [0.5, 0.75, 1.0, 1.25, 1.5, 2.0, 3.0]

SERVICES_ALL = ["piarflow", "flowly", "flyer", "mgram", "trafsly", "brizahub"]
INCOMPLETE_STATUSES = {"incomplete", "pending", "unsubscribed", "new", None, "abort"}

NICKNAMES = [
    "солнышко", "котик", "зайка", "звёздочка", "чемпион",
    "красавчик", "умничка", "дружок", "герой", "счастливчик"
]

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
flyer = Flyer(FLYER_API_KEY) if (Flyer and FLYER_API_KEY) else None

_pool = None
_http = None
_states = {}
_last = {}
_user_locks = {}


def pick_nickname():
    return random.choice(NICKNAMES)


def escape_html(text):
    return html_lib.escape(str(text or ""))


def fmt_money(val):
    try:
        v = float(val or 0)
    except Exception:
        return "0"
    if abs(v - round(v)) < 1e-6:
        return str(int(round(v)))
    return f"{v:.2f}".rstrip("0").rstrip(".")


def user_display(uid, username=None):
    if username:
        return f"@{escape_html(username.lstrip('@'))} (<code>{uid}</code>)"
    return f"<code>{uid}</code>"


async def get_user_display(uid):
    u = await get_user(uid)
    return user_display(uid, u["username"] if u else None)


async def http():
    global _http
    if _http is None or _http.closed:
        _http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12))
    return _http


async def get_pool():
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=15)
    return _pool


# ========== ИНИЦИАЛИЗАЦИЯ БД ==========
async def init_db():
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                referrer_id BIGINT,
                balance DOUBLE PRECISION DEFAULT 0,
                referred_earned DOUBLE PRECISION DEFAULT 0,
                blocked BOOLEAN DEFAULT FALSE,
                daily_streak INTEGER DEFAULT 0,
                last_daily_at DOUBLE PRECISION DEFAULT 0,
                sponsors_msg_id BIGINT,
                created_at DOUBLE PRECISION DEFAULT 0
            )
        """)
        await db.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS daily_streak INTEGER DEFAULT 0")
        await db.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_daily_at DOUBLE PRECISION DEFAULT 0")
        await db.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS sponsors_msg_id BIGINT")
        await db.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS referred_earned DOUBLE PRECISION DEFAULT 0")

        await db.execute("""
            CREATE TABLE IF NOT EXISTS sponsor_tasks (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                service TEXT NOT NULL,
                assignment_id TEXT NOT NULL,
                link TEXT NOT NULL,
                reward DOUBLE PRECISION DEFAULT 0.5,
                signature TEXT,
                task_type TEXT,
                session_id TEXT,
                provider_pool TEXT,
                status TEXT DEFAULT 'unsubscribed',
                created_at DOUBLE PRECISION DEFAULT 0,
                UNIQUE(user_id, service, assignment_id)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS withdrawals (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                amount DOUBLE PRECISION NOT NULL,
                wallet TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                created_at DOUBLE PRECISION DEFAULT 0
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS promo_codes (
                code TEXT PRIMARY KEY,
                amount DOUBLE PRECISION NOT NULL,
                max_uses INTEGER DEFAULT 1,
                used_count INTEGER DEFAULT 0,
                active BOOLEAN DEFAULT TRUE,
                created_at DOUBLE PRECISION DEFAULT 0
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS promo_uses (
                user_id BIGINT NOT NULL,
                code TEXT NOT NULL,
                used_at DOUBLE PRECISION DEFAULT 0,
                PRIMARY KEY (user_id, code)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS ref_milestones_claimed (
                user_id BIGINT NOT NULL,
                milestone INTEGER NOT NULL,
                claimed_at DOUBLE PRECISION DEFAULT 0,
                PRIMARY KEY (user_id, milestone)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS lazy_watch (
                user_id BIGINT PRIMARY KEY,
                referrer_id BIGINT,
                created_at DOUBLE PRECISION DEFAULT 0,
                last_notified DOUBLE PRECISION DEFAULT 0,
                admin_notified BOOLEAN DEFAULT FALSE
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS error_log (
                id SERIAL PRIMARY KEY,
                service TEXT,
                error_type TEXT,
                message TEXT,
                created_at DOUBLE PRECISION DEFAULT 0
            )
        """)


async def log_error(service, error_type, message):
    try:
        pool = await get_pool()
        async with pool.acquire() as db:
            await db.execute(
                "INSERT INTO error_log (service, error_type, message, created_at) VALUES ($1,$2,$3,$4)",
                str(service), str(error_type), str(message)[:500], time.time()
            )
    except Exception:
        pass


# ========== НАСТРОЙКИ ==========
async def get_setting(key, default):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT value FROM settings WHERE key=$1", key)
    return row["value"] if row else str(default)


async def set_setting(key, value):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute(
            "INSERT INTO settings (key, value) VALUES ($1,$2) ON CONFLICT (key) DO UPDATE SET value=$2",
            key, str(value)
        )


async def get_reward():
    return float(await get_setting("reward", DEFAULT_REWARD))


async def get_max_sponsors():
    return int(await get_setting("max_sponsors", DEFAULT_MAX_SPONSORS))


async def get_min_withdraw():
    return float(await get_setting("min_withdraw", DEFAULT_MIN_WITHDRAW))


async def get_ref_bonus():
    return float(await get_setting("ref_bonus", DEFAULT_REF_BONUS))


async def is_service_enabled(service):
    return (await get_setting(f"svc_{service}", "true")) == "true"


async def set_service_enabled(service, enabled: bool):
    await set_setting(f"svc_{service}", "true" if enabled else "false")


async def get_enabled_services():
    res = set()
    for s in SERVICES_ALL:
        if await is_service_enabled(s):
            res.add(s)
    return res


# ========== УВЕДОМЛЕНИЯ ==========
async def notify_referrer(ref_id, username, count, earned):
    try:
        who = f"@{escape_html(username)}" if username else "Твой друг"
        await bot.send_message(
            ref_id,
            f"👥 <b>Реферальный бонус!</b>\n\n"
            f"{who} выполнил <b>{count}</b> заданий.\n"
            f"⭐ Тебе начислено: <b>+{fmt_money(earned)}⭐</b>"
        )
    except Exception:
        pass


async def notify_admin_new_subs(user_id, new_subs):
    if not ADMIN_ID or not new_subs:
        return
    try:
        disp = await get_user_display(user_id)
        by_svc = {}
        total_rw = 0.0
        for svc, aid, rw, link, _ in new_subs:
            by_svc.setdefault(svc, []).append((link or aid, rw))
            total_rw += rw
        lines = [
            f"🆕 <b>Новые подписки!</b>\n",
            f"👤 {disp}\n",
            f"📦 <b>{len(new_subs)}</b> заданий:\n"
        ]
        for svc, items in by_svc.items():
            svc_sum = sum(x[1] for x in items)
            lines.append(f"⭐ <b>{escape_html(svc)}</b> ({len(items)} шт, +{fmt_money(svc_sum)}⭐)")
            for lnk, _ in items[:10]:
                lines.append(f"  • {escape_html(lnk)}")
        ts = time.strftime("%d.%m %H:%M", time.localtime())
        lines.append(f"\n⭐ Итого: <b>+{fmt_money(total_rw)}⭐</b>\n🕐 {ts}")
        await bot.send_message(ADMIN_ID, "\n".join(lines)[:4000], disable_web_page_preview=True)
    except Exception:
        pass


async def notify_admin_new_user(user_id, un, full_name, total_tasks, services_map, ref_id):
    if not ADMIN_ID:
        return
    try:
        svc_str = ", ".join(f"{k}: {v}" for k, v in services_map.items()) if services_map else "нет"
        ref_str = f"<code>{ref_id}</code>" if ref_id else "нет"
        await bot.send_message(
            ADMIN_ID,
            f"👤 <b>Новый пользователь!</b>\n\n"
            f"🆔 <code>{user_id}</code> {escape_html(un)}\n"
            f"📛 {escape_html(full_name)}\n"
            f"🔗 Реферер: {ref_str}\n"
            f"📦 Заданий выдано: <b>{total_tasks}</b> ({escape_html(svc_str)})"
        )
    except Exception:
        pass


async def notify_lazy_user(uid):
    try:
        nick = pick_nickname()
        await bot.send_message(
            uid,
            f"👋 <b>Эй, {nick}, ты забыл забрать свои звёзды!</b>\n\n"
            f"У тебя есть невыполненные задания — жми «⭐ Получить» и забирай награду!"
        )
        return True
    except Exception:
        return False


async def notify_admin_lazy(uid, total, mins):
    if not ADMIN_ID:
        return
    try:
        disp = await get_user_display(uid)
        await bot.send_message(
            ADMIN_ID,
            f"😴 <b>Ленивый реферал</b>\n👤 {disp}\n📊 Подписок: {total}\n⏱ Прошло: {mins} мин"
        )
    except Exception:
        pass


# ========== БЛОКИРОВКА ПРОВЕРКИ ПОЛЬЗОВАТЕЛЯ ==========
async def check_all_safe(uid):
    lock = _user_locks.setdefault(uid, asyncio.Lock())
    if lock.locked():
        return 0, 0.0, 0
    async with lock:
        return await check_all(uid)


# ========== РАБОТА С ПОЛЬЗОВАТЕЛЯМИ И ЗАДАЧАМИ ==========
async def set_sponsors_msg_id(user_id, msg_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE users SET sponsors_msg_id=$1 WHERE user_id=$2", msg_id, user_id)


async def get_sponsors_msg_id(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT sponsors_msg_id FROM users WHERE user_id=$1", user_id)
    return row["sponsors_msg_id"] if row else None


async def clear_sponsors_msg_id(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE users SET sponsors_msg_id=NULL WHERE user_id=$1", user_id)


async def register_user(user_id, referrer_id=None, username=None):
    pool = await get_pool()
    async with pool.acquire() as db:
        exists = await db.fetchrow("SELECT user_id FROM users WHERE user_id=$1", user_id)
        if exists:
            if username:
                await db.execute("UPDATE users SET username=$1 WHERE user_id=$2", username, user_id)
            return False
        if referrer_id == user_id:
            referrer_id = None
        if referrer_id:
            r = await db.fetchrow("SELECT user_id FROM users WHERE user_id=$1", referrer_id)
            if not r:
                referrer_id = None
        await db.execute(
            "INSERT INTO users (user_id, username, referrer_id, created_at) VALUES ($1,$2,$3,$4)",
            user_id, username, referrer_id, time.time()
        )
        return True


async def update_username(user_id, username):
    if not username:
        return
    try:
        pool = await get_pool()
        async with pool.acquire() as db:
            await db.execute("UPDATE users SET username=$1 WHERE user_id=$2", username, user_id)
    except Exception:
        pass


async def get_user(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        return await db.fetchrow(
            "SELECT balance, referrer_id, referred_earned, blocked, username, daily_streak, last_daily_at FROM users WHERE user_id=$1",
            user_id
        )


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


async def save_sponsor(user_id, service, aid, link, reward, signature=None, task_type=None, session_id=None, provider_pool=None):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute(
            """INSERT INTO sponsor_tasks
            (user_id, service, assignment_id, link, reward, signature, task_type, session_id, provider_pool, status, created_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'unsubscribed',$10)
            ON CONFLICT (user_id, service, assignment_id) DO UPDATE
                SET link = EXCLUDED.link,
                    reward = EXCLUDED.reward,
                    signature = COALESCE(EXCLUDED.signature, sponsor_tasks.signature),
                    task_type = COALESCE(EXCLUDED.task_type, sponsor_tasks.task_type),
                    session_id = COALESCE(EXCLUDED.session_id, sponsor_tasks.session_id),
                    provider_pool = COALESCE(EXCLUDED.provider_pool, sponsor_tasks.provider_pool),
                    status = CASE
                        WHEN sponsor_tasks.service = 'brizahub' THEN 'unsubscribed'
                        ELSE sponsor_tasks.status
                    END""",
            user_id, service, str(aid), link, reward,
            str(signature) if signature is not None else None,
            task_type,
            str(session_id) if session_id is not None else None,
            str(provider_pool) if provider_pool is not None else None,
            time.time()
        )


async def mark_subscribed(user_id, service, aid):
    pool = await get_pool()
    async with pool.acquire() as db:
        res = await db.execute(
            "UPDATE sponsor_tasks SET status='subscribed' WHERE user_id=$1 AND service=$2 AND assignment_id=$3 AND status!='subscribed'",
            user_id, service, str(aid)
        )
        return res != "UPDATE 0"


async def delete_stale_task(user_id, service, aid):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute(
            "DELETE FROM sponsor_tasks WHERE user_id=$1 AND service=$2 AND assignment_id=$3 AND status!='subscribed'",
            user_id, service, str(aid)
        )


async def clear_pending_tasks(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("DELETE FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed'", user_id)


async def pending_tasks(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch(
            """SELECT service, assignment_id, link, reward, signature, task_type, session_id, provider_pool
            FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed'""",
            user_id
        )
        return [
            (r["service"], r["assignment_id"], r["link"], r["reward"], r["signature"], r["task_type"], r["session_id"], r["provider_pool"])
            for r in rows
        ]


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


# ========== ПРОВЕРКА ВЕХ ЗА РЕФЕРАЛОВ ==========
async def check_ref_milestones(referrer_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow(
            """SELECT COUNT(*) as c FROM users u
               WHERE u.referrer_id = $1
               AND (SELECT COUNT(*) FROM sponsor_tasks st WHERE st.user_id = u.user_id AND st.status = 'subscribed') >= $2""",
            referrer_id, REF_ACTIVE_MIN_SUBS
        )
        active_refs = row["c"] if row else 0
        claimed_rows = await db.fetch("SELECT milestone FROM ref_milestones_claimed WHERE user_id=$1", referrer_id)
        claimed = {r["milestone"] for r in claimed_rows}

        for m_count, bonus in sorted(REF_MILESTONES.items()):
            if active_refs >= m_count and m_count not in claimed:
                await db.execute(
                    "INSERT INTO ref_milestones_claimed (user_id, milestone, claimed_at) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING",
                    referrer_id, m_count, time.time()
                )
                await add_balance(referrer_id, bonus)
                await add_referred_earned(referrer_id, bonus)
                try:
                    await bot.send_message(
                        referrer_id,
                        f"🏆 <b>Достижение разблокировано!</b>\n\n"
                        f"Ты пригласил <b>{m_count} активных друзей</b>!\n"
                        f"🎁 Твой бонус: <b>+{fmt_money(bonus)}⭐</b> зачислен на баланс!"
                    )
                except Exception:
                    pass


# ========== API: PIARFLOW ==========
async def get_piarflow(user):
    if not PIARFLOW_API_KEY:
        return []
    s = await http()
    try:
        payload = {
            "user_id": user.id,
            "chat_id": user.id,
            "max_sponsors": await get_max_sponsors(),
            "language_code": getattr(user, "language_code", None) or "ru",
            "is_premium": bool(getattr(user, "is_premium", False)),
            "username": getattr(user, "username", None) or "",
            "first_name": getattr(user, "first_name", None) or "",
            "last_name": getattr(user, "last_name", None) or "",
        }
        async with s.post(
            "https://piarflow.com/v1/sponsors",
            json=payload,
            headers={"Authorization": f"Bearer {PIARFLOW_API_KEY}", "Content-Type": "application/json"}
        ) as r:
            if r.status == 404:
                return []
            if r.status != 200:
                await log_error("piarflow", f"http_{r.status}", await r.text())
                return []
            d = await r.json()
            if d.get("status") == "ok":
                return d.get("sponsors") or []
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
            json={"user_id": int(user_id), "links": links},
            headers={"Authorization": f"Bearer {PIARFLOW_API_KEY}", "Content-Type": "application/json"}
        ) as r:
            if r.status != 200:
                return []
            d = await r.json()
            if d.get("status") == "ok":
                return d.get("sponsors") or []
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
            headers={"Auth": FLOWLY_API_KEY, "Content-Type": "application/json"},
            json={"tg_user_id": int(user_id), "lang": lang or "ru", "is_premium": bool(is_premium)},
            timeout=aiohttp.ClientTimeout(total=FLOWLY_TIMEOUT)
        ) as r:
            if r.status != 200:
                return None
            return await r.json()
    except Exception:
        return None


async def get_flowly(user):
    lang = getattr(user, "language_code", None) or "ru"
    is_prem = bool(getattr(user, "is_premium", False))
    d = await _flowly_request(user.id, lang, is_prem)
    if not d:
        return []
    out = []
    for item in d.get("offers") or []:
        if item.get("subscribed") is True:
            continue
        link = item.get("link")
        if link:
            out.append({"id": link, "link": link, "name": item.get("name", "")})
    return out


async def check_flowly(user_id, lang="ru"):
    d = await _flowly_request(user_id, lang)
    if not d:
        return None, {}
    offers_map = {}
    for item in d.get("offers") or []:
        link = item.get("link")
        if link:
            offers_map[link] = bool(item.get("subscribed"))
    return d.get("status"), offers_map


# ========== API: FLYER ==========
async def fetch_flyer(user_id, lang="ru"):
    if not flyer:
        return []
    try:
        tasks = await flyer.get_tasks(user_id=int(user_id), language_code=lang or "ru", limit=await get_max_sponsors())
        if not tasks:
            return []
        incomplete = [t for t in tasks if t.get("status") in INCOMPLETE_STATUSES]
        out = []
        for t in incomplete:
            links = t.get("links") or []
            sig = t.get("signature")
            if not links or not sig:
                continue
            out.append({"id": sig, "link": links[0], "signature": sig, "task": t.get("task", "")})
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


# ========== API: MGRAM ==========
async def get_mgram(user_id):
    if not MGRAM_API_KEY:
        return []
    s = await http()
    try:
        async with s.post(
            "https://api.mgram.biz/tasks",
            json={"user_id": int(user_id), "limit": min(await get_max_sponsors(), 10)},
            headers={"Authorization": f"Bearer {MGRAM_API_KEY}", "Content-Type": "application/json"}
        ) as r:
            if r.status != 200:
                return []
            d = await r.json()
            data = d.get("data") or {}
            if data.get("skip"):
                return []
            out = []
            for t in data.get("tasks") or []:
                if t.get("status") == "completed":
                    continue
                link = t.get("link")
                aid = t.get("id")
                if link and aid is not None:
                    out.append({"id": aid, "link": link})
            return out
    except Exception as e:
        await log_error("mgram", "fetch", str(e))
    return []


async def check_mgram(user_id):
    if not MGRAM_API_KEY:
        return False
    s = await http()
    try:
        async with s.post(
            "https://api.mgram.biz/tasks/check",
            json={"user_id": int(user_id)},
            headers={"Authorization": f"Bearer {MGRAM_API_KEY}", "Content-Type": "application/json"}
        ) as r:
            if r.status != 200:
                return False
            d = await r.json()
            return bool((d.get("data") or {}).get("skip"))
    except Exception as e:
        await log_error("mgram", "check", str(e))
    return False


# ========== API: TRAFSLY ==========
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
                link = x.get("link")
                aid = x.get("ads_id") or x.get("id")
                if link and aid is not None:
                    out.append({
                        "id": str(aid),
                        "link": link,
                        "source": x.get("source"),
                        "provider_pool": x.get("provider_pool")
                    })
            return out
    except Exception as e:
        await log_error("trafsly", "fetch", str(e))
    return []


async def check_trafsly(user_id, ads_id, source=None, provider_pool=None):
    if not TRAFSLY_API_KEY or not ads_id:
        return False
    s = await http()
    try:
        payload = {"user_id": int(user_id), "ads_id": str(ads_id)}
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
            return d.get("subscribed") is True or d.get("status") == "subscribed"
    except Exception as e:
        await log_error("trafsly", "check", str(e))
    return False


# ========== API: BRIZAHUB ==========
async def get_brizahub(user_id):
    if not BRIZAHUB_API_KEY:
        return []
    s = await http()
    limit = max(1, min(await get_max_sponsors(), 10))
    try:
        async with s.post(
            "https://api.brizahub.com/v1/tasks",
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
    if not BRIZAHUB_API_KEY or not task_ids:
        return set()
    s = await http()
    try:
        norm_ids = [int(x) if str(x).isdigit() else str(x) for x in task_ids]
        async with s.post(
            "https://api.brizahub.com/v1/tasks/check",
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


# ========== СБОР (с учётом выключенных) ==========
async def collect_sponsors(user):
    uid = user.id
    lang = getattr(user, "language_code", None) or "ru"
    reward = await get_reward()
    enabled = await get_enabled_services()

    async def _call(name, coro):
        if name in enabled:
            return await coro
        return []

    results = await asyncio.gather(
        _call("piarflow", get_piarflow(user)),
        _call("flowly", get_flowly(user)),
        _call("flyer", fetch_flyer(uid, lang)),
        _call("mgram", get_mgram(uid)),
        _call("trafsly", get_trafsly(user)),
        _call("brizahub", get_brizahub(uid)),
        return_exceptions=True,
    )

    piarflow, flowly, flyer_, mgram, trafsly, brizahub = results

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
    if isinstance(mgram, list):
        for x in mgram:
            if x.get("link") and x.get("id") is not None:
                await save_sponsor(uid, "mgram", str(x["id"]), x["link"], reward)
    if isinstance(trafsly, list):
        for x in trafsly:
            if x.get("link") and x.get("id") is not None:
                await save_sponsor(uid, "trafsly", str(x["id"]), x["link"], reward, signature=x.get("source"), provider_pool=x.get("provider_pool"))
    if isinstance(brizahub, list):
        for x in brizahub:
            if x.get("link") and x.get("id") is not None:
                await save_sponsor(uid, "brizahub", str(x["id"]), x["link"], reward, task_type=x.get("type"))


# ========== ПРОВЕРКА (с учётом выключенных) ==========
async def _task_reward(user_id, service, aid):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow(
            "SELECT reward FROM sponsor_tasks WHERE user_id=$1 AND service=$2 AND assignment_id=$3",
            user_id, service, str(aid)
        )
        return row["reward"] if row else 0.0


async def check_all(user_id):
    tasks = await pending_tasks(user_id)
    if not tasks:
        return 0, 0.0, 0

    enabled = await get_enabled_services()

    pf, fl, mg, tr, bh = [], [], [], [], []
    flowly_pending = []
    link_map, task_type_map = {}, {}

    for service, aid, link, reward, sig, task_type, _, pool_ in tasks:
        if service not in enabled:
            continue
        link_map[aid] = link
        task_type_map[aid] = task_type
        if service == "piarflow" and link:
            pf.append(link)
        elif service == "flyer" and sig:
            fl.append((aid, sig))
        elif service == "mgram" and aid:
            mg.append(aid)
        elif service == "trafsly" and aid:
            tr.append((aid, sig, pool_))
        elif service == "brizahub" and aid:
            bh.append(aid)
        elif service == "flowly":
            flowly_pending.append((aid, link, task_type))

    done_count, done_sum, skipped_count, new_subs = 0, 0.0, 0, []

    # 1. Piarflow
    if pf:
        for r in await check_piarflow(user_id, pf):
            st = r.get("status")
            lnk = r.get("link")
            if st == "subscribed" and lnk:
                if await mark_subscribed(user_id, "piarflow", lnk):
                    rw = await _task_reward(user_id, "piarflow", lnk)
                    await add_balance(user_id, rw)
                    done_count += 1
                    done_sum += rw
                    new_subs.append(("piarflow", lnk, rw, lnk, None))
            elif st == "not_counted" and lnk:
                await delete_stale_task(user_id, "piarflow", lnk)
                skipped_count += 1

    # 2. MGRAM
    if mg:
        if await check_mgram(user_id):
            pool = await get_pool()
            async with pool.acquire() as db:
                rows = await db.fetch(
                    "SELECT assignment_id, reward FROM sponsor_tasks WHERE user_id=$1 AND service='mgram' AND status!='subscribed'",
                    user_id
                )
                total_rw = sum(r["reward"] for r in rows)
                await db.execute("UPDATE sponsor_tasks SET status='subscribed' WHERE user_id=$1 AND service='mgram' AND status!='subscribed'", user_id)
            for r in rows:
                done_count += 1
                done_sum += r["reward"]
                new_subs.append(("mgram", r["assignment_id"], r["reward"], link_map.get(r["assignment_id"]), None))
            if total_rw > 0:
                await add_balance(user_id, total_rw)

    # 3. Trafsly
    if tr:
        for aid, src, pp in tr:
            if await check_trafsly(user_id, aid, src, pp):
                if await mark_subscribed(user_id, "trafsly", aid):
                    rw = await _task_reward(user_id, "trafsly", aid)
                    await add_balance(user_id, rw)
                    done_count += 1
                    done_sum += rw
                    new_subs.append(("trafsly", aid, rw, link_map.get(aid), None))

    # 4. Brizahub
    if bh:
        completed_bh = await check_brizahub(user_id, bh)
        for aid in bh:
            if str(aid) in completed_bh and await mark_subscribed(user_id, "brizahub", aid):
                rw = await _task_reward(user_id, "brizahub", aid)
                await add_balance(user_id, rw)
                done_count += 1
                done_sum += rw
                new_subs.append(("brizahub", aid, rw, link_map.get(aid), task_type_map.get(aid)))

    # 5. Flyer
    for aid, sig in fl:
        res_fl = await check_flyer(sig)
        if res_fl in ("complete", "subscribed"):
            if await mark_subscribed(user_id, "flyer", aid):
                rw = await _task_reward(user_id, "flyer", aid)
                await add_balance(user_id, rw)
                done_count += 1
                done_sum += rw
                new_subs.append(("flyer", aid, rw, link_map.get(aid), task_type_map.get(aid)))

    # 6. Flowly
    if flowly_pending:
        status, offers_map = await check_flowly(user_id)
        if status is not None:
            total_rw = 0.0
            for aid, link, tt in flowly_pending:
                is_sub = (status == "ok") or (offers_map.get(link) is True) or (link not in offers_map)
                if is_sub:
                    if await mark_subscribed(user_id, "flowly", aid):
                        rw = await _task_reward(user_id, "flowly", aid)
                        total_rw += rw
                        done_count += 1
                        done_sum += rw
                        new_subs.append(("flowly", aid, rw, link, tt))
            if total_rw > 0:
                await add_balance(user_id, total_rw)

    # Начисление рефереру и проверка этапов
    if done_count > 0:
        pool = await get_pool()
        async with pool.acquire() as db:
            row = await db.fetchrow("SELECT referrer_id, username FROM users WHERE user_id=$1", user_id)
        if row and row["referrer_id"] and row["referrer_id"] != user_id:
            ref_id = row["referrer_id"]
            ref_total = (await get_ref_bonus()) * done_count
            if ref_total > 0:
                await add_balance(ref_id, ref_total)
                await add_referred_earned(ref_id, ref_total)
                await notify_referrer(ref_id, row["username"] or "", done_count, ref_total)
            await check_ref_milestones(ref_id)

    if new_subs:
        await notify_admin_new_subs(user_id, new_subs)
    return done_count, done_sum, skipped_count


# ========== ВОРКЕРЫ (ТОЛЬКО LAZY WATCH) ==========
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
        json={"user_id": ADMIN_ID, "chat_id": ADMIN_ID, "max_sponsors": 5, "language_code": "ru"},
        headers={"Authorization": f"Bearer {PIARFLOW_API_KEY}", "Content-Type": "application/json"}
    ) as r:
        body = await r.text()
        if r.status == 404:
            return {"count": 0, "extra": " (нет офферов)"}
        if r.status != 200:
            return {"error": f"HTTP {r.status}", "count": 0}
        d = json.loads(body)
        if d.get("status") == "ok":
            return {"count": len(d.get("sponsors") or [])}
        return {"error": d.get("message", "unknown"), "count": 0}


async def _diag_mgram():
    if not MGRAM_API_KEY:
        return {"error": "ключ не задан", "count": 0}
    s = await http()
    async with s.post(
        "https://api.mgram.biz/tasks",
        json={"user_id": ADMIN_ID, "limit": 3},
        headers={"Authorization": f"Bearer {MGRAM_API_KEY}", "Content-Type": "application/json"}
    ) as r:
        body = await r.text()
        if r.status != 200:
            return {"error": f"HTTP {r.status}", "count": 0}
        d = json.loads(body)
        data = d.get("data") or {}
        if data.get("skip"):
            return {"count": 0, "extra": " (всё выполнено)"}
        return {"count": len([t for t in data.get("tasks") or [] if t.get("status") != "completed"])}


async def _diag_flowly():
    if not FLOWLY_API_KEY:
        return {"error": "ключ не задан", "count": 0}
    try:
        s = await http()
        async with s.post(
            "https://flowly.guru/api/v1/offers",
            headers={"Auth": FLOWLY_API_KEY, "Content-Type": "application/json"},
            json={"tg_user_id": ADMIN_ID, "lang": "ru", "is_premium": False},
            timeout=aiohttp.ClientTimeout(total=FLOWLY_TIMEOUT)
        ) as r:
            if r.status != 200:
                return {"error": f"HTTP {r.status}", "count": 0}
            d = await r.json()
            offers = [o for o in d.get("offers") or [] if not o.get("subscribed")]
            if not offers:
                return {"count": 0, "extra": " (всё выполнено)"}
            return {"count": len(offers)}
    except asyncio.TimeoutError:
        return {"error": "timeout", "count": 0}


async def _diag_flyer():
    if not flyer:
        return {"error": "flyerapi не подключён", "count": 0}
    tasks = await flyer.get_tasks(user_id=ADMIN_ID, language_code="ru", limit=5)
    if not tasks:
        return {"count": 0}
    return {"count": len([t for t in tasks if t.get("status") in INCOMPLETE_STATUSES])}


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
        return {"count": len(d.get("sponsors") or [])}


async def _diag_brizahub():
    if not BRIZAHUB_API_KEY:
        return {"error": "ключ не задан", "count": 0}
    s = await http()
    async with s.post(
        "https://api.brizahub.com/v1/tasks",
        json={"user_id": ADMIN_ID, "limit": 3},
        headers={"Authorization": f"Bearer {BRIZAHUB_API_KEY}", "Content-Type": "application/json"}
    ) as r:
        body = await r.text()
        if r.status != 200:
            return {"error": f"HTTP {r.status}", "count": 0}
        d = json.loads(body)
        tasks = [t for t in (d.get("tasks") or []) if (t.get("task_id") is not None or t.get("id") is not None) and (t.get("link") or t.get("url"))]
        return {"count": len(tasks)}


async def run_diagnostics():
    enabled = await get_enabled_services()
    checks = []
    mapping = [
        ("Piarflow", "piarflow", PIARFLOW_API_KEY, _diag_piarflow),
        ("Flowly", "flowly", FLOWLY_API_KEY, _diag_flowly),
        ("Flyer", "flyer", FLYER_API_KEY, _diag_flyer),
        ("MGRAM", "mgram", MGRAM_API_KEY, _diag_mgram),
        ("Trafsly", "trafsly", TRAFSLY_API_KEY, _diag_trafsly),
        ("BrizaHub", "brizahub", BRIZAHUB_API_KEY, _diag_brizahub),
    ]
    for name, key, api, fn in mapping:
        if key not in enabled:
            checks.append(f"⏸ <b>{name}</b> — выключен")
        else:
            checks.append(await diagnose_service(name, api, fn))
    return f"🔎 <b>Диагностика</b>\n🆔 <code>{ADMIN_ID}</code>\n\n" + "\n".join(checks)
