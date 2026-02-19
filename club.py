import os
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import aiosqlite
from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.client.session.aiohttp import AiohttpSession

MSK = ZoneInfo("Europe/Moscow")

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))
DB_PATH = os.getenv("DB_PATH", "bot.db")

PROXY_URL = (
    os.getenv("PROXY_URL")
    or os.getenv("HTTPS_PROXY")
    or os.getenv("https_proxy")
    or os.getenv("HTTP_PROXY")
    or os.getenv("http_proxy")
)

router = Router()

def is_admin(chat_id: int) -> bool:
    return chat_id == ADMIN_CHAT_ID

def user_label(u) -> str:
    if u.username:
        return f"@{u.username}"
    name = " ".join([x for x in [u.first_name, u.last_name] if x])
    return name if name else str(u.id)

def fmt_dt(ts: int) -> str:
    dt = datetime.fromtimestamp(ts, tz=MSK)
    return dt.strftime("%d.%m.%Y %H:%M")

def main_menu_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="Расписание", callback_data="menu:schedule")
    kb.adjust(1)
    return kb.as_markup()

def back_main_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="Назад", callback_data="menu:back")
    kb.adjust(1)
    return kb.as_markup()

def cancel_entry_btn_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="Отменить запись", callback_data="user:cancel_menu")
    kb.button(text="Назад", callback_data="menu:back")
    kb.adjust(1)
    return kb.as_markup()

def confirm_kb(event_id: int):
    kb = InlineKeyboardBuilder()
    kb.button(text="Да", callback_data=f"confirm:{event_id}:yes")
    kb.button(text="Нет", callback_data=f"confirm:{event_id}:no")
    kb.adjust(2)
    return kb.as_markup()

def admin_request_kb(event_id: int, user_id: int):
    kb = InlineKeyboardBuilder()
    kb.button(text="Подтвердить запись", callback_data=f"admin:approve:{event_id}:{user_id}")
    kb.button(text="Отклонить", callback_data=f"admin:decline:{event_id}:{user_id}")
    kb.adjust(1)
    return kb.as_markup()

def admin_events_kb(prefix: str, events_rows):
    kb = InlineKeyboardBuilder()
    for event_id, start_ts, title, capacity, remaining, link in events_rows:
        left_text = "МЕСТ НЕТ" if remaining <= 0 else f"{remaining}/{capacity}"
        kb.button(text=f"#{event_id} {fmt_dt(start_ts)} — {title} ({left_text})", callback_data=f"{prefix}:{event_id}")
    kb.adjust(1)
    return kb.as_markup()

async def db_init():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users(
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                started_ts INTEGER
            );
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS blocked_users(
                user_id INTEGER PRIMARY KEY,
                blocked_ts INTEGER NOT NULL
            );
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS events(
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                start_ts INTEGER NOT NULL,
                title TEXT NOT NULL,
                capacity INTEGER NOT NULL,
                remaining INTEGER NOT NULL,
                link TEXT
            );
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS requests(
                request_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                event_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                created_ts INTEGER NOT NULL
            );
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS signups(
                user_id INTEGER NOT NULL,
                event_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                confirm_status TEXT NOT NULL,
                confirmed_ts INTEGER,
                PRIMARY KEY(user_id, event_id)
            );
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS jobs(
                job_id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_type TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                event_id INTEGER NOT NULL,
                run_ts INTEGER NOT NULL,
                sent INTEGER NOT NULL DEFAULT 0
            );
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS admin_map(
                admin_msg_id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL
            );
        """)
        await db.commit()

async def db_is_blocked(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT 1 FROM blocked_users WHERE user_id=? LIMIT 1", (user_id,))
        return (await cur.fetchone()) is not None

async def db_block_user(user_id: int):
    now_ts = int(datetime.now(tz=MSK).timestamp())
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR REPLACE INTO blocked_users(user_id, blocked_ts) VALUES(?,?)", (user_id, now_ts))
        await db.commit()

async def db_unblock_user(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM blocked_users WHERE user_id=?", (user_id,))
        await db.commit()

async def db_user_upsert(u):
    now_ts = int(datetime.now(tz=MSK).timestamp())
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO users(user_id, username, first_name, last_name, started_ts) VALUES(?,?,?,?,?) "
            "ON CONFLICT(user_id) DO UPDATE SET username=excluded.username, first_name=excluded.first_name, last_name=excluded.last_name",
            (u.id, u.username, u.first_name, u.last_name, now_ts)
        )
        await db.commit()

async def db_add_admin_map(admin_msg_id: int, user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR REPLACE INTO admin_map(admin_msg_id, user_id) VALUES(?,?)", (admin_msg_id, user_id))
        await db.commit()

async def db_get_mapped_user(admin_msg_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT user_id FROM admin_map WHERE admin_msg_id=?", (admin_msg_id,))
        row = await cur.fetchone()
        return row[0] if row else None

async def db_all_users():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT user_id FROM users")
        return [r[0] for r in await cur.fetchall()]

async def db_list_events_future():
    now_ts = int(datetime.now(tz=MSK).timestamp())
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT event_id, start_ts, title, capacity, remaining, COALESCE(link,'') FROM events WHERE start_ts>? ORDER BY start_ts ASC",
            (now_ts,)
        )
        return await cur.fetchall()

async def db_get_event(event_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT event_id, start_ts, title, capacity, remaining, COALESCE(link,'') FROM events WHERE event_id=?",
            (event_id,)
        )
        return await cur.fetchone()

async def db_add_event(start_ts: int, title: str, capacity: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "INSERT INTO events(start_ts, title, capacity, remaining, link) VALUES(?,?,?,?,NULL)",
            (start_ts, title, capacity, capacity)
        )
        await db.commit()
        return cur.lastrowid

async def db_delete_event(event_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM events WHERE event_id=?", (event_id,))
        await db.execute("DELETE FROM requests WHERE event_id=?", (event_id,))
        await db.execute("DELETE FROM signups WHERE event_id=?", (event_id,))
        await db.execute("DELETE FROM jobs WHERE event_id=?", (event_id,))
        await db.commit()

async def db_set_link(event_id: int, link: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE events SET link=? WHERE event_id=?", (link, event_id))
        await db.commit()

async def db_create_request(user_id: int, event_id: int):
    now_ts = int(datetime.now(tz=MSK).timestamp())
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO requests(user_id, event_id, status, created_ts) VALUES(?,?,?,?)",
            (user_id, event_id, "pending", now_ts)
        )
        await db.commit()

async def db_has_pending_request(user_id: int, event_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT 1 FROM requests WHERE user_id=? AND event_id=? AND status='pending' LIMIT 1",
            (user_id, event_id)
        )
        return (await cur.fetchone()) is not None

async def db_mark_request(request_id: int, status: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE requests SET status=? WHERE request_id=?", (status, request_id))
        await db.commit()

async def db_find_pending_request(user_id: int, event_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT request_id FROM requests WHERE user_id=? AND event_id=? AND status='pending' ORDER BY created_ts DESC LIMIT 1",
            (user_id, event_id)
        )
        row = await cur.fetchone()
        return row[0] if row else None

async def db_signup_get(user_id: int, event_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT status, confirm_status FROM signups WHERE user_id=? AND event_id=?",
            (user_id, event_id)
        )
        return await cur.fetchone()

async def db_signup_confirm(user_id: int, event_id: int):
    now_ts = int(datetime.now(tz=MSK).timestamp())
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO signups(user_id, event_id, status, confirm_status, confirmed_ts) VALUES(?,?,?,?,?) "
            "ON CONFLICT(user_id, event_id) DO UPDATE SET status='confirmed', confirm_status='unknown', confirmed_ts=?",
            (user_id, event_id, "confirmed", "unknown", now_ts, now_ts)
        )
        await db.commit()

async def db_set_confirm_status(user_id: int, event_id: int, status: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE signups SET confirm_status=? WHERE user_id=? AND event_id=?",
            (status, user_id, event_id)
        )
        await db.commit()

async def db_set_signup_cancelled(user_id: int, event_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE signups SET status='cancelled', confirm_status='no' WHERE user_id=? AND event_id=?",
            (user_id, event_id)
        )
        await db.commit()

async def db_event_decrement_remaining(event_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("BEGIN IMMEDIATE;")
        cur = await db.execute("SELECT remaining FROM events WHERE event_id=?", (event_id,))
        row = await cur.fetchone()
        if not row:
            await db.execute("ROLLBACK;")
            return False
        rem = int(row[0])
        if rem <= 0:
            await db.execute("ROLLBACK;")
            return False
        await db.execute("UPDATE events SET remaining=? WHERE event_id=?", (rem - 1, event_id))
        await db.commit()
        return True

async def db_event_increment_remaining(event_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("BEGIN IMMEDIATE;")
        cur = await db.execute("SELECT remaining, capacity FROM events WHERE event_id=?", (event_id,))
        row = await cur.fetchone()
        if not row:
            await db.execute("ROLLBACK;")
            return
        rem, cap = int(row[0]), int(row[1])
        rem2 = rem + 1
        if rem2 > cap:
            rem2 = cap
        await db.execute("UPDATE events SET remaining=? WHERE event_id=?", (rem2, event_id))
        await db.commit()

async def db_add_job(job_type: str, user_id: int, event_id: int, run_ts: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO jobs(job_type, user_id, event_id, run_ts, sent) VALUES(?,?,?,?,0)",
            (job_type, user_id, event_id, run_ts)
        )
        await db.commit()

async def db_next_jobs(now_ts: int, limit: int = 30):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT job_id, job_type, user_id, event_id, run_ts FROM jobs WHERE sent=0 AND run_ts<=? ORDER BY run_ts ASC LIMIT ?",
            (now_ts, limit)
        )
        return await cur.fetchall()

async def db_mark_job_sent(job_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE jobs SET sent=1 WHERE job_id=?", (job_id,))
        await db.commit()

async def db_event_stats(event_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT u.user_id, COALESCE(u.username,''), COALESCE(u.first_name,''), COALESCE(u.last_name,'') "
            "FROM signups s JOIN users u ON u.user_id=s.user_id "
            "WHERE s.event_id=? AND s.status='confirmed' ORDER BY u.username ASC",
            (event_id,)
        )
        return await cur.fetchall()

async def db_cleanup_expired_events():
    now_ts = int(datetime.now(tz=MSK).timestamp())
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT event_id FROM events WHERE start_ts<=?", (now_ts,))
        rows = await cur.fetchall()
        for (eid,) in rows:
            await db.execute("DELETE FROM events WHERE event_id=?", (eid,))
            await db.execute("DELETE FROM requests WHERE event_id=?", (eid,))
            await db.execute("DELETE FROM signups WHERE event_id=?", (eid,))
            await db.execute("DELETE FROM jobs WHERE event_id=?", (eid,))
        await db.commit()

async def db_user_confirmed_future_events(user_id: int):
    now_ts = int(datetime.now(tz=MSK).timestamp())
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT e.event_id, e.start_ts, e.title, e.capacity, e.remaining, COALESCE(e.link,'') "
            "FROM signups s JOIN events e ON e.event_id=s.event_id "
            "WHERE s.user_id=? AND s.status='confirmed' AND e.start_ts>? "
            "ORDER BY e.start_ts ASC",
            (user_id, now_ts)
        )
        return await cur.fetchall()

async def admin_send_user_log(bot: Bot, user_id: int, text: str):
    msg = await bot.send_message(ADMIN_CHAT_ID, text)
    await db_add_admin_map(msg.message_id, user_id)
    return msg

async def build_schedule_kb():
    events = await db_list_events_future()
    kb = InlineKeyboardBuilder()
    for event_id, start_ts, title, capacity, remaining, link in events:
        left_text = "МЕСТ НЕТ" if remaining <= 0 else f"мест: {remaining}"
        kb.button(text=f"{fmt_dt(start_ts)} — {title} ({left_text})", callback_data=f"signup:{event_id}")
    kb.button(text="Отменить запись", callback_data="user:cancel_menu")
    kb.button(text="Назад", callback_data="menu:back")
    kb.adjust(1)
    return kb.as_markup()

async def build_user_cancel_kb(user_id: int):
    rows = await db_user_confirmed_future_events(user_id)
    kb = InlineKeyboardBuilder()
    for event_id, start_ts, title, capacity, remaining, link in rows:
        kb.button(text=f"{fmt_dt(start_ts)} — {title}", callback_data=f"user:cancel:{event_id}")
    kb.button(text="Назад", callback_data="menu:back")
    kb.adjust(1)
    return kb.as_markup(), rows

async def cancel_signup_flow(bot: Bot, user_id: int, event_id: int, by_admin: bool, admin_chat_id: int | None = None):
    ev = await db_get_event(event_id)
    if not ev:
        return False, "Встреча недоступна."
    _, start_ts, title, capacity, remaining, link = ev
    s = await db_signup_get(user_id, event_id)
    if not s or s[0] != "confirmed":
        return False, "Пользователь не записан(а) на эту встречу."
    await db_set_signup_cancelled(user_id, event_id)
    await db_event_increment_remaining(event_id)
    try:
        await bot.send_message(user_id, f"Ваша запись отменена: {fmt_dt(start_ts)} — {title}")
    except:
        pass
    if by_admin and admin_chat_id:
        await bot.send_message(admin_chat_id, f"Отменено: пользователь (id={user_id}) — #{event_id} {fmt_dt(start_ts)} — {title}")
    return True, f"Запись отменена: #{event_id} {fmt_dt(start_ts)} — {title}"

@router.message(CommandStart())
async def start(m: Message, bot: Bot):
    if not is_admin(m.chat.id) and await db_is_blocked(m.from_user.id):
        return
    await db_user_upsert(m.from_user)
    uname = user_label(m.from_user)
    await admin_send_user_log(bot, m.from_user.id, f"ℹ️ {uname} (id={m.from_user.id}) запустил(а) бота")
    await m.answer(
        f"Дорогая <i>{uname}</i>, нам очень приятно, что вас заинтересовал наш разговорный клуб! "
        f"Выберите то, что вас интересует или задайте вопрос в этом чате!",
        reply_markup=main_menu_kb(),
        parse_mode="HTML"
    )

@router.callback_query(F.data == "menu:back")
async def back_main(c: CallbackQuery, bot: Bot):
    if not is_admin(c.message.chat.id) and await db_is_blocked(c.from_user.id):
        await c.answer()
        return
    await c.message.edit_text("Выберите то, что вас интересует или задайте вопрос в этом чате!", reply_markup=main_menu_kb())
    await c.answer()

@router.callback_query(F.data == "menu:schedule")
async def schedule(c: CallbackQuery, bot: Bot):
    if not is_admin(c.message.chat.id) and await db_is_blocked(c.from_user.id):
        await c.answer()
        return
    await db_user_upsert(c.from_user)
    uname = user_label(c.from_user)
    await admin_send_user_log(bot, c.from_user.id, f"🗓️ {uname} (id={c.from_user.id}) открыл(а) Расписание")
    await c.message.edit_text("На какую встречу вы бы хотели записаться?", reply_markup=await build_schedule_kb())
    await c.answer()

@router.callback_query(F.data == "user:cancel_menu")
async def user_cancel_menu(c: CallbackQuery, bot: Bot):
    if not is_admin(c.message.chat.id) and await db_is_blocked(c.from_user.id):
        await c.answer()
        return
    kb, rows = await build_user_cancel_kb(c.from_user.id)
    if not rows:
        await c.message.answer("У вас нет активных записей на будущие встречи.", reply_markup=back_main_kb())
        await c.answer()
        return
    await c.message.answer("Выберите встречу, которую хотите отменить:", reply_markup=kb)
    await c.answer()

@router.callback_query(F.data.startswith("user:cancel:"))
async def user_cancel_pick(c: CallbackQuery, bot: Bot):
    if not is_admin(c.message.chat.id) and await db_is_blocked(c.from_user.id):
        await c.answer()
        return
    event_id = int(c.data.split(":")[2])
    ok, msg = await cancel_signup_flow(bot, c.from_user.id, event_id, by_admin=False)
    await c.message.answer(msg, reply_markup=back_main_kb())
    if ok:
        uname = user_label(c.from_user)
        ev = await db_get_event(event_id)
        if ev:
            _, start_ts, title, *_ = ev
            await admin_send_user_log(bot, c.from_user.id, f"❗ Отмена пользователем: {uname} (id={c.from_user.id}) отменил(а) #{event_id} {fmt_dt(start_ts)} — {title}")
    await c.answer()

@router.callback_query(F.data.startswith("signup:"))
async def signup_request(c: CallbackQuery, bot: Bot):
    if not is_admin(c.message.chat.id) and await db_is_blocked(c.from_user.id):
        await c.answer()
        return
    await db_user_upsert(c.from_user)
    event_id = int(c.data.split(":")[1])
    ev = await db_get_event(event_id)
    if not ev:
        await c.message.answer("Эта встреча уже недоступна.", reply_markup=back_main_kb())
        await c.answer()
        return
    _, start_ts, title, capacity, remaining, link = ev
    if remaining <= 0:
        await c.message.answer("На эту встречу уже нет мест.", reply_markup=back_main_kb())
        await c.answer()
        return
    if await db_has_pending_request(c.from_user.id, event_id):
        await c.message.answer("Ваша заявка уже отправлена. Мы скоро подтвердим.", reply_markup=back_main_kb())
        await c.answer()
        return
    s = await db_signup_get(c.from_user.id, event_id)
    if s and s[0] == "confirmed":
        await c.message.answer("Вы уже записаны на эту встречу.", reply_markup=cancel_entry_btn_kb())
        await c.answer()
        return
    await db_create_request(c.from_user.id, event_id)
    uname = user_label(c.from_user)
    admin_msg = await bot.send_message(
        ADMIN_CHAT_ID,
        f"‼️ ЗАЯВКА: {uname} (id={c.from_user.id}) хочет записаться на #{event_id} {fmt_dt(start_ts)} — {title}",
        reply_markup=admin_request_kb(event_id, c.from_user.id)
    )
    await db_add_admin_map(admin_msg.message_id, c.from_user.id)
    await c.message.answer("Заявка отправлена! Мы подтвердим и напишем вам здесь.", reply_markup=back_main_kb())
    await c.answer()

@router.callback_query(F.data.startswith("admin:approve:"))
async def admin_approve(c: CallbackQuery, bot: Bot):
    if not is_admin(c.message.chat.id):
        await c.answer()
        return
    _, _, event_id_s, user_id_s = c.data.split(":")
    event_id = int(event_id_s)
    user_id = int(user_id_s)

    ev = await db_get_event(event_id)
    if not ev:
        await c.message.edit_text("Эта встреча уже недоступна.")
        await c.answer()
        return

    ok = await db_event_decrement_remaining(event_id)
    if not ok:
        await c.message.edit_text("Не удалось подтвердить: мест уже нет.")
        await c.answer()
        return

    req_id = await db_find_pending_request(user_id, event_id)
    if req_id:
        await db_mark_request(req_id, "approved")

    await db_signup_confirm(user_id, event_id)

    _, start_ts, title, capacity, remaining, link = ev
    await c.message.edit_text(f"✅ Подтверждено: пользователь записан на #{event_id} {fmt_dt(start_ts)} — {title}")

    try:
        await bot.send_message(user_id, f"Вы записаны на встречу: {fmt_dt(start_ts)} — {title}", reply_markup=cancel_entry_btn_kb())
    except:
        pass

    now_ts = int(datetime.now(tz=MSK).timestamp())
    confirm_ts = int((datetime.fromtimestamp(start_ts, tz=MSK) - timedelta(hours=24)).timestamp())
    reminder_ts = int((datetime.fromtimestamp(start_ts, tz=MSK) - timedelta(hours=1)).timestamp())

    if confirm_ts <= now_ts:
        await db_add_job("confirm", user_id, event_id, now_ts)
    else:
        await db_add_job("confirm", user_id, event_id, confirm_ts)

    if reminder_ts <= now_ts:
        await db_add_job("reminder", user_id, event_id, now_ts)
    else:
        await db_add_job("reminder", user_id, event_id, reminder_ts)

    await c.answer()

@router.callback_query(F.data.startswith("admin:decline:"))
async def admin_decline(c: CallbackQuery, bot: Bot):
    if not is_admin(c.message.chat.id):
        await c.answer()
        return
    _, _, event_id_s, user_id_s = c.data.split(":")
    event_id = int(event_id_s)
    user_id = int(user_id_s)
    ev = await db_get_event(event_id)
    req_id = await db_find_pending_request(user_id, event_id)
    if req_id:
        await db_mark_request(req_id, "declined")
    if ev:
        _, start_ts, title, *_ = ev
        await c.message.edit_text(f"❌ Отклонено: заявка на #{event_id} {fmt_dt(start_ts)} — {title}")
        try:
            await bot.send_message(user_id, f"К сожалению, вашу заявку на {fmt_dt(start_ts)} — {title} мы не подтвердили.")
        except:
            pass
    else:
        await c.message.edit_text("❌ Отклонено: встреча уже недоступна.")
        try:
            await bot.send_message(user_id, "К сожалению, вашу заявку мы не подтвердили.")
        except:
            pass
    await c.answer()

@router.callback_query(F.data.startswith("confirm:"))
async def user_confirm(c: CallbackQuery, bot: Bot):
    if not is_admin(c.message.chat.id) and await db_is_blocked(c.from_user.id):
        await c.answer()
        return
    _, event_id_s, ans = c.data.split(":")
    event_id = int(event_id_s)
    s = await db_signup_get(c.from_user.id, event_id)
    ev = await db_get_event(event_id)
    if not s or s[0] != "confirmed" or not ev:
        await c.message.edit_text("Эта встреча уже недоступна.")
        await c.answer()
        return
    _, start_ts, title, capacity, remaining, link = ev

    if ans == "yes":
        await db_set_confirm_status(c.from_user.id, event_id, "yes")
        await c.message.edit_text("Спасибо, ждем вас!")
        uname = user_label(c.from_user)
        await admin_send_user_log(bot, c.from_user.id, f"✅ Подтверждение: {uname} (id={c.from_user.id}) подтвердил(а) участие в #{event_id} {fmt_dt(start_ts)} — {title}")
    else:
        await db_set_confirm_status(c.from_user.id, event_id, "no")
        await db_set_signup_cancelled(c.from_user.id, event_id)
        await db_event_increment_remaining(event_id)
        await c.message.edit_text("Жаль, что вы не сможете к нам прийти.")
        uname = user_label(c.from_user)
        await admin_send_user_log(bot, c.from_user.id, f"❗ Отмена: {uname} (id={c.from_user.id}) отказался(лась) от #{event_id} {fmt_dt(start_ts)} — {title}")
    await c.answer()

@router.message(F.chat.id == ADMIN_CHAT_ID, F.reply_to_message)
async def admin_reply(m: Message, bot: Bot):
    uid = await db_get_mapped_user(m.reply_to_message.message_id)
    if uid:
        await bot.copy_message(chat_id=uid, from_chat_id=m.chat.id, message_id=m.message_id)

@router.message(Command("to"))
async def admin_to(m: Message, bot: Bot):
    if not is_admin(m.chat.id):
        return
    parts = (m.text or "").split(maxsplit=2)
    if len(parts) < 3:
        await m.answer("Формат: /to <user_id> <текст>")
        return
    try:
        uid = int(parts[1])
    except:
        await m.answer("Формат: /to <user_id> <текст>")
        return
    await bot.send_message(uid, parts[2])

@router.message(Command("events"))
async def admin_events(m: Message, bot: Bot):
    if not is_admin(m.chat.id):
        return
    rows = await db_list_events_future()
    if not rows:
        await m.answer("Расписание пустое.")
        return
    lines = []
    for event_id, start_ts, title, capacity, remaining, link in rows:
        left_text = "МЕСТ НЕТ" if remaining <= 0 else f"{remaining}/{capacity}"
        link_txt = "link✅" if (link or "").strip() else "link—"
        lines.append(f"#{event_id} {fmt_dt(start_ts)} — {title} ({left_text}, {link_txt})")
    await m.answer("\n".join(lines))

@router.message(Command("add_event"))
async def admin_add_event(m: Message, bot: Bot):
    if not is_admin(m.chat.id):
        return
    txt = (m.text or "").strip()
    parts = txt.split(maxsplit=4)
    if len(parts) < 5:
        await m.answer("Формат: /add_event YYYY-MM-DD HH:MM <места> <название>")
        return
    date_s, time_s, cap_s, title = parts[1], parts[2], parts[3], parts[4]
    try:
        cap = int(cap_s)
        if cap <= 0:
            raise ValueError
    except:
        await m.answer("Места должны быть числом > 0. Формат: /add_event YYYY-MM-DD HH:MM <места> <название>")
        return
    try:
        dt = datetime.strptime(f"{date_s} {time_s}", "%Y-%m-%d %H:%M").replace(tzinfo=MSK)
        start_ts = int(dt.timestamp())
    except:
        await m.answer("Дата/время неверные. Формат: YYYY-MM-DD HH:MM (по МСК)")
        return
    now_ts = int(datetime.now(tz=MSK).timestamp())
    if start_ts <= now_ts:
        await m.answer("Нельзя добавить встречу в прошлом.")
        return
    eid = await db_add_event(start_ts, title, cap)
    await m.answer(f"Добавлено: #{eid} {fmt_dt(start_ts)} — {title} (мест: {cap})")

@router.message(Command("del_event"))
async def admin_del_event(m: Message, bot: Bot):
    if not is_admin(m.chat.id):
        return
    parts = (m.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].isdigit():
        await m.answer("Формат: /del_event <event_id>")
        return
    eid = int(parts[1])
    await db_delete_event(eid)
    await m.answer(f"Удалено (если существовало): #{eid}")

@router.message(Command("set_link"))
async def admin_set_link(m: Message, bot: Bot):
    if not is_admin(m.chat.id):
        return
    parts = (m.text or "").split(maxsplit=2)
    if len(parts) < 3 or not parts[1].isdigit():
        await m.answer("Формат: /set_link <event_id> <ссылка>")
        return
    eid = int(parts[1])
    link = parts[2].strip()
    await db_set_link(eid, link)
    await m.answer(f"Ссылка сохранена для #{eid}")

@router.message(Command("stats"))
async def admin_stats(m: Message, bot: Bot):
    if not is_admin(m.chat.id):
        return
    rows = await db_list_events_future()
    if not rows:
        await m.answer("Расписание пустое.")
        return
    await m.answer("Выберите встречу:", reply_markup=admin_events_kb("stats", rows))

@router.callback_query(F.data.startswith("stats:"))
async def admin_stats_pick(c: CallbackQuery, bot: Bot):
    if not is_admin(c.message.chat.id):
        await c.answer()
        return
    eid = int(c.data.split(":")[1])
    ev = await db_get_event(eid)
    if not ev:
        await c.message.edit_text("Встреча недоступна.")
        await c.answer()
        return
    _, start_ts, title, capacity, remaining, link = ev
    people = await db_event_stats(eid)
    if not people:
        await c.message.edit_text(f"#{eid} {fmt_dt(start_ts)} — {title}\nЗаписанных: 0")
        await c.answer()
        return
    lines = []
    for uid, username, fn, ln in people:
        if username:
            lines.append(f"@{username} (id={uid})")
        else:
            name = " ".join([x for x in [fn, ln] if x]).strip()
            lines.append(f"{name if name else 'user'} (id={uid})")
    await c.message.edit_text(f"#{eid} {fmt_dt(start_ts)} — {title}\nЗаписанных: {len(people)}\n\n" + "\n".join(lines))
    await c.answer()

@router.message(Command("broadcast_all"))
async def admin_broadcast_all(m: Message, bot: Bot):
    if not is_admin(m.chat.id):
        return
    text = (m.text or "").split(maxsplit=1)
    if len(text) < 2 or not text[1].strip():
        await m.answer("Формат: /broadcast_all <текст>")
        return
    msg = text[1]
    users = await db_all_users()
    sent = 0
    for uid in users:
        try:
            if await db_is_blocked(uid):
                continue
            await bot.send_message(uid, msg)
            sent += 1
        except:
            pass
    await m.answer(f"Рассылка отправлена: {sent}/{len(users)}")

@router.message(Command("broadcast"))
async def admin_broadcast(m: Message, bot: Bot):
    if not is_admin(m.chat.id):
        return
    txt = (m.text or "").split(maxsplit=2)
    if len(txt) < 3:
        await m.answer("Формат: /broadcast <user_ids через запятую или @username> <текст>\nПример: /broadcast 123,456 Привет!\nПример: /broadcast @user1,@user2 Привет!")
        return
    who = txt[1]
    msg = txt[2]
    targets = []
    tokens = [x.strip() for x in who.split(",") if x.strip()]
    async with aiosqlite.connect(DB_PATH) as db:
        for t in tokens:
            if t.startswith("@"):
                uname = t[1:]
                cur = await db.execute("SELECT user_id FROM users WHERE username=?", (uname,))
                row = await cur.fetchone()
                if row:
                    targets.append(int(row[0]))
            else:
                if t.isdigit():
                    targets.append(int(t))
    targets = list(dict.fromkeys(targets))
    sent = 0
    for uid in targets:
        try:
            if await db_is_blocked(uid):
                continue
            await bot.send_message(uid, msg)
            sent += 1
        except:
            pass
    await m.answer(f"Отправлено: {sent}/{len(targets)}")

@router.message(Command("cancel_signup"))
async def admin_cancel_signup(m: Message, bot: Bot):
    if not is_admin(m.chat.id):
        return
    parts = (m.text or "").split(maxsplit=2)
    if len(parts) < 3:
        await m.answer("Формат: /cancel_signup <event_id> <user_id или @username>")
        return
    if not parts[1].isdigit():
        await m.answer("Формат: /cancel_signup <event_id> <user_id или @username>")
        return
    event_id = int(parts[1])
    who = parts[2].strip()
    user_id = None
    if who.startswith("@"):
        uname = who[1:]
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute("SELECT user_id FROM users WHERE username=?", (uname,))
            row = await cur.fetchone()
            if row:
                user_id = int(row[0])
    else:
        if who.isdigit():
            user_id = int(who)
    if not user_id:
        await m.answer("Не найден пользователь. Укажите user_id или @username.")
        return
    ok, msg = await cancel_signup_flow(bot, user_id, event_id, by_admin=True, admin_chat_id=m.chat.id)
    await m.answer(msg)

@router.message(Command("block"))
async def admin_block(m: Message, bot: Bot):
    if not is_admin(m.chat.id):
        return
    parts = (m.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await m.answer("Формат: /block <user_id или @username>")
        return
    who = parts[1].strip()
    user_id = None
    if who.startswith("@"):
        uname = who[1:]
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute("SELECT user_id FROM users WHERE username=?", (uname,))
            row = await cur.fetchone()
            if row:
                user_id = int(row[0])
    else:
        if who.isdigit():
            user_id = int(who)
    if not user_id:
        await m.answer("Не найден пользователь. Укажите user_id или @username.")
        return
    await db_block_user(user_id)
    await m.answer(f"Пользователь заблокирован: id={user_id}")

@router.message(Command("unblock"))
async def admin_unblock(m: Message, bot: Bot):
    if not is_admin(m.chat.id):
        return
    parts = (m.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await m.answer("Формат: /unblock <user_id или @username>")
        return
    who = parts[1].strip()
    user_id = None
    if who.startswith("@"):
        uname = who[1:]
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute("SELECT user_id FROM users WHERE username=?", (uname,))
            row = await cur.fetchone()
            if row:
                user_id = int(row[0])
    else:
        if who.isdigit():
            user_id = int(who)
    if not user_id:
        await m.answer("Не найден пользователь. Укажите user_id или @username.")
        return
    await db_unblock_user(user_id)
    await m.answer(f"Пользователь разблокирован: id={user_id}")

@router.message()
async def any_message(m: Message, bot: Bot):
    if is_admin(m.chat.id):
        return
    if await db_is_blocked(m.from_user.id):
        return
    await db_user_upsert(m.from_user)
    uname = user_label(m.from_user)
    await admin_send_user_log(bot, m.from_user.id, f"✉️ Сообщение от {uname} (id={m.from_user.id})")
    copied = await bot.copy_message(chat_id=ADMIN_CHAT_ID, from_chat_id=m.chat.id, message_id=m.message_id)
    await db_add_admin_map(copied.message_id, m.from_user.id)

async def scheduler_loop(bot: Bot):
    while True:
        try:
            await db_cleanup_expired_events()
            now_ts = int(datetime.now(tz=MSK).timestamp())
            jobs = await db_next_jobs(now_ts, limit=50)
            for job_id, job_type, user_id, event_id, run_ts in jobs:
                ev = await db_get_event(event_id)
                s = await db_signup_get(user_id, event_id)
                if not ev or not s or s[0] != "confirmed":
                    await db_mark_job_sent(job_id)
                    continue
                if await db_is_blocked(user_id):
                    await db_mark_job_sent(job_id)
                    continue
                _, start_ts, title, capacity, remaining, link = ev
                if start_ts <= now_ts:
                    await db_mark_job_sent(job_id)
                    continue

                if job_type == "confirm":
                    await bot.send_message(
                        user_id,
                        f"Подтвердите, пожалуйста, что вы придете на встречу: {fmt_dt(start_ts)} — {title}",
                        reply_markup=confirm_kb(event_id)
                    )
                elif job_type == "reminder":
                    link_txt = (link or "").strip()
                    if link_txt:
                        await bot.send_message(user_id, f"Встреча скоро начнется: {fmt_dt(start_ts)} — {title}\nМесто проведения: {link_txt}")
                    else:
                        await bot.send_message(user_id, f"Встреча скоро начнется: {fmt_dt(start_ts)} — {title}\nМесто проведения: (ссылка пока не указана)")
                await db_mark_job_sent(job_id)
        except:
            pass
        await asyncio.sleep(20)

async def main():
    if not BOT_TOKEN or not ADMIN_CHAT_ID:
        raise RuntimeError("Set BOT_TOKEN and ADMIN_CHAT_ID")
    await db_init()
    session = AiohttpSession(proxy=PROXY_URL) if PROXY_URL else AiohttpSession()
    bot = Bot(BOT_TOKEN, session=session)
    dp = Dispatcher()
    dp.include_router(router)
    asyncio.create_task(scheduler_loop(bot))
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
