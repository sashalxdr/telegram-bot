import os
import asyncio
from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.client.session.aiohttp import AiohttpSession

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "527522505"))

PROXY_URL = (
    os.getenv("PROXY_URL")
    or os.getenv("HTTPS_PROXY")
    or os.getenv("https_proxy")
    or os.getenv("HTTP_PROXY")
    or os.getenv("http_proxy")
)

router = Router()
ADMIN_REPLY_MAP = {}

PRICELIST_TEXT = (
    "PRICELIST\n\n"
    "🍬посещение 1 встречи - 700 руб\n\n"
    "🍕membership - 4, 6 и 8 встреч в месяц\n"
    "4 встречи - 2400 руб. (600 х 4)\n"
    "6 встреч - 3300 руб. (550 х 6)\n"
    "8 встреч - 4000 руб. (500 х 8)"
)

EVENTS = {
    "ev1": '15 февраля в 19:00 - "ANTI-VALENTINE\'S DAY" FREE ENTRY'
}

def main_menu_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="ПРАЙС", callback_data="price")
    kb.button(text="Расписание", callback_data="schedule")
    kb.adjust(2)
    return kb.as_markup()

def back_main_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="Назад", callback_data="back_main")
    kb.adjust(1)
    return kb.as_markup()

def schedule_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text=EVENTS["ev1"], callback_data="ev1")
    kb.button(text="Назад", callback_data="back_main")
    kb.adjust(1)
    return kb.as_markup()

def user_label(u):
    if u.username:
        return f"@{u.username}"
    name = " ".join([x for x in [u.first_name, u.last_name] if x])
    return name if name else str(u.id)

async def admin_log(bot: Bot, text: str):
    await bot.send_message(ADMIN_CHAT_ID, text)

@router.message(CommandStart())
async def start(m: Message, bot: Bot):
    uname = user_label(m.from_user)
    await admin_log(bot, f"ℹ️ {uname} (id={m.from_user.id}) запустил(а) бота")
    await m.answer(
        f"Дорогая <i>{uname}</i>, нам очень приятно, что вас заинтересовал наш разговорный клуб! "
        f"Выберите то, что вас интересует или задайте вопрос в этом чате!",
        reply_markup=main_menu_kb(),
        parse_mode="HTML"
    )

@router.callback_query(F.data == "back_main")
async def back_main(c: CallbackQuery, bot: Bot):
    uname = user_label(c.from_user)
    await admin_log(bot, f"↩️ {uname} (id={c.from_user.id}) нажал(а) Назад")
    await c.message.edit_text(
        "Выберите то, что вас интересует или задайте вопрос в этом чате!",
        reply_markup=main_menu_kb()
    )
    await c.answer()

@router.callback_query(F.data == "price")
async def price(c: CallbackQuery, bot: Bot):
    uname = user_label(c.from_user)
    await admin_log(bot, f"💳 {uname} (id={c.from_user.id}) открыл(а) ПРАЙС")
    await c.message.edit_text(PRICELIST_TEXT, reply_markup=back_main_kb())
    await c.message.answer("Какой формат вам больше подходит?", reply_markup=back_main_kb())
    await c.answer()

@router.callback_query(F.data == "schedule")
async def schedule(c: CallbackQuery, bot: Bot):
    uname = user_label(c.from_user)
    await admin_log(bot, f"🗓️ {uname} (id={c.from_user.id}) открыл(а) Расписание")
    await c.message.edit_text("На какую встречу вы бы хотели записаться?", reply_markup=schedule_kb())
    await c.answer()

@router.callback_query(F.data.in_({"ev1", "ev2"}))
async def signup(c: CallbackQuery, bot: Bot):
    uname = user_label(c.from_user)
    event_text = EVENTS.get(c.data, c.data)
    await bot.send_message(ADMIN_CHAT_ID, f"‼️ {uname} (id={c.from_user.id}) хочет записаться на {event_text}")
    await c.message.answer(
        "Отлично! Я передала вашу заявку. Если хотите, напишите здесь вопрос или комментарий.",
        reply_markup=back_main_kb()
    )
    await c.answer()

@router.callback_query()
async def any_callback(c: CallbackQuery, bot: Bot):
    uname = user_label(c.from_user)
    await admin_log(bot, f"🔘 {uname} (id={c.from_user.id}) действие: {c.data}")
    await c.answer()

@router.message(Command("to"))
async def admin_to(m: Message, bot: Bot):
    if m.chat.id != ADMIN_CHAT_ID:
        return
    parts = (m.text or "").split(maxsplit=2)
    if len(parts) < 3:
        return
    try:
        uid = int(parts[1])
    except:
        return
    await bot.send_message(uid, parts[2])

@router.message(F.chat.id == ADMIN_CHAT_ID, F.reply_to_message)
async def admin_reply(m: Message, bot: Bot):
    rt = m.reply_to_message
    uid = ADMIN_REPLY_MAP.get(rt.message_id)
    if uid:
        await bot.copy_message(chat_id=uid, from_chat_id=m.chat.id, message_id=m.message_id)
        return

@router.message()
async def any_message(m: Message, bot: Bot):
    if m.chat.id == ADMIN_CHAT_ID:
        return
    uname = user_label(m.from_user)
    await admin_log(bot, f"✉️ Сообщение от {uname} (id={m.from_user.id})")
    copied = await bot.copy_message(chat_id=ADMIN_CHAT_ID, from_chat_id=m.chat.id, message_id=m.message_id)
    ADMIN_REPLY_MAP[copied.message_id] = m.from_user.id

async def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is empty")
    session = AiohttpSession(proxy=PROXY_URL) if PROXY_URL else AiohttpSession()
    bot = Bot(BOT_TOKEN, session=session)
    dp = Dispatcher()
    dp.include_router(router)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())

