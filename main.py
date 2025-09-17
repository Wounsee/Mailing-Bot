# main.py — полный рабочий файл
import os
import io
import json
import asyncio
import logging
import re
import tempfile
import sys
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

import asyncpg
from aiogram import Bot, Dispatcher, types
from aiogram.client.bot import DefaultBotProperties
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

# Надёжный импорт исключений (поддерживает разные версии aiogram)
try:
    from aiogram.exceptions import TelegramBadRequest, Forbidden
except Exception:
    TelegramBadRequest = Exception
    Forbidden = Exception

# ----------------- Конфигурация (через env) -----------------
DATABASE_URL = os.environ.get("DATABASE_URL")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
# Админы оставлены в коде (можно вынести в env при необходимости)
ADMIN_IDS: List[int] = [1627227943]

if not BOT_TOKEN or not DATABASE_URL:
    print("ERROR: BOT_TOKEN and DATABASE_URL must be set in environment variables", file=sys.stderr)
    raise RuntimeError("BOT_TOKEN and DATABASE_URL must be set")

# ----------------- Логирование -----------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ----------------- Aiogram init -----------------
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=None))
dp = Dispatcher(storage=MemoryStorage())

# ----------------- FSM -----------------
class S(StatesGroup):
    reg_wait_lang = State()
    reg_wait_channel = State()
    waiting_for_channel = State()
    waiting_for_text = State()
    waiting_for_buttons = State()
    waiting_for_modifiers = State()
    waiting_for_time = State()
    preview = State()
    admin_wait_delete = State()
    admin_wait_msg_channel = State()
    admin_wait_msg_text = State()
    admin_wait_user_block = State()
    admin_wait_mass_text = State()
    admin_wait_transfer = State()

# ----------------- Локализация -----------------
STRINGS = {
    "ru": {
        "start_welcome": "Добро пожаловать! Выберите язык:",
        "choose_lang": "Выберите язык:",
        "lang_selected": "Язык выбран: {lang}. Теперь добавьте канал (перешлите сообщение из него или отправьте @username или ID). Админ может пропустить.",
        "ask_channel": "Добавьте бота в канал как администратора, затем перешлите сюда любое сообщение из канала или отправьте его @username или ID (например -100...).",
        "ask_channel_short": "Добавить канал",
        "send_message_text": "Отправьте текст сообщения (без форматирования).",
        "channel_added": "Канал {title} добавлен/обновлён.",
        "channel_exists_other": "Этот канал уже зарегистрирован другим пользователем.",
        "channel_already": "Канал уже в вашем списке.",
        "main_menu": "Главное меню:",
        "add_buttons_prompt": "Отправьте кнопки по строкам: Текст - URL",
        "buttons_saved": "Кнопки сохранены.",
        "buttons_none": "Кнопок нет.",
        "invalid_buttons": "Некоторые строки пропущены (невалидный формат/URL):\n{lines}",
        "mod_added": "Модификатор {m} добавлен: {minutes}.",
        "enter_minutes": "Через сколько минут выполнить модификатор? (0 — немедленно)",
        "enter_new_text": "Отправьте новый текст, который нужно установить при срабатывании модификатора.",
        "preview_header": "Предпросмотр:",
        "published": "Опубликовано.",
        "publish_error": "Ошибка публикации: {err}",
        "no_channels": "У вас нет каналов. Сначала добавьте канал.",
        "not_owner": "Вы не владелец этого канала.",
        "channel_locked": "Этот канал заблокирован админом.",
        "manage_mailings": "Управление рассылками:",
        "no_mailings": "Нет рассылок.",
        "mailing_deleted": "Рассылка удалена.",
        "force_started": "Принудительное выполнение модификаторов запущено.",
        "admin_panel": "Админ-панель:",
        "wipe_done": "Данные удалены.",
        "delete_done": "Аккаунт и каналы удалены.",
        "maintenance_on": "Режим техработ: {state}.",
        "channels_html_sent": "Список каналов (HTML) отправлен.",
        "channels_csv_sent": "Список каналов (CSV) отправлен.",
        "admin_send_ok": "Отправлено в {target}.",
        "admin_send_err": "Ошибка отправки: {err}",
        "no_tasks": "Нет запланированных задач.",
        "tasks_list": "Запланированные задачи:\n{list}",
        "user_blocked": "Пользователь {uid} заблокирован.",
        "user_unblocked": "Пользователь {uid} разблокирован.",
        "mass_send_results": "Результаты массовой отправки:\n{res}",
        "stats": "Статистика:\nКаналов: {c}\nРассылок: {m}\nОжидающих задач: {t}",
        "blocked_notice": "Вы заблокированы и не можете пользоваться ботом.",
        "skip": "Пропустить",
        "watermark": "\n\n@MailingsManagerbot",
        "lang_ru": "Русский",
        "lang_en": "English",
        "add_buttons": "Добавить кнопки",
        "add_channel": "➕ Добавить канал",
        "cancel": "Отмена",
        "ok": "ОК",
        "error": "Ошибка",
        "invalid_user_id": "Неверный user_id",
        "deleted": "Удалено",
        "not_found": "Не найдено",
        "no_modifiers": "Нет модификаторов",
        "send_user_id": "Отправьте user_id:",
        "send_channel_id_new_owner": "Отправьте: <channel_id> <new_owner_id>",
        "send_user_id_toggle_block": "Отправьте user_id для блокировки/разблокировки:",
        "owner_changed": "Владелец изменён на {nid}",
        "back": "◀ Главное меню",
        "chat_not_found": "Ошибка: чат не найден. Проверьте ID/@username и добавлен ли бот в чат.",
        "forbidden": "Ошибка: доступ запрещён. Возможно, бот не админ или заблокирован."
    },
    "en": {
        "start_welcome": "Welcome! Choose language:",
        "choose_lang": "Choose language:",
        "lang_selected": "Language selected: {lang}. Now add a channel (forward a message from it or send its @username or ID). Admins may skip.",
        "ask_channel": "Add the bot to the channel as admin, then forward any message from the channel here, or send its @username or ID (e.g. -100...).",
        "ask_channel_short": "Add channel",
        "send_message_text": "Send message text (plain).",
        "channel_added": "Channel {title} added/updated.",
        "channel_exists_other": "This channel is already registered by another user.",
        "channel_already": "Channel already in your list.",
        "main_menu": "Main menu:",
        "add_buttons_prompt": "Send buttons, one per line: Text - URL",
        "buttons_saved": "Buttons saved.",
        "buttons_none": "No buttons.",
        "invalid_buttons": "Some lines skipped (invalid format/URL):\n{lines}",
        "mod_added": "Modifier {m} added: {minutes}.",
        "enter_minutes": "Enter minutes until modifier runs (0 — immediately)",
        "enter_new_text": "Send the new text that will replace the message when modifier triggers.",
        "preview_header": "Preview:",
        "published": "Published.",
        "publish_error": "Publish error: {err}",
        "no_channels": "You have no channels. Add one first.",
        "not_owner": "You are not the owner of this channel.",
        "channel_locked": "This channel is locked by admin.",
        "manage_mailings": "Manage mailings:",
        "no_mailings": "No mailings.",
        "mailing_deleted": "Mailing deleted.",
        "force_started": "Force modifier execution started.",
        "admin_panel": "Admin panel:",
        "wipe_done": "All data wiped.",
        "delete_done": "Account and channels deleted.",
        "maintenance_on": "Bot maintenance toggled to {state}.",
        "channels_html_sent": "Channels list (HTML) sent.",
        "channels_csv_sent": "Channels list (CSV) sent.",
        "admin_send_ok": "Sent to {target}.",
        "admin_send_err": "Send error: {err}",
        "no_tasks": "No scheduled tasks.",
        "tasks_list": "Pending tasks:\n{list}",
        "user_blocked": "User {uid} blocked.",
        "user_unblocked": "User {uid} unblocked.",
        "mass_send_results": "Mass send results:\n{res}",
        "stats": "Stats:\nChannels: {c}\nMailings: {m}\nPending tasks: {t}",
        "blocked_notice": "You are blocked from using this bot.",
        "skip": "Skip",
        "watermark": "\n\n@MailingsManagerbot",
        "lang_ru": "Русский",
        "lang_en": "English",
        "add_buttons": "Add buttons",
        "add_channel": "➕ Add channel",
        "cancel": "Cancel",
        "ok": "OK",
        "error": "Error",
        "invalid_user_id": "Invalid user_id",
        "deleted": "Deleted",
        "not_found": "Not found",
        "no_modifiers": "No modifiers",
        "send_user_id": "Send user_id:",
        "send_channel_id_new_owner": "Send: <channel_id> <new_owner_id>",
        "send_user_id_toggle_block": "Send user_id to toggle block/unblock:",
        "owner_changed": "Owner changed to {nid}",
        "back": "◀ Main menu",
        "chat_not_found": "Error: chat not found. Check ID/@username and bot membership.",
        "forbidden": "Error: forbidden. Bot may lack rights or is blocked."
    }
}

def tr(key: str, lang: str = "ru", **kwargs) -> str:
    if lang not in STRINGS:
        lang = "ru"
    return STRINGS[lang].get(key, key).format(**kwargs)

# ----------------- DB pool -----------------
db_pool: Optional[asyncpg.pool.Pool] = None

# ----------------- Utility funcs -----------------
def is_valid_url(u: str) -> bool:
    if not u:
        return False
    return bool(re.match(r"^https?://", u.strip(), re.IGNORECASE))

async def safe_delete_message(msg: types.Message):
    try:
        await msg.delete()
    except Exception:
        pass

async def try_edit_or_send(chat_id: Optional[int], message_id: Optional[int], text: str, reply_markup=None):
    if chat_id is None:
        raise ValueError("chat_id is None for try_edit_or_send")
    if message_id is not None:
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, reply_markup=reply_markup)
            return chat_id, message_id
        except Exception:
            logger.debug("edit failed, will send new message")
    sent = await bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup)
    return sent.chat.id, sent.message_id

def normalize_chat_input(raw: str) -> str:
    s = raw.strip()
    if s.startswith("@"):
        return s
    try:
        _ = int(s)
        return s
    except Exception:
        return s

# ----------------- DB helpers -----------------
async def init_db():
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
    async with db_pool.acquire() as conn:
        # таблицы
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS channels (
            channel_id TEXT PRIMARY KEY,
            channel_title TEXT,
            owner_id BIGINT,
            locked BOOLEAN DEFAULT FALSE,
            added_date TIMESTAMP DEFAULT (now() at time zone 'utc')
        );
        """)
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS mailings (
            id SERIAL PRIMARY KEY,
            channel_id TEXT REFERENCES channels(channel_id) ON DELETE SET NULL,
            message_text TEXT,
            buttons JSONB,
            modifiers JSONB,
            message_id BIGINT,
            created_date TIMESTAMP DEFAULT (now() at time zone 'utc')
        );
        """)
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS scheduled_tasks (
            id SERIAL PRIMARY KEY,
            mailing_id INT REFERENCES mailings(id) ON DELETE CASCADE,
            modifier_type TEXT,
            execute_time TIMESTAMP,
            executed BOOLEAN DEFAULT FALSE
        );
        """)
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS accounts (
            user_id BIGINT PRIMARY KEY,
            last_seen TIMESTAMP DEFAULT (now() at time zone 'utc'),
            lang TEXT DEFAULT 'ru',
            blocked BOOLEAN DEFAULT FALSE
        );
        """)
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        """)
    async with db_pool.acquire() as conn:
        v = await conn.fetchval("SELECT value FROM settings WHERE key='enabled'")
        if v is None:
            await conn.execute("INSERT INTO settings(key,value) VALUES('enabled','true') ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value")
        v2 = await conn.fetchval("SELECT value FROM settings WHERE key='mailing_count'")
        if v2 is None:
            await conn.execute("INSERT INTO settings(key,value) VALUES('mailing_count','0') ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value")

async def ensure_account(uid: int):
    async with db_pool.acquire() as conn:
        await conn.execute("INSERT INTO accounts(user_id,last_seen,lang,blocked) VALUES($1, now() at time zone 'utc','ru', false) ON CONFLICT (user_id) DO UPDATE SET last_seen=EXCLUDED.last_seen", uid)

async def get_account(uid: int):
    async with db_pool.acquire() as conn:
        return await conn.fetchrow("SELECT user_id, last_seen, COALESCE(lang,'ru') AS lang, COALESCE(blocked,false) AS blocked FROM accounts WHERE user_id=$1", uid)

async def set_lang(uid: int, lang: str):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE accounts SET lang=$1 WHERE user_id=$2", lang, uid)

async def set_block(uid: int, val: bool):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE accounts SET blocked=$1 WHERE user_id=$2", val, uid)

async def add_channel(channel_id: str, title: str, owner: int):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT owner_id FROM channels WHERE channel_id=$1", channel_id)
        if not row:
            await conn.execute("INSERT INTO channels(channel_id,channel_title,owner_id) VALUES($1,$2,$3)", channel_id, title, owner)
            return True, "added"
        existing = row["owner_id"]
        if existing is None:
            await conn.execute("UPDATE channels SET owner_id=$1, channel_title=$2 WHERE channel_id=$3", owner, title, channel_id)
            return True, "added"
        if existing == owner:
            await conn.execute("UPDATE channels SET channel_title=$1 WHERE channel_id=$2", title, channel_id)
            return True, "already"
        return False, "exists"

async def get_user_channels(uid: int):
    async with db_pool.acquire() as conn:
        return await conn.fetch("SELECT channel_id,channel_title,owner_id,locked FROM channels WHERE owner_id=$1 ORDER BY added_date DESC", uid)

async def get_all_channels():
    async with db_pool.acquire() as conn:
        return await conn.fetch("SELECT channel_id,channel_title,owner_id,locked FROM channels ORDER BY added_date DESC")

async def get_channel(channel_id: str):
    async with db_pool.acquire() as conn:
        return await conn.fetchrow("SELECT channel_id,channel_title,owner_id,locked FROM channels WHERE channel_id=$1", channel_id)

async def set_channel_owner(channel_id: str, new_owner: int):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE channels SET owner_id=$1 WHERE channel_id=$2", new_owner, channel_id)

async def add_mailing(channel_id, text, buttons, modifiers, message_id=None):
    async with db_pool.acquire() as conn:
        rec = await conn.fetchrow("INSERT INTO mailings(channel_id,message_text,buttons,modifiers,message_id) VALUES($1,$2,$3,$4,$5) RETURNING id", channel_id, text, json.dumps(buttons), json.dumps(modifiers), message_id)
        return rec["id"] if rec else None

async def update_mailing_message_id(mid: int, message_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE mailings SET message_id=$1 WHERE id=$2", message_id, mid)

async def delete_mailing(mid: int):
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM mailings WHERE id=$1", mid)

async def get_mailing(mid: int):
    async with db_pool.acquire() as conn:
        return await conn.fetchrow("SELECT id,channel_id,message_text,buttons,modifiers,message_id FROM mailings WHERE id=$1", mid)

async def add_task(mailing_id: int, modifier_type: str, execute_time: datetime):
    async with db_pool.acquire() as conn:
        rec = await conn.fetchrow("INSERT INTO scheduled_tasks(mailing_id,modifier_type,execute_time) VALUES($1,$2,$3) RETURNING id", mailing_id, modifier_type, execute_time)
        return rec["id"] if rec else None

async def get_pending_tasks():
    async with db_pool.acquire() as conn:
        return await conn.fetch("SELECT id,mailing_id,modifier_type,execute_time FROM scheduled_tasks WHERE executed=false")

async def mark_executed(task_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE scheduled_tasks SET executed=true WHERE id=$1", task_id)

async def cleanup_db(days: int = 90):
    cutoff = datetime.utcnow() - timedelta(days=days)
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM scheduled_tasks WHERE executed=true AND execute_time < $1", cutoff)
        await conn.execute("DELETE FROM mailings WHERE channel_id IS NULL OR created_date < $1", cutoff)
        await conn.execute("DELETE FROM accounts WHERE last_seen < $1", cutoff)

async def get_setting(k: str):
    async with db_pool.acquire() as conn:
        return await conn.fetchval("SELECT value FROM settings WHERE key=$1", k)

async def set_setting(k: str, v: str):
    async with db_pool.acquire() as conn:
        await conn.execute("INSERT INTO settings(key,value) VALUES($1,$2) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value", k, v)

async def incr_mailing_count():
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            v = await conn.fetchval("SELECT value FROM settings WHERE key='mailing_count' FOR UPDATE")
            if v is None:
                await conn.execute("INSERT INTO settings(key,value) VALUES('mailing_count','1') ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value")
                return 1
            n = int(v) + 1
            await conn.execute("UPDATE settings SET value=$1 WHERE key='mailing_count'", str(n))
            return n

# ----------------- Scheduler -----------------
async def schedule_pending_on_startup():
    rows = await get_pending_tasks()
    now = datetime.utcnow()
    for r in rows:
        delay = max((r["execute_time"] - now).total_seconds(), 0)
        asyncio.create_task(run_scheduled(r["id"], r["mailing_id"], r["modifier_type"], delay))

async def run_scheduled(task_id: int, mailing_id: int, modifier_type: str, delay: float):
    await asyncio.sleep(delay)
    try:
        await apply_modifier(mailing_id, modifier_type)
    except Exception:
        logger.exception("scheduled apply error")
    await mark_executed(task_id)

async def apply_modifier(mailing_id: int, modifier_type: str):
    m = await get_mailing(mailing_id)
    if not m:
        return
    ch_id = m["channel_id"]
    text = m["message_text"]
    buttons = json.loads(m["buttons"]) if m["buttons"] else []
    msg_id = m["message_id"]
    kb = None
    if buttons:
        kb = types.InlineKeyboardMarkup(inline_keyboard=[[types.InlineKeyboardButton(text=b["text"], url=b["url"])] for b in buttons if is_valid_url(b.get("url"))])
    try:
        if modifier_type == "unpin":
            try:
                await bot.unpin_chat_message(chat_id=ch_id, message_id=msg_id)
            except Exception:
                try:
                    await bot.unpin_all_chat_messages(chat_id=ch_id)
                except Exception:
                    logger.exception("unpin")
        elif modifier_type == "delete":
            try:
                if msg_id:
                    await bot.delete_message(chat_id=ch_id, message_id=msg_id)
            except Exception:
                logger.exception("delete")
            async with db_pool.acquire() as conn:
                await conn.execute("UPDATE mailings SET message_id=NULL WHERE id=$1", mailing_id)
        elif modifier_type == "edit":
            new_text = f"{text}\n\n✏️ {datetime.utcnow().isoformat()}"
            try:
                if msg_id:
                    await bot.edit_message_text(chat_id=ch_id, message_id=msg_id, text=new_text)
            except Exception:
                logger.exception("edit")
        elif modifier_type == "resend":
            try:
                if msg_id:
                    await bot.delete_message(chat_id=ch_id, message_id=msg_id)
            except Exception:
                pass
            try:
                sent = await bot.send_message(chat_id=ch_id, text=text)
                await update_mailing_message_id(mailing_id, sent.message_id)
            except Exception:
                logger.exception("resend")
        elif modifier_type == "update_buttons":
            try:
                if msg_id:
                    await bot.edit_message_reply_markup(chat_id=ch_id, message_id=msg_id, reply_markup=kb)
            except Exception:
                logger.exception("update buttons")
        elif modifier_type == "replace_text":
            rec = await get_mailing(mailing_id)
            mods = json.loads(rec["modifiers"]) if rec and rec["modifiers"] else {}
            new_text = mods.get("replace_text_new")
            if new_text and msg_id:
                try:
                    await bot.edit_message_text(chat_id=ch_id, message_id=msg_id, text=new_text)
                except Exception:
                    logger.exception("replace text")
        elif modifier_type == "forward_to":
            rec = await get_mailing(mailing_id)
            mods = json.loads(rec["modifiers"]) if rec and rec["modifiers"] else {}
            target = mods.get("forward_to_channel")
            try:
                if msg_id:
                    await bot.forward_message(chat_id=target, from_chat_id=ch_id, message_id=msg_id)
                else:
                    await bot.send_message(chat_id=target, text=text)
            except Exception:
                logger.exception("forward_to")
    except Exception:
        logger.exception("apply_modifier overall")

# ----------------- Middlewares -----------------
@dp.message.middleware()
async def mw_last_seen(handler, event: types.Message, data):
    if isinstance(event, types.Message):
        try:
            await ensure_account(event.from_user.id)
            async with db_pool.acquire() as conn:
                await conn.execute("UPDATE accounts SET last_seen = now() at time zone 'utc' WHERE user_id=$1", event.from_user.id)
        except Exception:
            logger.exception("mw_last_seen")
    return await handler(event, data)

# ----------------- Keyboards -----------------
async def keyboard_main(uid: int) -> types.InlineKeyboardMarkup:
    acc = await get_account(uid)
    lang = acc["lang"] if acc else "ru"
    chs = await get_user_channels(uid)
    rows: List[List[types.InlineKeyboardButton]] = []
    if chs:
        for c in chs:
            title = c["channel_title"] or c["channel_id"]
            if c["locked"]:
                title = "🔒 " + title
            rows.append([types.InlineKeyboardButton(text=f"📣 {title}", callback_data=f"create_mailing:{c['channel_id']}")])
    rows.append([types.InlineKeyboardButton(text=tr("add_channel", lang), callback_data="add_channel")])
    rows.append([types.InlineKeyboardButton(text="📋 " + tr("manage_mailings", lang), callback_data="manage_mailings")])
    rows.append([types.InlineKeyboardButton(text="🌐 " + tr("choose_lang", lang), callback_data="change_lang")])
    if uid in ADMIN_IDS:
        rows.append([types.InlineKeyboardButton(text="⚙️ " + tr("admin_panel", lang), callback_data="admin_panel")])
    return types.InlineKeyboardMarkup(inline_keyboard=rows)

def back_button_row(lang: str):
    return [types.InlineKeyboardButton(text=tr("back", lang), callback_data="back_to_main")]

# ----------------- Handlers -----------------
@dp.message(Command("start"))
async def cmd_start(m: types.Message, state: FSMContext):
    await ensure_account(m.from_user.id)
    acc = await get_account(m.from_user.id)
    if acc and acc["blocked"]:
        await m.answer(tr("blocked_notice", acc["lang"]))
        await safe_delete_message(m)
        return
    # если пользователь без языка — предлагаем выбор
    if not acc or not acc["lang"]:
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text=STRINGS["ru"]["lang_ru"], callback_data="lang:ru")],
            [types.InlineKeyboardButton(text=STRINGS["en"]["lang_en"], callback_data="lang:en")]
        ])
        sent = await m.answer(tr("start_welcome", "ru"), reply_markup=kb)
        await state.update_data(bot_msg_chat=sent.chat.id, bot_msg_id=sent.message_id)
        await state.set_state(S.reg_wait_lang)
        await safe_delete_message(m)
        return
    if not await user_allowed(m.from_user.id):
        await m.answer(tr("blocked_notice", acc["lang"]))
        await safe_delete_message(m)
        return
    kb = await keyboard_main(m.from_user.id)
    sent = await m.answer(tr("main_menu", acc["lang"]), reply_markup=kb)
    await state.update_data(bot_msg_chat=sent.chat.id, bot_msg_id=sent.message_id)
    await safe_delete_message(m)

async def user_allowed(uid: int) -> bool:
    s = await get_setting("enabled")
    if s == "false" and uid not in ADMIN_IDS:
        return False
    acc = await get_account(uid)
    if acc and acc["blocked"]:
        return False
    return True

@dp.callback_query(lambda c: c.data and c.data.startswith("lang:"))
async def cb_lang(c: types.CallbackQuery, state: FSMContext):
    lang = c.data.split(":",1)[1]
    await ensure_account(c.from_user.id)
    await set_lang(c.from_user.id, lang)
    display = STRINGS[lang]["lang_ru"] if lang == "ru" else STRINGS[lang]["lang_en"]
    try:
        await c.message.edit_text(tr("lang_selected", lang, lang=display))
        await c.message.edit_reply_markup(None)
        await state.update_data(bot_msg_chat=c.message.chat.id, bot_msg_id=c.message.message_id)
    except Exception:
        await c.message.answer(tr("lang_selected", lang, lang=display))
    try:
        await bot.send_message(chat_id=c.message.chat.id, text=tr("ask_channel", lang))
    except Exception:
        pass
    await state.set_state(S.reg_wait_channel)

@dp.callback_query(lambda c: c.data == "change_lang")
async def cb_change_lang(c: types.CallbackQuery):
    acc = await get_account(c.from_user.id)
    lang = acc["lang"] if acc else "ru"
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text=STRINGS["ru"]["lang_ru"], callback_data="lang:ru")],
        [types.InlineKeyboardButton(text=STRINGS["en"]["lang_en"], callback_data="lang:en")]
    ])
    try:
        await c.message.edit_text(tr("choose_lang", lang), reply_markup=kb)
    except Exception:
        await c.message.answer(tr("choose_lang", lang), reply_markup=kb)

@dp.message(S.reg_wait_channel)
async def msg_reg_channel(m: types.Message, state: FSMContext):
    acc = await get_account(m.from_user.id)
    lang = acc["lang"] if acc else "ru"
    cid = None
    title = None
    if m.forward_from_chat and getattr(m.forward_from_chat, "type", None) == "channel":
        cid = str(m.forward_from_chat.id)
        title = m.forward_from_chat.title
    elif m.text and m.text.startswith("@"):
        cid = m.text.strip()
        title = cid
    elif m.text and (m.text.startswith("-") or m.text.isdigit()):
        cid = m.text.strip()
        title = f"Канал {cid}" if lang == "ru" else f"Channel {cid}"
    else:
        await m.answer(tr("ask_channel", lang))
        await safe_delete_message(m)
        return
    ok, code = await add_channel(cid, title, m.from_user.id)
    if ok:
        await m.answer(tr("channel_added", lang, title=title))
    else:
        await m.answer(tr("channel_exists_other", lang))
    kb = await keyboard_main(m.from_user.id)
    sent = await m.answer(tr("main_menu", lang), reply_markup=kb)
    await state.update_data(bot_msg_chat=sent.chat.id, bot_msg_id=sent.message_id)
    await safe_delete_message(m)
    await state.clear()

@dp.callback_query(lambda c: c.data == "add_channel")
async def cb_add_channel(c: types.CallbackQuery, state: FSMContext):
    acc = await get_account(c.from_user.id)
    lang = acc["lang"] if acc else "ru"
    try:
        await c.message.edit_text(tr("ask_channel", lang))
        await state.update_data(bot_msg_chat=c.message.chat.id, bot_msg_id=c.message.message_id)
    except Exception:
        await c.message.answer(tr("ask_channel", lang))
    await state.set_state(S.waiting_for_channel)

@dp.message(S.waiting_for_channel)
async def msg_add_channel(m: types.Message, state: FSMContext):
    acc = await get_account(m.from_user.id)
    lang = acc["lang"] if acc else "ru"
    cid = None
    title = None
    if m.forward_from_chat and getattr(m.forward_from_chat, "type", None) == "channel":
        cid = str(m.forward_from_chat.id)
        title = m.forward_from_chat.title
    elif m.text and m.text.startswith("@"):
        cid = m.text.strip()
        title = cid
    elif m.text and (m.text.startswith("-") or m.text.isdigit()):
        cid = m.text.strip()
        title = f"Канал {cid}" if lang == "ru" else f"Channel {cid}"
    else:
        await m.answer(tr("ask_channel", lang))
        await safe_delete_message(m)
        await state.clear()
        return
    ok, code = await add_channel(cid, title, m.from_user.id)
    if ok:
        await m.answer(tr("channel_added", lang, title=title))
    else:
        await m.answer(tr("channel_exists_other", lang))
    kb = await keyboard_main(m.from_user.id)
    sent = await m.answer(tr("main_menu", lang), reply_markup=kb)
    await state.update_data(bot_msg_chat=sent.chat.id, bot_msg_id=sent.message_id)
    await safe_delete_message(m)
    await state.clear()

@dp.callback_query(lambda c: c.data and c.data.startswith("create_mailing:"))
async def cb_create_mailing(c: types.CallbackQuery, state: FSMContext):
    uid = c.from_user.id
    if not await user_allowed(uid):
        acc = await get_account(uid)
        await c.answer(tr("blocked_notice", acc["lang"] if acc else "ru"), show_alert=True)
        return
    channel_id = c.data.split(":",1)[1]
    ch = await get_channel(channel_id)
    if ch and ch["owner_id"] and ch["owner_id"] != uid:
        acc = await get_account(uid)
        try:
            await c.message.edit_text(tr("not_owner", acc["lang"] if acc else "ru"))
        except Exception:
            await c.message.answer(tr("not_owner", acc["lang"] if acc else "ru"))
        return
    if ch and ch["locked"]:
        acc = await get_account(uid)
        try:
            await c.message.edit_text(tr("channel_locked", acc["lang"] if acc else "ru"))
        except Exception:
            await c.message.answer(tr("channel_locked", acc["lang"] if acc else "ru"))
        return
    await state.update_data(channel_id=channel_id)
    acc = await get_account(uid)
    lang = acc["lang"] if acc else "ru"
    try:
        await c.message.edit_text(tr("send_message_text", lang))
        await state.update_data(bot_msg_chat=c.message.chat.id, bot_msg_id=c.message.message_id)
    except Exception:
        sent = await c.message.answer(tr("send_message_text", lang))
        await state.update_data(bot_msg_chat=sent.chat.id, bot_msg_id=sent.message_id)
    await state.set_state(S.waiting_for_text)

@dp.message(S.waiting_for_text)
async def msg_mailing_text(m: types.Message, state: FSMContext):
    await state.update_data(message_text=m.text)
    acc = await get_account(m.from_user.id)
    lang = acc["lang"] if acc else "ru"
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="➕ " + tr("add_buttons", lang), callback_data="add_buttons")],
        [types.InlineKeyboardButton(text=tr("skip", lang), callback_data="skip_buttons")],
        [types.InlineKeyboardButton(text=tr("back", lang), callback_data="back_to_main")]
    ])
    data = await state.get_data()
    bot_chat = data.get("bot_msg_chat")
    bot_mid = data.get("bot_msg_id")
    text = tr("add_buttons_prompt", lang)
    try:
        if bot_chat and bot_mid:
            chat_id, mid = await try_edit_or_send(bot_chat, bot_mid, text, reply_markup=kb)
        else:
            sent = await m.answer(text, reply_markup=kb)
            chat_id, mid = sent.chat.id, sent.message_id
        await state.update_data(bot_msg_chat=chat_id, bot_msg_id=mid)
    except Exception:
        sent = await m.answer(text, reply_markup=kb)
        await state.update_data(bot_msg_chat=sent.chat.id, bot_msg_id=sent.message_id)
    await safe_delete_message(m)
    await state.set_state(S.waiting_for_buttons)

@dp.callback_query(lambda c: c.data == "add_buttons")
async def cb_add_buttons(c: types.CallbackQuery):
    acc = await get_account(c.from_user.id)
    lang = acc["lang"] if acc else "ru"
    try:
        await c.message.edit_text(tr("add_buttons_prompt", lang))
    except Exception:
        await c.message.answer(tr("add_buttons_prompt", lang))

@dp.message(S.waiting_for_buttons)
async def msg_buttons(m: types.Message, state: FSMContext):
    lines = [l.strip() for l in m.text.splitlines() if l.strip()]
    buttons: List[Dict[str,str]] = []
    invalid: List[str] = []
    for ln in lines:
        if " - " in ln:
            ttxt, url = ln.split(" - ",1)
            if is_valid_url(url.strip()):
                buttons.append({"text": ttxt.strip(), "url": url.strip()})
            else:
                invalid.append(ln)
        else:
            invalid.append(ln)
    await state.update_data(buttons=buttons)
    acc = await get_account(m.from_user.id)
    lang = acc["lang"] if acc else "ru"
    reply = tr("buttons_saved", lang) if buttons else tr("buttons_none", lang)
    if invalid:
        reply += "\n" + tr("invalid_buttons", lang, lines="\n".join(invalid))
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="📌 pin", callback_data="modifier_pin")],
        [types.InlineKeyboardButton(text="🗑 delete", callback_data="modifier_delete")],
        [types.InlineKeyboardButton(text="✏ edit", callback_data="modifier_edit")],
        [types.InlineKeyboardButton(text="🔄 resend", callback_data="modifier_resend")],
        [types.InlineKeyboardButton(text="🔧 update_buttons", callback_data="modifier_update_buttons")],
        [types.InlineKeyboardButton(text="🔁 replace_text", callback_data="modifier_replace_text")],
        [types.InlineKeyboardButton(text="➡️ preview", callback_data="preview_mailing")],
        [types.InlineKeyboardButton(text=tr("back", lang), callback_data="back_to_main")]
    ])
    data = await state.get_data()
    bot_chat = data.get("bot_msg_chat")
    bot_mid = data.get("bot_msg_id")
    try:
        if bot_chat and bot_mid:
            chat_id, mid = await try_edit_or_send(bot_chat, bot_mid, reply, reply_markup=kb)
        else:
            sent = await m.answer(reply, reply_markup=kb)
            chat_id, mid = sent.chat.id, sent.message_id
        await state.update_data(bot_msg_chat=chat_id, bot_msg_id=mid)
    except Exception:
        sent = await m.answer(reply, reply_markup=kb)
        await state.update_data(bot_msg_chat=sent.chat.id, bot_msg_id=sent.message_id)
    await safe_delete_message(m)
    await state.set_state(S.waiting_for_modifiers)

@dp.callback_query(lambda c: c.data and c.data.startswith("modifier_"))
async def cb_modifier(c: types.CallbackQuery, state: FSMContext):
    mod = c.data.replace("modifier_", "")
    await state.update_data(current_modifier=mod)
    acc = await get_account(c.from_user.id)
    lang = acc["lang"] if acc else "ru"
    if mod == "replace_text":
        try:
            await c.message.edit_text(tr("enter_new_text", lang))
            await state.update_data(bot_msg_chat=c.message.chat.id, bot_msg_id=c.message.message_id)
        except Exception:
            await c.message.answer(tr("enter_new_text", lang))
        await state.set_state(S.waiting_for_time)
        return
    try:
        await c.message.edit_text(tr("enter_minutes", lang))
        await state.update_data(bot_msg_chat=c.message.chat.id, bot_msg_id=c.message.message_id)
    except Exception:
        await c.message.answer(tr("enter_minutes", lang))
    await state.set_state(S.waiting_for_time)

@dp.message(S.waiting_for_time)
async def msg_modifier_time(m: types.Message, state: FSMContext):
    data = await state.get_data()
    mod = data.get("current_modifier")
    acc = await get_account(m.from_user.id)
    lang = acc["lang"] if acc else "ru"
    if not mod:
        await m.answer(tr("error", lang))
        await safe_delete_message(m)
        await state.clear()
        return
    if mod == "replace_text":
        mods = data.get("modifiers", {})
        mods["replace_text_new"] = m.text
        await state.update_data(modifiers=mods)
        await m.answer(tr("mod_added", lang, m=mod, minutes="(text set)"))
        await safe_delete_message(m)
        await state.set_state(S.waiting_for_modifiers)
        return
    try:
        minutes = int(m.text.strip())
        if minutes < 0:
            raise ValueError()
    except Exception:
        await m.answer(tr("enter_minutes", lang))
        await safe_delete_message(m)
        return
    mods = data.get("modifiers", {})
    mods[mod] = minutes
    await state.update_data(modifiers=mods)
    await m.answer(tr("mod_added", lang, m=mod, minutes=str(minutes)+" min"))
    await safe_delete_message(m)
    await state.set_state(S.waiting_for_modifiers)

@dp.callback_query(lambda c: c.data == "preview_mailing")
async def cb_preview(c: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    text = data.get("message_text", "")
    buttons = data.get("buttons", [])
    modifiers = data.get("modifiers", {})
    acc = await get_account(c.from_user.id)
    lang = acc["lang"] if acc else "ru"
    kb_buttons = [[types.InlineKeyboardButton(text=b["text"], url=b["url"])] for b in buttons if is_valid_url(b.get("url"))]
    kb = types.InlineKeyboardMarkup(inline_keyboard=[*kb_buttons, [types.InlineKeyboardButton(text="✅ " + tr("published", lang), callback_data="publish_mailing")], [types.InlineKeyboardButton(text="❌ " + tr("cancel", lang), callback_data="cancel_creation")], [types.InlineKeyboardButton(text=tr("back", lang), callback_data="back_to_main")]])
    txt = tr("preview_header", lang) + "\n\n" + (text or "") + "\n\n" + "Modifiers:\n"
    for k,v in (modifiers or {}).items():
        txt += f"- {k}: {v}\n"
    try:
        await c.message.edit_text(txt, reply_markup=kb)
        await state.update_data(bot_msg_chat=c.message.chat.id, bot_msg_id=c.message.message_id)
    except Exception:
        sent = await c.message.answer(txt, reply_markup=kb)
        await state.update_data(bot_msg_chat=sent.chat.id, bot_msg_id=sent.message_id)
    await state.set_state(S.preview)

@dp.callback_query(lambda c: c.data == "publish_mailing")
async def cb_publish(c: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    channel_id = data.get("channel_id")
    acc = await get_account(c.from_user.id)
    lang = acc["lang"] if acc else "ru"
    if not channel_id:
        await c.message.answer(tr("no_channels", lang))
        await state.clear()
        return
    ch = await get_channel(channel_id)
    if ch and ch["owner_id"] and ch["owner_id"] != c.from_user.id:
        await c.message.answer(tr("not_owner", lang))
        await state.clear()
        return
    if ch and ch["locked"]:
        await c.message.answer(tr("channel_locked", lang))
        await state.clear()
        return
    message_text = data.get("message_text", "")
    buttons = data.get("buttons", [])
    modifiers = data.get("modifiers", {})
    valid_buttons = []
    invalid = []
    for b in buttons:
        if is_valid_url(b.get("url")):
            valid_buttons.append(b)
        else:
            invalid.append(b)
    kb = None
    if valid_buttons:
        kb = types.InlineKeyboardMarkup(inline_keyboard=[[types.InlineKeyboardButton(text=b["text"], url=b["url"])] for b in valid_buttons])
    try:
        cnt = await incr_mailing_count()
        if cnt % 2 == 0:
            message_text = message_text + tr("watermark", lang)
        try:
            sent = await bot.send_message(chat_id=channel_id, text=message_text, reply_markup=kb)
        except Forbidden:
            await c.message.answer(tr("forbidden", lang))
            await state.clear()
            return
        except TelegramBadRequest as e:
            msg = str(e).lower()
            if "chat not found" in msg or "chat_not_found" in msg or "user not found" in msg:
                await c.message.answer(tr("chat_not_found", lang))
                await state.clear()
                return
            # fallback: попробуем без клавиатуры
            try:
                sent = await bot.send_message(chat_id=channel_id, text=message_text)
            except Exception as e2:
                await c.message.answer(tr("publish_error", lang, err=str(e2)))
                await state.clear()
                return
        mailing_id = await add_mailing(channel_id, message_text, valid_buttons, modifiers, sent.message_id)
        if mailing_id:
            now = datetime.utcnow()
            for mtype, minutes in (modifiers or {}).items():
                try:
                    minutes_int = int(minutes)
                except Exception:
                    minutes_int = 0
                if mtype == "pin":
                    try:
                        await bot.pin_chat_message(chat_id=channel_id, message_id=sent.message_id)
                    except Exception:
                        logger.exception("pin")
                    if minutes_int > 0:
                        et = now + timedelta(minutes=minutes_int)
                        tid = await add_task(mailing_id, "unpin", et)
                        asyncio.create_task(run_scheduled(tid, mailing_id, "unpin", (et-now).total_seconds()))
                else:
                    et = now + timedelta(minutes=minutes_int)
                    tid = await add_task(mailing_id, mtype, et)
                    asyncio.create_task(run_scheduled(tid, mailing_id, mtype, (et-now).total_seconds()))
        msg = tr("published", lang)
        if invalid:
            msg += "\n⚠️ " + tr("invalid_buttons", lang, lines="\n".join([str(x) for x in invalid]))
        bot_chat = data.get("bot_msg_chat")
        bot_mid = data.get("bot_msg_id")
        try:
            if bot_chat and bot_mid:
                await try_edit_or_send(bot_chat, bot_mid, msg)
            else:
                await c.message.answer(msg)
        except Exception:
            await c.message.answer(msg)
    except Exception as e:
        logger.exception("publish")
        await c.message.answer(tr("publish_error", lang, err=str(e)))
    await state.clear()

@dp.callback_query(lambda c: c.data == "cancel_creation")
async def cb_cancel(c: types.CallbackQuery, state: FSMContext):
    acc = await get_account(c.from_user.id)
    lang = acc["lang"] if acc else "ru"
    kb = await keyboard_main(c.from_user.id)
    try:
        await c.message.edit_text(tr("main_menu", lang), reply_markup=kb)
        await state.update_data(bot_msg_chat=c.message.chat.id, bot_msg_id=c.message.message_id)
    except Exception:
        sent = await c.message.answer(tr("main_menu", lang), reply_markup=kb)
        await state.update_data(bot_msg_chat=sent.chat.id, bot_msg_id=sent.message_id)
    await state.clear()

@dp.callback_query(lambda c: c.data == "manage_mailings")
async def cb_manage(c: types.CallbackQuery):
    acc = await get_account(c.from_user.id)
    lang = acc["lang"] if acc else "ru"
    rows = await db_pool.fetch("SELECT m.id, m.channel_id, m.message_text, c.channel_title FROM mailings m LEFT JOIN channels c ON m.channel_id=c.channel_id ORDER BY m.created_date DESC LIMIT 40")
    if not rows:
        try:
            await c.message.edit_text(tr("no_mailings", lang))
        except Exception:
            await c.message.answer(tr("no_mailings", lang))
        return
    kb = []
    for r in rows:
        preview = (r["message_text"][:50]+"...") if r["message_text"] and len(r["message_text"])>50 else (r["message_text"] or "")
        title = r["channel_title"] or r["channel_id"]
        kb.append([types.InlineKeyboardButton(text=f"{title}: {preview}", callback_data=f"edit_mailing:{r['id']}")])
    kb.append([types.InlineKeyboardButton(text=tr("back", lang), callback_data="back_to_main")])
    try:
        await c.message.edit_text(tr("manage_mailings", lang), reply_markup=types.InlineKeyboardMarkup(inline_keyboard=kb))
    except Exception:
        await c.message.answer(tr("manage_mailings", lang), reply_markup=types.InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(lambda c: c.data and c.data.startswith("edit_mailing:"))
async def cb_edit_mailing(c: types.CallbackQuery):
    mid = int(c.data.split(":",1)[1])
    acc = await get_account(c.from_user.id)
    lang = acc["lang"] if acc else "ru"
    m = await get_mailing(mid)
    if not m:
        await c.message.answer(tr("no_mailings", lang))
        return
    txt = f"ID: {m['id']}\nChannel: {m['channel_id']}\nText:\n" + (m['message_text'][:400]+"..." if m['message_text'] and len(m['message_text'])>400 else (m['message_text'] or ""))
    kb = [
        [types.InlineKeyboardButton(text="🗑 " + tr("mailing_deleted", lang), callback_data=f"delete_mailing:{m['id']}")],
        [types.InlineKeyboardButton(text="▶ " + tr("force_started", lang), callback_data=f"force_task:{m['id']}")],
        [types.InlineKeyboardButton(text=tr("back", lang), callback_data="manage_mailings")]
    ]
    try:
        await c.message.edit_text(txt, reply_markup=types.InlineKeyboardMarkup(inline_keyboard=kb))
    except Exception:
        await c.message.answer(txt, reply_markup=types.InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(lambda c: c.data and c.data.startswith("delete_mailing:"))
async def cb_delete_mailing(c: types.CallbackQuery):
    mid = int(c.data.split(":",1)[1])
    m = await get_mailing(mid)
    if m and m["message_id"]:
        try:
            await bot.delete_message(chat_id=m["channel_id"], message_id=m["message_id"])
        except Exception:
            pass
    await delete_mailing(mid)
    acc = await get_account(c.from_user.id)
    lang = acc["lang"] if acc else "ru"
    try:
        await c.message.edit_text(tr("mailing_deleted", lang))
    except Exception:
        await c.message.answer(tr("mailing_deleted", lang))

@dp.callback_query(lambda c: c.data and c.data.startswith("force_task:"))
async def cb_force(c: types.CallbackQuery):
    mid = int(c.data.split(":",1)[1])
    m = await get_mailing(mid)
    if not m:
        try:
            await c.message.edit_text(tr("not_found", "ru"))
        except Exception:
            await c.message.answer(tr("not_found", "ru"))
        return
    mods = json.loads(m["modifiers"]) if m["modifiers"] else {}
    if not mods:
        try:
            await c.message.edit_text(tr("no_modifiers", "ru"))
        except Exception:
            await c.message.answer(tr("no_modifiers", "ru"))
        return
    for mod in mods.keys():
        asyncio.create_task(apply_modifier(mid, mod))
    acc = await get_account(c.from_user.id)
    lang = acc["lang"] if acc else "ru"
    try:
        await c.message.edit_text(tr("force_started", lang))
    except Exception:
        await c.message.answer(tr("force_started", lang))

@dp.callback_query(lambda c: c.data == "back_to_main")
async def cb_back(c: types.CallbackQuery):
    acc = await get_account(c.from_user.id)
    lang = acc["lang"] if acc else "ru"
    kb = await keyboard_main(c.from_user.id)
    try:
        await c.message.edit_text(tr("main_menu", lang), reply_markup=kb)
    except Exception:
        await c.message.answer(tr("main_menu", lang), reply_markup=kb)

# ----------------- Admin panel -----------------
@dp.callback_query(lambda c: c.data == "admin_panel")
async def cb_admin(c: types.CallbackQuery):
    if c.from_user.id not in ADMIN_IDS:
        await c.message.answer("Access denied")
        return
    acc = await get_account(c.from_user.id)
    lang = acc["lang"] if acc else "ru"
    kb = [
        [types.InlineKeyboardButton(text="Wipe ALL", callback_data="admin_wipe")],
        [types.InlineKeyboardButton(text="Delete account", callback_data="admin_delete_account")],
        [types.InlineKeyboardButton(text="Toggle maintenance", callback_data="admin_toggle")],
        [types.InlineKeyboardButton(text="Channels (HTML)", callback_data="admin_channels_html")],
        [types.InlineKeyboardButton(text="Channels (CSV)", callback_data="admin_channels_csv")],
        [types.InlineKeyboardButton(text="Send to channel", callback_data="admin_send_channel")],
        [types.InlineKeyboardButton(text="Send to ALL channels", callback_data="admin_send_all")],
        [types.InlineKeyboardButton(text="View tasks", callback_data="admin_tasks")],
        [types.InlineKeyboardButton(text="Block/Unblock user", callback_data="admin_block_user")],
        [types.InlineKeyboardButton(text="Transfer channel", callback_data="admin_transfer")],
        [types.InlineKeyboardButton(text="Mass send", callback_data="admin_mass_send")],
        [types.InlineKeyboardButton(text=tr("back", lang), callback_data="back_to_main")]
    ]
    try:
        await c.message.edit_text(tr("admin_panel", lang), reply_markup=types.InlineKeyboardMarkup(inline_keyboard=kb))
    except Exception:
        await c.message.answer(tr("admin_panel", lang), reply_markup=types.InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(lambda c: c.data == "admin_wipe")
async def cb_wipe(c: types.CallbackQuery):
    if c.from_user.id not in ADMIN_IDS:
        return
    async with db_pool.acquire() as conn:
        await conn.execute("TRUNCATE TABLE scheduled_tasks CASCADE")
        await conn.execute("TRUNCATE TABLE mailings CASCADE")
        await conn.execute("TRUNCATE TABLE channels CASCADE")
        await conn.execute("TRUNCATE TABLE accounts CASCADE")
        await conn.execute("INSERT INTO settings(key,value) VALUES('enabled','true') ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value")
        await conn.execute("INSERT INTO settings(key,value) VALUES('mailing_count','0') ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value")
    acc = await get_account(c.from_user.id)
    lang = acc["lang"] if acc else "ru"
    try:
        await c.message.edit_text(tr("wipe_done", lang))
    except Exception:
        await c.message.answer(tr("wipe_done", lang))

@dp.callback_query(lambda c: c.data == "admin_delete_account")
async def cb_admin_delete(c: types.CallbackQuery, state: FSMContext):
    if c.from_user.id not in ADMIN_IDS:
        return
    await state.set_state(S.admin_wait_delete)
    try:
        await c.message.edit_text(tr("send_user_id", "ru"))
    except Exception:
        await c.message.answer(tr("send_user_id", "ru"))

@dp.message(S.admin_wait_delete)
async def msg_admin_delete(m: types.Message, state: FSMContext):
    if m.from_user.id not in ADMIN_IDS:
        await state.clear()
        return
    try:
        uid = int(m.text.strip())
    except Exception:
        await m.answer(tr("invalid_user_id", "ru"))
        await state.clear()
        return
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM accounts WHERE user_id=$1", uid)
        await conn.execute("DELETE FROM channels WHERE owner_id=$1", uid)
    await m.answer(tr("deleted", "ru"))
    await state.clear()

@dp.callback_query(lambda c: c.data == "admin_toggle")
async def cb_admin_toggle(c: types.CallbackQuery):
    if c.from_user.id not in ADMIN_IDS:
        return
    cur = await get_setting("enabled")
    new = "false" if cur == "true" else "true"
    await set_setting("enabled", new)
    acc = await get_account(c.from_user.id)
    lang = acc["lang"] if acc else "ru"
    try:
        await c.message.edit_text(tr("maintenance_on", lang, state=new))
    except Exception:
        await c.message.answer(tr("maintenance_on", lang, state=new))

@dp.callback_query(lambda c: c.data == "admin_channels_html")
async def cb_channels_html(c: types.CallbackQuery):
    if c.from_user.id not in ADMIN_IDS:
        return
    rows = await get_all_channels()
    html = "<html><body><h1>Channels</h1><ul>"
    for r in rows:
        title = r["channel_title"] or r["channel_id"]
        html += f"<li>{title} ({r['channel_id']}) owner={r['owner_id']} locked={r['locked']}</li>"
    html += "</ul></body></html>"
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".html")
    try:
        tmp.write(html.encode("utf-8"))
        tmp.flush()
        tmp.close()
        with open(tmp.name, "rb") as fh:
            try:
                await c.message.answer_document(document=fh)
            except Exception as e:
                await c.message.answer("Error sending file: " + str(e))
    finally:
        try:
            os.unlink(tmp.name)
        except Exception:
            pass

@dp.callback_query(lambda c: c.data == "admin_channels_csv")
async def cb_channels_csv(c: types.CallbackQuery):
    if c.from_user.id not in ADMIN_IDS:
        return
    rows = await get_all_channels()
    csv = "channel_id,channel_title,owner_id,locked\n"
    for r in rows:
        title = (r["channel_title"] or "").replace(",", " ")
        csv += f'{r["channel_id"]},"{title}",{r["owner_id"]},{r["locked"]}\n'
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    try:
        tmp.write(csv.encode("utf-8"))
        tmp.flush()
        tmp.close()
        with open(tmp.name, "rb") as fh:
            try:
                await c.message.answer_document(document=fh)
            except Exception as e:
                await c.message.answer("Error sending file: " + str(e))
    finally:
        try:
            os.unlink(tmp.name)
        except Exception:
            pass

@dp.callback_query(lambda c: c.data == "admin_send_channel")
async def cb_admin_send_channel(c: types.CallbackQuery, state: FSMContext):
    if c.from_user.id not in ADMIN_IDS:
        return
    await state.set_state(S.admin_wait_msg_channel)
    acc = await get_account(c.from_user.id)
    lang = acc["lang"] if acc else "ru"
    try:
        await c.message.edit_text(tr("ask_channel", lang))
    except Exception:
        await c.message.answer(tr("ask_channel", lang))

@dp.message(S.admin_wait_msg_channel)
async def msg_admin_target(m: types.Message, state: FSMContext):
    if m.from_user.id not in ADMIN_IDS:
        await state.clear()
        return
    target = normalize_chat_input(m.text.strip())
    await state.update_data(target=target)
    acc = await get_account(m.from_user.id)
    lang = acc["lang"] if acc else "ru"
    await m.answer(tr("send_message_text", lang))
    await state.set_state(S.admin_wait_msg_text)

@dp.message(S.admin_wait_msg_text)
async def msg_admin_send_text(m: types.Message, state: FSMContext):
    if m.from_user.id not in ADMIN_IDS:
        await state.clear()
        return
    data = await state.get_data()
    target = data.get("target")
    try:
        await bot.send_message(chat_id=target, text=m.text)
        await m.answer(tr("admin_send_ok", "ru", target=target))
    except Forbidden:
        await m.answer(tr("forbidden", "ru"))
    except TelegramBadRequest as e:
        msg = str(e).lower()
        if "chat not found" in msg or "chat_not_found" in msg or "user not found" in msg:
            await m.answer(tr("chat_not_found", "ru"))
        else:
            await m.answer(tr("admin_send_err", "ru", err=str(e)))
    except Exception as e:
        await m.answer(tr("admin_send_err", "ru", err=str(e)))
    await state.clear()

@dp.callback_query(lambda c: c.data == "admin_send_all")
async def cb_admin_send_all(c: types.CallbackQuery, state: FSMContext):
    if c.from_user.id not in ADMIN_IDS:
        return
    await state.set_state(S.admin_wait_mass_text)
    acc = await get_account(c.from_user.id)
    lang = acc["lang"] if acc else "ru"
    try:
        await c.message.edit_text(tr("send_message_text", lang))
    except Exception:
        await c.message.answer(tr("send_message_text", lang))

@dp.message(S.admin_wait_mass_text)
async def msg_admin_mass_text(m: types.Message, state: FSMContext):
    if m.from_user.id not in ADMIN_IDS:
        await state.clear()
        return
    rows = await get_all_channels()
    results = []
    for r in rows:
        try:
            await bot.send_message(chat_id=r["channel_id"], text=m.text)
            results.append(f"{r['channel_id']}: OK")
        except Exception as e:
            results.append(f"{r['channel_id']}: ERROR ({e})")
    await m.answer(tr("mass_send_results", "ru", res="\n".join(results)))
    await state.clear()

@dp.callback_query(lambda c: c.data == "admin_tasks")
async def cb_admin_tasks(c: types.CallbackQuery):
    if c.from_user.id not in ADMIN_IDS:
        return
    rows = await get_pending_tasks()
    if not rows:
        await c.message.answer(tr("no_tasks", "ru"))
        return
    txt = ""
    for r in rows:
        txt += f"task_id={r['id']} mailing_id={r['mailing_id']} type={r['modifier_type']} execute={r['execute_time']}\n"
    await c.message.answer(tr("tasks_list", "ru", list=txt))

@dp.callback_query(lambda c: c.data == "admin_block_user")
async def cb_admin_block_user(c: types.CallbackQuery, state: FSMContext):
    if c.from_user.id not in ADMIN_IDS:
        return
    await state.set_state(S.admin_wait_user_block)
    try:
        await c.message.edit_text(tr("send_user_id_toggle_block", "ru"))
    except Exception:
        await c.message.answer(tr("send_user_id_toggle_block", "ru"))

@dp.message(S.admin_wait_user_block)
async def msg_admin_block_user(m: types.Message, state: FSMContext):
    if m.from_user.id not in ADMIN_IDS:
        await state.clear()
        return
    try:
        uid = int(m.text.strip())
    except Exception:
        await m.answer(tr("invalid_user_id", "ru"))
        await state.clear()
        return
    acc = await get_account(uid)
    if not acc:
        await ensure_account(uid)
        acc = await get_account(uid)
    if acc["blocked"]:
        await set_block(uid, False)
        await m.answer(tr("user_unblocked", "ru", uid=uid))
    else:
        await set_block(uid, True)
        await m.answer(tr("user_blocked", "ru", uid=uid))
    await state.clear()

@dp.callback_query(lambda c: c.data == "admin_transfer")
async def cb_admin_transfer(c: types.CallbackQuery, state: FSMContext):
    if c.from_user.id not in ADMIN_IDS:
        return
    await state.set_state(S.admin_wait_transfer)
    try:
        await c.message.edit_text(tr("send_channel_id_new_owner", "ru"))
    except Exception:
        await c.message.answer(tr("send_channel_id_new_owner", "ru"))

@dp.message(S.admin_wait_transfer)
async def msg_admin_transfer(m: types.Message, state: FSMContext):
    if m.from_user.id not in ADMIN_IDS:
        await state.clear()
        return
    try:
        ch, new = m.text.strip().split()
        nid = int(new)
    except Exception:
        await m.answer(tr("error", "ru"))
        await state.clear()
        return
    chrec = await get_channel(ch)
    if not chrec:
        await m.answer(tr("not_found", "ru"))
        await state.clear()
        return
    await set_channel_owner(ch, nid)
    await m.answer(tr("owner_changed", "ru", nid=nid))
    await state.clear()

# ----------------- Periodic cleanup/startup -----------------
async def periodic_cleanup_task():
    while True:
        try:
            await cleanup_db()
        except Exception:
            logger.exception("cleanup")
        await asyncio.sleep(24*3600)

async def on_startup():
    await init_db()
    await schedule_pending_on_startup()
    asyncio.create_task(periodic_cleanup_task())

# ----------------- Run -----------------
async def main():
    await on_startup()
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("shutdown")
