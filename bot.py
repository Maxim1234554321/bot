#!/usr/bin/env python3

from colorama import Fore, Style, init as colorama_init
import asyncio
import aiohttp
import json
import os
import time
import logging
import re
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
import aiofiles
from bs4 import BeautifulSoup
from aiogram import Bot, Dispatcher, Router, types
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, FSInputFile
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from fpdf import FPDF
import aiofiles.os
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

colorama_init(autoreset=True)

# Вывод информации о боте
print(Fore.GREEN + "🍎 Бот для отслеживания стоков Blox Fruits 🍎")
print(Fore.YELLOW + "Автор: @aw3keN")
print(Fore.CYAN + "Версия: 2.6 (с исправленным InputFile)")
print(Fore.MAGENTA + "Что нового:")
print(Fore.CYAN + " • Исправлена ошибка с InputFile (замена на FSInputFile)")
print(Fore.CYAN + " • Сохранена поддержка PDF с DejaVuSans и скачивание логов")
print(Fore.CYAN + " • Сохранена информация о боте в главном меню")
print(Fore.CYAN + " • Добавлен водяной знак на стоки (fruityblox.com)")
print(Fore.GREEN + "\nБот успешно запущен!\n")

# 📜 Конфигурация
class Settings:
    TOKEN = os.getenv("BOT_TOKEN", "8353992511:AAGLTI6dhxis23prl4AzLVh4Atau2jDbh_0")
    STOCK_PATH = "data/stock.json"
    USERS_PATH = "data/users.json"
    ADMINS_PATH = "data/admins.json"
    BACKUP_DIR = "data/backups"
    FONT_PATH = "data/DejaVuSans.ttf"
    URL = "https://fruityblox.com/stock"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/"
    }
    NORMAL_EMOJI = "🍎"
    MIRAGE_EMOJI = "🌌"
    OWNER_ID = 5890065908

# 📋 Настройка логирования
logger = logging.getLogger("BloxFruitsBot")
logger.setLevel(logging.DEBUG)
handler = RotatingFileHandler("bot.log", maxBytes=10*1024*1024, backupCount=5, encoding="utf-8")
formatter = logging.Formatter('%(asctime)s - 🍎 %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# 🤖 Инициализация бота
try:
    bot = Bot(token=Settings.TOKEN)
except Exception as e:
    logger.error(f"❌ Неверный токен бота: {e}")
    raise SystemExit("Bot initialization failed")

dp = Dispatcher()
user_router = Router()
bot_stats = {"messages_processed": 0, "callbacks_processed": 0, "last_latency": 0.0}
current_stock = {}
file_lock = asyncio.Lock()

# Состояния для админ-команд
class AdminStates(StatesGroup):
    waiting_for_broadcast = State()
    waiting_for_role = State()
    waiting_for_remove_role = State()

# Инициализация данных
async def init_data_files():
    os.makedirs("data", exist_ok=True)
    if not await aiofiles.os.path.exists(Settings.USERS_PATH):
        async with aiofiles.open(Settings.USERS_PATH, "w", encoding="utf-8") as f:
            await f.write(json.dumps({}, ensure_ascii=False, indent=2))
    if not await aiofiles.os.path.exists(Settings.ADMINS_PATH):
        admins = {str(Settings.OWNER_ID): "owner"}
        async with aiofiles.open(Settings.ADMINS_PATH, "w", encoding="utf-8") as f:
            await f.write(json.dumps(admins, ensure_ascii=False, indent=2))
    if not await aiofiles.os.path.exists(Settings.STOCK_PATH):
        async with aiofiles.open(Settings.STOCK_PATH, "w", encoding="utf-8") as f:
            await f.write(json.dumps({"normal": [], "mirage": []}, ensure_ascii=False, indent=2))

async def load_json(path, default=None):
    async with file_lock:
        if await aiofiles.os.path.exists(path):
            try:
                async with aiofiles.open(path, "r", encoding="utf-8") as f:
                    content = await f.read()
                    return json.loads(content) if content.strip() else default
            except json.JSONDecodeError as e:
                logger.error(f"❌ Ошибка парсинга {path}: {e}")
                return default
        return default

async def save_json(path, data):
    async with file_lock:
        try:
            async with aiofiles.open(path, "w", encoding="utf-8") as f:
                await f.write(json.dumps(data, ensure_ascii=False, indent=2))
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения {path}: {e}")

async def update_user_stats(user: types.User):
    users = await load_json(Settings.USERS_PATH, {})
    user_id = str(user.id)
    now = datetime.now().isoformat()
    if user_id not in users:
        users[user_id] = {
            "username": user.username,
            "first_name": user.first_name,
            "join_date": now,
            "last_active": now,
            "message_count": 1
        }
    else:
        users[user_id]["last_active"] = now
        users[user_id]["message_count"] += 1
    await save_json(Settings.USERS_PATH, users)

async def get_user_role(user_id: int) -> str:
    admins = await load_json(Settings.ADMINS_PATH, {})
    return admins.get(str(user_id), None)

ROLE_PERMISSIONS = {
    "owner": ["all"],
    "admin": ["broadcast", "pdf", "ban", "view_users", "view_admins", "manual_update", "view_logs", "download_logs"],
    "moderator": ["view_users", "view_admins", "manual_update", "view_logs"]
}

def has_permission(role: str, permission: str) -> bool:
    if role == "owner":
        return True
    return permission in ROLE_PERMISSIONS.get(role, [])

class StockFileHandler(FileSystemEventHandler):
    def __init__(self, bot):
        super().__init__()
        self.bot = bot
        self.last_modified = time.time()
        self.last_stock_data = {}
        self.is_updating = False
        self.update_interval = 30
        self.request_limit = 10
        self.last_request_time = 0
        self.request_count = 0
        logger.info("ℹ️ Начинаю отслеживание изменений и обновление стока")
        asyncio.create_task(self.periodic_update_stock())

    async def periodic_update_stock(self):
        while True:
            try:
                start_time = time.time()
                await self.update_stock()
                bot_stats["last_latency"] = time.time() - start_time
                await auto_backup()
                logger.info(f"✅ Сток успешно обновлен в {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}. Следующее обновление через {self.update_interval} секунд")
            except Exception as e:
                logger.error(f"❌ Ошибка при обновлении стока: {e}")
            await asyncio.sleep(self.update_interval)

    async def fetch_stock_data(self):
        current_time = time.time()
        if current_time - self.last_request_time >= 60:
            self.request_count = 0
            self.last_request_time = current_time
        if self.request_count >= self.request_limit:
            logger.warning("⚠️ Достигнут лимит запросов. Ожидание...")
            await asyncio.sleep(60 - (current_time - self.last_request_time))
            self.request_count = 0
            self.last_request_time = time.time()
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(Settings.URL, headers=Settings.HEADERS, timeout=10) as response:
                    response.raise_for_status()
                    self.request_count += 1
                    logger.info("✅ Успешный запрос к сайту")
                    text = await response.text()
                    if not text.strip():
                        logger.error("❌ Получен пустой ответ от сайта")
                        return None
                    return text
            except aiohttp.ClientError as e:
                logger.error(f"❌ Ошибка при запросе к сайту {Settings.URL}: {e}")
                return None
            except Exception as e:
                logger.error(f"❌ Неизвестная ошибка в fetch: {e}")
                return None

    async def parse_stock_data(self, html_content):
        if not html_content:
            logger.error("❌ HTML-контент пустой")
            return {"normal": [], "mirage": []}
        
        stocks = {"normal": [], "mirage": []}
        unique_items = set()
        
        try:
            soup = BeautifulSoup(html_content, "lxml")
            stock_grids = soup.select("div.grid")
            if stock_grids:
                for grid in stock_grids:
                    items = grid.select("div.border.rounded-lg")
                    for item in items:
                        name_tag = item.find("h3")
                        type_tag = item.find("span", string=re.compile("normal|mirage", re.I))
                        
                        if name_tag:
                            name = name_tag.get_text(strip=True).lower()
                            type_text = type_tag.get_text(strip=True).lower() if type_tag else ""
                            stock_type = "normal" if "normal" in type_text else "mirage" if "mirage" in type_text else None
                            if stock_type and name not in unique_items:
                                stocks[stock_type].append(name)
                                unique_items.add(name)
                                logger.info(f"✅ HTML: Извлечен фрукт: {name}, Тип: {stock_type}")
            script_tags = soup.select("script")
            for script in script_tags:
                script_content = script.get_text()
                if "currentStock" in script_content:
                    try:
                        match = re.search(r'currentStock":\s*(\{.*?\})\s*}', script_content, re.DOTALL)
                        if match:
                            json_str = match.group(1)
                            json_str = json_str.replace("'", '"')
                            stock_data = json.loads(json_str)
                            for stock_type in ["normal", "mirage"]:
                                for name in stock_data.get(stock_type, []):
                                    name = name.lower()
                                    if name not in unique_items:
                                        stocks[stock_type].append(name)
                                        unique_items.add(name)
                                        logger.info(f"✅ __next_f: Извлечен фрукт: {name}, Тип: {stock_type}")
                    except Exception as e:
                        logger.error(f"❌ Ошибка обработки __next_f: {e}")
                    break
            for stock_type in ["normal", "mirage"]:
                if not stocks[stock_type]:
                    logger.info(f"ℹ️ {stock_type.capitalize()} сток пуст")
                    stocks[stock_type] = []
            await save_json(Settings.STOCK_PATH, stocks)
            return stocks
        except Exception as e:
            logger.error(f"❌ Ошибка парсинга данных: {e}")
            return {"normal": [], "mirage": []}

    async def update_stock(self):
        if self.is_updating:
            logger.info("ℹ️ Обновление стока уже выполняется, пропуск...")
            return
        self.is_updating = True
        try:
            html_content = await self.fetch_stock_data()
            if html_content:
                new_stock_data = await self.parse_stock_data(html_content)
                if new_stock_data:
                    self.last_stock_data = new_stock_data
                else:
                    logger.warning("⚠️ Не получены новые данные стока")
            else:
                logger.warning("⚠️ Не получен HTML-контент")
        except Exception as e:
            logger.error(f"❌ Ошибка обновления стока: {e}")
        finally:
            self.is_updating = False

    def on_modified(self, event):
        if event.src_path == Settings.STOCK_PATH and not self.is_updating:
            current_time = time.time()
            if current_time - self.last_modified >= 1:
                self.last_modified = current_time
                asyncio.create_task(self.update_stock())

async def auto_backup():
    try:
        os.makedirs(Settings.BACKUP_DIR, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = os.path.join(Settings.BACKUP_DIR, f"stock_{timestamp}.bak")
        if await aiofiles.os.path.exists(Settings.STOCK_PATH):
            async with aiofiles.open(Settings.STOCK_PATH, "r", encoding="utf-8") as src:
                content = await src.read()
            async with aiofiles.open(backup_path, "w", encoding="utf-8") as dst:
                await dst.write(content)
            logger.info(f"✅ Резервная копия создана: {backup_path}")
    except Exception as e:
        logger.error(f"❌ Ошибка автоматического резервного копирования: {e}")

def get_main_keyboard() -> ReplyKeyboardMarkup:
    keyboard = [
        [KeyboardButton(text=f"{Settings.NORMAL_EMOJI} Обычный сток"), 
         KeyboardButton(text=f"{Settings.MIRAGE_EMOJI} Миражный сток")],
        [KeyboardButton(text="📞 Контакты"), 
         KeyboardButton(text="ℹ️ О боте")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True, one_time_keyboard=True)

def get_stock_keyboard() -> ReplyKeyboardMarkup:
    keyboard = [
        [KeyboardButton(text=f"{Settings.NORMAL_EMOJI} Обычный сток"), 
         KeyboardButton(text=f"{Settings.MIRAGE_EMOJI} Миражный сток")],
        [KeyboardButton(text="📞 Контакты"), 
         KeyboardButton(text="ℹ️ О боте")],
        [KeyboardButton(text="⬅ Назад")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True, one_time_keyboard=True)

def get_admin_keyboard() -> ReplyKeyboardMarkup:
    keyboard = [
        [KeyboardButton(text="📢 Рассылка объявления")],
        [KeyboardButton(text="📊 Получить PDF статистики")],
        [KeyboardButton(text="👑 Выдать роль по ID")],
        [KeyboardButton(text="🚫 Убрать роль/бан")],
        [KeyboardButton(text="📋 Список админов")],
        [KeyboardButton(text="👥 Статистика пользователей")],
        [KeyboardButton(text="🔄 Ручное обновление стока")],
        [KeyboardButton(text="📜 Просмотр логов"), 
         KeyboardButton(text="📥 Скачать логи")],
        [KeyboardButton(text="⬅ Назад")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True, one_time_keyboard=True)

async def get_stock(stock_type: str) -> str:
    try:
        stock_data = await load_json(Settings.STOCK_PATH, {"normal": [], "mirage": []})
        items = stock_data.get(stock_type, [])
        emoji = Settings.NORMAL_EMOJI if stock_type == "normal" else Settings.MIRAGE_EMOJI
        text = f"{emoji} <b>{'Обычный сток' if stock_type == 'normal' else 'Миражный сток'}</b>\n"
        if items:
            for item in items:
                if isinstance(item, str):
                    text += f"   • {item.capitalize()}\n"
                else:
                    logger.warning(f"⚠️ Некорректный элемент стока: {item}")
        else:
            text += "   Сток пуст\n"
        text += "\n<i>Источник: fruityblox.com</i>\n"
        logger.info(f"ℹ️ Водяной знак добавлен для стока {stock_type}")
        return text
    except Exception as e:
        logger.error(f"❌ Ошибка получения стока {stock_type}: {e}")
        return f"❌ Ошибка получения данных: {e}"

def clean_text_for_pdf(text: str) -> str:
    """Удаление эмодзи и проблемных символов для PDF"""
    emoji_pattern = re.compile(
        "["
        u"\U0001F600-\U0001F64F"  # смайлики
        u"\U0001F300-\U0001F5FF"  # символы и пиктограммы
        u"\U0001F680-\U0001F6FF"  # транспорт и символы
        u"\U0001F1E0-\U0001F1FF"  # флаги
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        "]+", flags=re.UNICODE
    )
    return emoji_pattern.sub(r'', text)

def generate_stats_pdf(users: dict, bot_stats: dict) -> str:
    try:
        pdf = FPDF()
        pdf.add_page()
        if os.path.exists(Settings.FONT_PATH):
            pdf.add_font("DejaVu", "", Settings.FONT_PATH, uni=True)
            pdf.set_font("DejaVu", "", 12)
        else:
            pdf.set_font("Arial", "", 12)
            logger.warning("⚠️ Шрифт DejaVuSans не найден, используется Arial")

        pdf.set_text_color(150, 150, 150)
        pdf.set_font("DejaVu" if os.path.exists(Settings.FONT_PATH) else "Arial", "", 8)
        pdf.cell(200, 10, txt=clean_text_for_pdf("Источник: fruityblox.com"), ln=1, align="C")
        pdf.set_text_color(0, 0, 0)
        pdf.set_font("DejaVu" if os.path.exists(Settings.FONT_PATH) else "Arial", "", 12)

        title = clean_text_for_pdf("Статистика бота Blox Fruits")
        pdf.cell(200, 10, txt=title, ln=1, align="C")
        
        total_users = len(users)
        active_last_day = sum(1 for u in users.values() if datetime.fromisoformat(u["last_active"]) > datetime.now() - timedelta(days=1))
        total_messages = sum(u["message_count"] for u in users.values())
        avg_messages = total_messages / total_users if total_users else 0
        
        pdf.cell(200, 10, txt=clean_text_for_pdf(f"Всего пользователей: {total_users}"), ln=1)
        pdf.cell(200, 10, txt=clean_text_for_pdf(f"Активных за день: {active_last_day}"), ln=1)
        pdf.cell(200, 10, txt=clean_text_for_pdf(f"Общее сообщений: {total_messages}"), ln=1)
        pdf.cell(200, 10, txt=clean_text_for_pdf(f"Среднее сообщений на пользователя: {avg_messages:.2f}"), ln=1)
        pdf.cell(200, 10, txt=clean_text_for_pdf(f"Обработано сообщений: {bot_stats['messages_processed']}"), ln=1)
        pdf.cell(200, 10, txt=clean_text_for_pdf(f"Последняя задержка: {bot_stats['last_latency']:.2f} сек"), ln=1)
        
        pdf_path = "data/stats.pdf"
        pdf.output(pdf_path)
        logger.info(f"✅ PDF статистики создан: {pdf_path}")
        return pdf_path
    except Exception as e:
        logger.error(f"❌ Ошибка генерации PDF: {e}")
        return None

async def broadcast_message(text: str):
    users = await load_json(Settings.USERS_PATH, {})
    success_count = 0
    for user_id in users:
        try:
            await bot.send_message(int(user_id), text, parse_mode="HTML")
            success_count += 1
            logger.info(f"✅ Сообщение отправлено пользователю {user_id}")
        except Exception as e:
            logger.error(f"❌ Ошибка рассылки пользователю {user_id}: {e}")
    logger.info(f"📢 Рассылка завершена: успешно отправлено {success_count}/{len(users)}")
    return success_count

@user_router.message(Command("start"))
async def cmd_start(message: types.Message):
    await update_user_stats(message.from_user)
    bot_stats["messages_processed"] += 1
    logger.info(f"ℹ️ Пользователь {message.from_user.id} использовал команду /start")
    await message.answer(
        "Привет! Этот бот создан с целью просмотра внутриигрового стока Blox Fruit!",
        reply_markup=get_main_keyboard(),
        parse_mode="HTML"
    )

@user_router.message(Command("contacts"))
async def cmd_contacts(message: types.Message):
    await update_user_stats(message.from_user)
    bot_stats["messages_processed"] += 1
    logger.info(f"ℹ️ Пользователь {message.from_user.id} использовал команду /contacts")
    await message.answer(
        "Привет, похоже у тебя появился вопрос или идея для дополнения функционала бота? Пиши мне: @GentlemaNi78 или же заходи в группу в которой ты меня сможешь найти: https://t.me/gruppaUwU4w2",
        reply_markup=get_main_keyboard(),
        parse_mode="HTML"
    )

@user_router.message(Command("admin"))
async def cmd_admin(message: types.Message):
    await update_user_stats(message.from_user)
    bot_stats["messages_processed"] += 1
    role = await get_user_role(message.from_user.id)
    logger.info(f"ℹ️ Пользователь {message.from_user.id} запросил админ-панель, роль: {role}")
    if role:
        await message.answer("Добро пожаловать в админ-панель!", reply_markup=get_admin_keyboard(), parse_mode="HTML")
    else:
        await message.answer("❌ Доступ запрещен.")
        logger.warning(f"⚠️ Пользователь {message.from_user.id} пытался получить доступ к админ-панели без прав")

@user_router.message(lambda message: message.text == f"{Settings.NORMAL_EMOJI} Обычный сток")
async def view_normal_stock(message: types.Message):
    await update_user_stats(message.from_user)
    bot_stats["messages_processed"] += 1
    logger.info(f"ℹ️ Пользователь {message.from_user.id} запросил обычный сток")
    text = await get_stock("normal")
    await message.answer(text, reply_markup=get_stock_keyboard(), parse_mode="HTML")

@user_router.message(lambda message: message.text == f"{Settings.MIRAGE_EMOJI} Миражный сток")
async def view_mirage_stock(message: types.Message):
    await update_user_stats(message.from_user)
    bot_stats["messages_processed"] += 1
    logger.info(f"ℹ️ Пользователь {message.from_user.id} запросил миражный сток")
    text = await get_stock("mirage")
    await message.answer(text, reply_markup=get_stock_keyboard(), parse_mode="HTML")

@user_router.message(lambda message: message.text == "📞 Контакты")
async def view_contacts(message: types.Message):
    await update_user_stats(message.from_user)
    bot_stats["messages_processed"] += 1
    logger.info(f"ℹ️ Пользователь {message.from_user.id} запросил контакты")
    await message.answer(
        "Привет, похоже у тебя появился вопрос или идея для дополнения функционала бота? Пиши мне: @GentlemaNi78 или же заходи в группу в которой ты меня сможешь найти: https://t.me/gruppaUwU4w2",
        reply_markup=get_stock_keyboard(),
        parse_mode="HTML"
    )

@user_router.message(lambda message: message.text == "ℹ️ О боте")
async def view_bot_info(message: types.Message):
    await update_user_stats(message.from_user)
    bot_stats["messages_processed"] += 1
    logger.info(f"ℹ️ Пользователь {message.from_user.id} запросил информацию о боте")
    bot_info = (
        "<b>ℹ️ Информация о боте</b>\n"
        "• <b>Название:</b> Сток Бот Blox Fruit\n"
        "• <b>Создатель:</b> @aw3keN\n"
        "• <b>Владелец:</b> @GentlemaNi78\n"
        "• <b>Язык программирования:</b> Python\n"
        "• <b>Версия:</b> 2.6\n"
        "• <b>Назначение:</b> Отслеживание стоков Blox Fruits\n"
        "• <b>Контакты:</b> @GentlemaNi78 или https://t.me/gruppaUwU4w2"
    )
    await message.answer(bot_info, reply_markup=get_main_keyboard(), parse_mode="HTML")

@user_router.message(lambda message: message.text == "⬅ Назад")
async def back_to_main(message: types.Message):
    await update_user_stats(message.from_user)
    bot_stats["messages_processed"] += 1
    logger.info(f"ℹ️ Пользователь {message.from_user.id} вернулся в главное меню")
    await message.answer(
        "Привет! Этот бот создан с целью просмотра внутриигрового стока Blox Fruit!",
        reply_markup=get_main_keyboard(),
        parse_mode="HTML"
    )

# Админ-команды с FSM
@user_router.message(lambda message: message.text == "📢 Рассылка объявления")
async def admin_broadcast(message: types.Message, state: FSMContext):
    role = await get_user_role(message.from_user.id)
    if has_permission(role, "broadcast"):
        await message.answer("Введите текст объявления для рассылки:", reply_markup=types.ReplyKeyboardRemove())
        await state.set_state(AdminStates.waiting_for_broadcast)
        logger.info(f"ℹ️ Пользователь {message.from_user.id} начал ввод текста для рассылки")
    else:
        await message.answer("❌ Нет прав.", reply_markup=get_admin_keyboard())
        logger.warning(f"⚠️ Пользователь {message.from_user.id} пытался начать рассылку без прав")

@user_router.message(AdminStates.waiting_for_broadcast)
async def process_broadcast(message: types.Message, state: FSMContext):
    await update_user_stats(message.from_user)
    bot_stats["messages_processed"] += 1
    role = await get_user_role(message.from_user.id)
    if has_permission(role, "broadcast"):
        success_count = await broadcast_message(message.text)
        await message.answer(f"✅ Объявление разослано {success_count} пользователям.", reply_markup=get_admin_keyboard())
        logger.info(f"✅ Пользователь {message.from_user.id} успешно разослал объявление: {message.text[:50]}...")
        await state.clear()
    else:
        await message.answer("❌ Нет прав.", reply_markup=get_admin_keyboard())
        logger.warning(f"⚠️ Пользователь {message.from_user.id} пытался отправить рассылку без прав")
        await state.clear()

@user_router.message(lambda message: message.text == "📊 Получить PDF статистики")
async def admin_pdf(message: types.Message):
    role = await get_user_role(message.from_user.id)
    if has_permission(role, "pdf"):
        users = await load_json(Settings.USERS_PATH, {})
        pdf_path = generate_stats_pdf(users, bot_stats)
        if pdf_path and os.path.exists(pdf_path):
            try:
                await message.answer_document(FSInputFile(pdf_path), caption="Статистика в PDF", reply_markup=get_admin_keyboard())
                os.remove(pdf_path)
                logger.info(f"✅ Пользователь {message.from_user.id} получил PDF статистики")
            except Exception as e:
                await message.answer("❌ Ошибка отправки PDF.", reply_markup=get_admin_keyboard())
                logger.error(f"❌ Пользователь {message.from_user.id} не смог отправить PDF: {e}")
                if os.path.exists(pdf_path):
                    os.remove(pdf_path)
        else:
            await message.answer("❌ Ошибка создания PDF.", reply_markup=get_admin_keyboard())
            logger.error(f"❌ Пользователь {message.from_user.id} не смог получить PDF из-за ошибки")
    else:
        await message.answer("❌ Нет прав.", reply_markup=get_admin_keyboard())
        logger.warning(f"⚠️ Пользователь {message.from_user.id} пытался получить PDF без прав")

@user_router.message(lambda message: message.text == "👑 Выдать роль по ID")
async def admin_assign_role(message: types.Message, state: FSMContext):
    role = await get_user_role(message.from_user.id)
    if has_permission(role, "all"):
        await message.answer("Введите TG ID и роль (owner/admin/moderator): формат 'ID role'", reply_markup=types.ReplyKeyboardRemove())
        await state.set_state(AdminStates.waiting_for_role)
        logger.info(f"ℹ️ Пользователь {message.from_user.id} начал ввод для выдачи роли")
    else:
        await message.answer("❌ Нет прав.", reply_markup=get_admin_keyboard())
        logger.warning(f"⚠️ Пользователь {message.from_user.id} пытался выдать роль без прав")

@user_router.message(AdminStates.waiting_for_role)
async def process_assign_role(message: types.Message, state: FSMContext):
    await update_user_stats(message.from_user)
    bot_stats["messages_processed"] += 1
    role = await get_user_role(message.from_user.id)
    if has_permission(role, "all"):
        try:
            user_id, new_role = message.text.split()
            user_id = user_id.strip()
            new_role = new_role.strip().lower()
            if new_role not in ["owner", "admin", "moderator"]:
                await message.answer("❌ Неверная роль. Доступные: owner, admin, moderator", reply_markup=get_admin_keyboard())
                logger.warning(f"⚠️ Пользователь {message.from_user.id} указал неверную роль: {new_role}")
                await state.clear()
                return
            admins = await load_json(Settings.ADMINS_PATH, {})
            admins[user_id] = new_role
            await save_json(Settings.ADMINS_PATH, admins)
            await message.answer(f"✅ Роль {new_role} выдана пользователю {user_id}", reply_markup=get_admin_keyboard())
            logger.info(f"✅ Пользователь {message.from_user.id} выдал роль {new_role} пользователю {user_id}")
            await state.clear()
        except ValueError:
            await message.answer("❌ Неверный формат. Используйте: 'ID role'", reply_markup=get_admin_keyboard())
            logger.warning(f"⚠️ Пользователь {message.from_user.id} указал неверный формат для выдачи роли: {message.text}")
            await state.clear()
    else:
        await message.answer("❌ Нет прав.", reply_markup=get_admin_keyboard())
        logger.warning(f"⚠️ Пользователь {message.from_user.id} пытался выдать роль без прав")
        await state.clear()

@user_router.message(lambda message: message.text == "🚫 Убрать роль/бан")
async def admin_remove_role(message: types.Message, state: FSMContext):
    role = await get_user_role(message.from_user.id)
    if has_permission(role, "ban"):
        await message.answer("Введите TG ID для удаления роли/бана:", reply_markup=types.ReplyKeyboardRemove())
        await state.set_state(AdminStates.waiting_for_remove_role)
        logger.info(f"ℹ️ Пользователь {message.from_user.id} начал ввод для удаления роли")
    else:
        await message.answer("❌ Нет прав.", reply_markup=get_admin_keyboard())
        logger.warning(f"⚠️ Пользователь {message.from_user.id} пытался убрать роль без прав")

@user_router.message(AdminStates.waiting_for_remove_role)
async def process_remove_role(message: types.Message, state: FSMContext):
    await update_user_stats(message.from_user)
    bot_stats["messages_processed"] += 1
    role = await get_user_role(message.from_user.id)
    if has_permission(role, "ban"):
        try:
            user_id = message.text.strip()
            admins = await load_json(Settings.ADMINS_PATH, {})
            if user_id in admins:
                del admins[user_id]
                await save_json(Settings.ADMINS_PATH, admins)
                await message.answer(f"✅ Роль удалена для пользователя {user_id}", reply_markup=get_admin_keyboard())
                logger.info(f"✅ Пользователь {message.from_user.id} удалил роль у пользователя {user_id}")
            else:
                await message.answer(f"❌ Пользователь {user_id} не имеет роли", reply_markup=get_admin_keyboard())
                logger.warning(f"⚠️ Пользователь {message.from_user.id} пытался удалить несуществующую роль у {user_id}")
            await state.clear()
        except Exception as e:
            await message.answer("❌ Ошибка при удалении роли.", reply_markup=get_admin_keyboard())
            logger.error(f"❌ Пользователь {message.from_user.id} не смог удалить роль: {e}")
            await state.clear()
    else:
        await message.answer("❌ Нет прав.", reply_markup=get_admin_keyboard())
        logger.warning(f"⚠️ Пользователь {message.from_user.id} пытался удалить роль без прав")
        await state.clear()

@user_router.message(lambda message: message.text == "📋 Список админов")
async def admin_view_admins(message: types.Message):
    role = await get_user_role(message.from_user.id)
    if has_permission(role, "view_admins"):
        admins = await load_json(Settings.ADMINS_PATH, {})
        text = "Список админов:\n" + "\n".join(f"{uid}: {r}" for uid, r in admins.items())
        await message.answer(text, reply_markup=get_admin_keyboard())
        logger.info(f"ℹ️ Пользователь {message.from_user.id} просмотрел список админов")
    else:
        await message.answer("❌ Нет прав.", reply_markup=get_admin_keyboard())
        logger.warning(f"⚠️ Пользователь {message.from_user.id} пытался просмотреть список админов без прав")

@user_router.message(lambda message: message.text == "👥 Статистика пользователей")
async def admin_view_users(message: types.Message):
    role = await get_user_role(message.from_user.id)
    if has_permission(role, "view_users"):
        users = await load_json(Settings.USERS_PATH, {})
        total = len(users)
        active = sum(1 for u in users.values() if datetime.fromisoformat(u["last_active"]) > datetime.now() - timedelta(days=1))
        text = f"Всего: {total}\nАктивных: {active}"
        await message.answer(text, reply_markup=get_admin_keyboard())
        logger.info(f"ℹ️ Пользователь {message.from_user.id} просмотрел статистику пользователей")
    else:
        await message.answer("❌ Нет прав.", reply_markup=get_admin_keyboard())
        logger.warning(f"⚠️ Пользователь {message.from_user.id} пытался просмотреть статистику без прав")

@user_router.message(lambda message: message.text == "🔄 Ручное обновление стока")
async def admin_manual_update(message: types.Message):
    role = await get_user_role(message.from_user.id)
    if has_permission(role, "manual_update"):
        stock_handler = StockFileHandler(bot)
        await stock_handler.update_stock()
        await message.answer("✅ Сток обновлен.", reply_markup=get_admin_keyboard())
        logger.info(f"✅ Пользователь {message.from_user.id} выполнил ручное обновление стока")
    else:
        await message.answer("❌ Нет прав.", reply_markup=get_admin_keyboard())
        logger.warning(f"⚠️ Пользователь {message.from_user.id} пытался обновить сток без прав")

@user_router.message(lambda message: message.text == "📜 Просмотр логов")
async def admin_view_logs(message: types.Message):
    role = await get_user_role(message.from_user.id)
    if has_permission(role, "view_logs"):
        if os.path.exists("bot.log"):
            with open("bot.log", "r", encoding="utf-8") as f:
                logs = f.read()[-2000:]
            await message.answer(logs, reply_markup=get_admin_keyboard())
            logger.info(f"ℹ️ Пользователь {message.from_user.id} просмотрел логи")
        else:
            await message.answer("❌ Логи не найдены.", reply_markup=get_admin_keyboard())
            logger.warning(f"⚠️ Пользователь {message.from_user.id} пытался просмотреть несуществующие логи")
    else:
        await message.answer("❌ Нет прав.", reply_markup=get_admin_keyboard())
        logger.warning(f"⚠️ Пользователь {message.from_user.id} пытался просмотреть логи без прав")

@user_router.message(lambda message: message.text == "📥 Скачать логи")
async def admin_download_logs(message: types.Message):
    role = await get_user_role(message.from_user.id)
    if has_permission(role, "download_logs"):
        log_file = "bot.log"
        if os.path.exists(log_file):
            try:
                await message.answer_document(FSInputFile(log_file), caption="Файл логов", reply_markup=get_admin_keyboard())
                logger.info(f"✅ Пользователь {message.from_user.id} скачал файл логов")
            except Exception as e:
                await message.answer("❌ Ошибка отправки файла логов.", reply_markup=get_admin_keyboard())
                logger.error(f"❌ Пользователь {message.from_user.id} не смог скачать логи: {e}")
        else:
            await message.answer("❌ Файл логов не найден.", reply_markup=get_admin_keyboard())
            logger.warning(f"⚠️ Пользователь {message.from_user.id} пытался скачать несуществующий файл логов")
    else:
        await message.answer("❌ Нет прав.", reply_markup=get_admin_keyboard())
        logger.warning(f"⚠️ Пользователь {message.from_user.id} пытался скачать логи без прав")

async def main():
    try:
        if not Settings.TOKEN:
            logger.error("❌ Токен бота не указан")
            raise ValueError("Bot token is not set")
        await init_data_files()
        logger.info("📁 Данные инициализированы")
        dp.include_router(user_router)
        observer = Observer()
        stock_handler = StockFileHandler(bot)
        observer.schedule(stock_handler, path="data", recursive=False)
        observer.start()
        logger.info("ℹ️ Наблюдатель за файлами запущен")
        try:
            await dp.start_polling(bot)
        finally:
            observer.stop()
            observer.join()
            logger.info("ℹ️ Наблюдатель за файлами остановлен")
            await bot.session.close()
            logger.info("ℹ️ Сессия бота закрыта")
    except Exception as e:
        logger.error(f"❌ Ошибка запуска бота: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())