from __future__ import annotations

import asyncio
import logging
import os
from datetime import date, datetime
from typing import Optional

import aiosqlite
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiocryptopay import AioCryptoPay, Networks

# --- КОНФИГУРАЦИЯ (токены только из переменных окружения — не храните их в коде) ---
# Токены задайте в переменных окружения BOT_TOKEN и CRYPTO_BOT_TOKEN (или через .env при запуске).
BOT_TOKEN = os.getenv("8658610949:AAExh_qLAWHtK43igKmA4ImpScWMMaq5TWQ", "8658610949:AAExh_qLAWHtK43igKmA4ImpScWMMaq5TWQ")
CRYPTO_BOT_TOKEN = os.getenv("559493:AAI9ZqCm8MGpMgRdiAe3ey6rdZB0v89z81V", "559493:AAI9ZqCm8MGpMgRdiAe3ey6rdZB0v89z81V")
DB_PATH = "database.db"
ADMIN_ID = 8547519152
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()


# --- ЛОГГЕР ---
logger = logging.getLogger("bot")


# --- СОСТОЯНИЯ (FSM) ---
class Form(StatesGroup):
    waiting_for_deposit_amount = State()
    waiting_for_withdraw_amount = State()
    waiting_for_new_price = State()
    waiting_for_new_name = State()
    # --- Новые состояния из main2.py ---
    waiting_for_report_photo = State()  # отправка фото- или текстового отчёта по заданию
    waiting_for_chat_msg = State()  # пересылка сообщений в чате по заданию
    waiting_for_commission = State()  # ввод комиссии админом
    waiting_for_sim_number = State()  # ввод номера телефона (для заявки)
    waiting_for_sim_pin = State()  # ввод PIN-кода (для заявки)
    waiting_for_force_task_id = State()  # принудительное закрытие заказа (арбитраж)
    waiting_for_block_username = State()  # блокировка пользователя (ввод @username)
    waiting_for_unblock_username = State()  # разблокировка пользователя (ввод @username)


# --- ДИСПЕТЧЕР И СЕРВИСЫ (экземпляр Bot создаётся в main() после проверки BOT_TOKEN) ---
bot: Optional[Bot] = None
dp = Dispatcher()
crypto: Optional[AioCryptoPay] = None


# --- РАБОТА С БАЗОЙ ДАННЫХ ---
async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        # Таблица юзеров
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                balance REAL DEFAULT 0.0,
                frozen_balance REAL DEFAULT 0.0,
                sim_count INTEGER DEFAULT 0,
                username TEXT,
                is_blocked INTEGER DEFAULT 0,
                total_earned REAL DEFAULT 0.0
            )
            """
        )

        # Таблица тарифов
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS tariffs (
                id INTEGER PRIMARY KEY,
                name TEXT,
                price REAL
            )
            """
        )

        # Таблица купленных тарифов
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS user_tariffs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                tariff_id INTEGER,
                amount_frozen REAL,
                purchase_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # Таблица заявок
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS applications (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                status TEXT DEFAULT 'pending'
            )
            """
        )

        # Таблица доступных заданий (кто-то купил тариф, и теперь другой может его выполнить)
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS active_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                creator_id INTEGER,
                worker_id INTEGER DEFAULT NULL,
                tariff_name TEXT,
                reward REAL,
                sim_number TEXT,
                sim_pin TEXT,
                report_text TEXT,
                status TEXT DEFAULT 'open' -- open, in_progress, waiting_approval, completed
            )
            """
        )

        # МИГРАЦИИ (если таблицы уже были созданы старой версией)
        await _ensure_users_columns(db)
        await _ensure_applications_columns(db)
        await _ensure_active_tasks_columns(db)

        # Таблица настроек (для комиссии)
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value REAL
            )
            """
        )
        async with db.execute(
            "SELECT value FROM settings WHERE key = 'commission'"
        ) as cursor:
            row = await cursor.fetchone()
            if row is None:
                # По умолчанию фиксированная комиссия (USDT)
                await db.execute(
                    "INSERT INTO settings (key, value) VALUES ('commission', ?)",
                    (0.5,),
                )

        # Дефолтные тарифы
        async with db.execute("SELECT COUNT(*) FROM tariffs") as cursor:
            count_row = await cursor.fetchone()
            if count_row and count_row[0] == 0:
                tariffs = [
                    ("Тариф 1", 1.0),
                    ("Тариф 2", 5.0),
                    ("Тариф 3", 10.0),
                    ("Тариф 4", 20.0),
                ]
                await db.executemany(
                    "INSERT INTO tariffs (name, price) VALUES (?, ?)", tariffs
                )

        await db.commit()


async def _ensure_users_columns(db: aiosqlite.Connection) -> None:
    """Добавляет недостающие колонки в users, если таблица уже создана ранее."""
    async with db.execute("PRAGMA table_info(users)") as cursor:
        cols = await cursor.fetchall()

    # PRAGMA table_info: (cid, name, type, notnull, dflt_value, pk)
    col_names = {row[1] for row in cols}

    if "username" not in col_names:
        await db.execute("ALTER TABLE users ADD COLUMN username TEXT")
    if "is_blocked" not in col_names:
        await db.execute("ALTER TABLE users ADD COLUMN is_blocked INTEGER DEFAULT 0")
    if "total_earned" not in col_names:
        await db.execute("ALTER TABLE users ADD COLUMN total_earned REAL DEFAULT 0.0")
    if "joined_at" not in col_names:
        await db.execute("ALTER TABLE users ADD COLUMN joined_at TEXT")


async def _ensure_applications_columns(db: aiosqlite.Connection) -> None:
    async with db.execute("PRAGMA table_info(applications)") as cursor:
        cols = await cursor.fetchall()

    # PRAGMA table_info: (cid, name, type, notnull, dflt_value, pk)
    col_names = {row[1] for row in cols}

    # Добавляем только отсутствующие колонки. SQLite поддерживает ALTER TABLE ADD COLUMN.
    if "username" not in col_names:
        await db.execute("ALTER TABLE applications ADD COLUMN username TEXT")
    if "status" not in col_names:
        await db.execute("ALTER TABLE applications ADD COLUMN status TEXT DEFAULT 'pending'")


async def _ensure_active_tasks_columns(db: aiosqlite.Connection) -> None:
    """Добавляет недостающие колонки в active_tasks, если таблица уже создана ранее."""
    async with db.execute("PRAGMA table_info(active_tasks)") as cursor:
        cols = await cursor.fetchall()

    # PRAGMA table_info: (cid, name, type, notnull, dflt_value, pk)
    col_names = {row[1] for row in cols}

    if "worker_id" not in col_names:
        await db.execute("ALTER TABLE active_tasks ADD COLUMN worker_id INTEGER DEFAULT NULL")
    if "creator_id" not in col_names:
        await db.execute("ALTER TABLE active_tasks ADD COLUMN creator_id INTEGER")
    if "tariff_name" not in col_names:
        await db.execute("ALTER TABLE active_tasks ADD COLUMN tariff_name TEXT")
    if "reward" not in col_names:
        await db.execute("ALTER TABLE active_tasks ADD COLUMN reward REAL DEFAULT 0.0")
    if "status" not in col_names:
        await db.execute("ALTER TABLE active_tasks ADD COLUMN status TEXT DEFAULT 'open'")
    if "report_text" not in col_names:
        await db.execute("ALTER TABLE active_tasks ADD COLUMN report_text TEXT")
    if "sim_number" not in col_names:
        await db.execute("ALTER TABLE active_tasks ADD COLUMN sim_number TEXT")
    if "sim_pin" not in col_names:
        await db.execute("ALTER TABLE active_tasks ADD COLUMN sim_pin TEXT")


async def get_user_status(user_id: int) -> Optional[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT status FROM applications WHERE user_id = ?", (user_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None


async def get_queue_position(task_id: int) -> int:
    """
    Порядковый номер заявки в очереди на проверку (среди задач со статусом waiting_approval).
    Меньший id = раньше в очереди; для данной задачи позиция = 1 + число более старых ожидающих.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """
            SELECT COUNT(*) FROM active_tasks
            WHERE status = 'waiting_approval' AND id < ?
            """,
            (task_id,),
        ) as cursor:
            row = await cursor.fetchone()
            n_before = int(row[0]) if row and row[0] is not None else 0
            return n_before + 1


async def get_user_data(user_id: int) -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT balance, frozen_balance, sim_count, total_earned, username, is_blocked FROM users WHERE user_id = ?",
            (user_id,),
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return {
                    "balance": row[0],
                    "frozen": row[1],
                    "sim_count": row[2],
                    "total_earned": row[3],
                    "username": row[4],
                    "is_blocked": row[5] if row[5] is not None else 0,
                }

        # Если пользователя нет — создаём запись (дата первого /start для статистики)
        await db.execute(
            "INSERT INTO users (user_id, joined_at) VALUES (?, ?)",
            (user_id, datetime.now().isoformat(timespec="seconds")),
        )
        await db.commit()
        return {
            "balance": 0.0,
            "frozen": 0.0,
            "sim_count": 0,
            "total_earned": 0.0,
            "username": None,
            "is_blocked": 0,
        }


async def set_user_username(user_id: int, username: Optional[str]) -> None:
    """Сохраняем @username в БД для админских блокировок по нику."""
    if not username:
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET username = ? WHERE user_id = ?",
            (username, user_id),
        )
        await db.commit()


async def update_db(
    user_id: int, balance_change: float = 0, frozen_change: float = 0, sim_change: int = 0
) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            UPDATE users 
            SET balance = balance + ?, 
                frozen_balance = frozen_balance + ?, 
                sim_count = sim_count + ?
            WHERE user_id = ?
            """,
            (balance_change, frozen_change, sim_change, user_id),
        )
        await db.commit()


async def get_tariffs():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT id, name, price FROM tariffs") as cursor:
            return await cursor.fetchall()


async def get_approved_worker_ids(*, exclude_user_id: Optional[int] = None) -> list[int]:
    """id пользователей, которым доступно 'Зарабатывать' (approved и не заблокированы)."""
    async with aiosqlite.connect(DB_PATH) as db:
        query = """
        SELECT a.user_id
        FROM applications a
        JOIN users u ON u.user_id = a.user_id
        WHERE a.status = 'approved' AND COALESCE(u.is_blocked, 0) = 0
        """
        params: tuple = ()
        if exclude_user_id is not None:
            query += " AND a.user_id != ?"
            params = (exclude_user_id,)

        async with db.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            return [int(r[0]) for r in rows if r and r[0] is not None]


# --- КЛАВИАТУРЫ ---
async def get_main_kb(user_id: int) -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="👤 Мой профиль", callback_data="profile")
    builder.button(text="📲 Регистрация СИМ", callback_data="buy_sim")

    # Проверка статуса заявки перед отрисовкой кнопки заработка
    status = await get_user_status(user_id)
    if status == "approved":
        builder.button(text="💰 Заработать", callback_data="earn_action")
    else:
        builder.button(text="🚀 Начать зарабатывать", callback_data="start_earn")

    if user_id == ADMIN_ID:
        builder.button(text="⚙️ Админка", callback_data="admin_panel")

    builder.adjust(1)
    return builder.as_markup()


def get_profile_kb() -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="💳 Пополнить", callback_data="deposit")
    builder.button(text="📤 Вывод (Чек)", callback_data="withdraw")
    builder.button(text="⬅️ В меню", callback_data="back_to_main")
    builder.adjust(2, 1)
    return builder.as_markup()


def get_admin_kb_admin_panel() -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="📈 Полная статистика", callback_data="admin_full_stats")
    builder.button(text="📩 Заявки (ожидают)", callback_data="view_apps")
    builder.button(text="✅ Заявки (одобренные)", callback_data="admin_view_approved_apps")
    builder.button(text="❌ Заявки (отклонённые)", callback_data="admin_view_rejected_apps")
    builder.button(text="📜 Все заявки", callback_data="admin_view_all_apps")
    builder.button(text="⚙️ Настройка тарифов", callback_data="admin_tariffs_list")
    builder.button(text="📊 Комиссия", callback_data="admin_commission")
    builder.button(text="⚖️ Список на выплату", callback_data="admin_arbitrage_list")
    builder.button(text="⚖️ Арбитраж (закрыть заказ)", callback_data="admin_force_close")
    builder.button(text="🚫 Блокировать", callback_data="admin_block_user")
    builder.button(text="✅ Разблокировать", callback_data="admin_unblock_user")
    builder.button(text="⬅️ Назад", callback_data="back_to_main")
    builder.adjust(1)
    return builder.as_markup()


async def _admin_only(callback: types.CallbackQuery) -> bool:
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Недостаточно прав.", show_alert=True)
        return False
    return True


# --- ХЕНДЛЕРЫ ---
@dp.message(Command("start"))
async def cmd_start(message: types.Message) -> None:
    user = await get_user_data(message.from_user.id)
    await set_user_username(message.from_user.id, message.from_user.username)
    if message.from_user.id != ADMIN_ID and user.get("is_blocked"):
        return await message.answer("❌ Вы заблокированы в системе.")
    await message.answer(
        f"Привет, {message.from_user.first_name}! Бот готов к работе.",
        reply_markup=await get_main_kb(message.from_user.id),
    )


@dp.callback_query(F.data == "back_to_main")
async def back_to_main(callback: types.CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    user = await get_user_data(callback.from_user.id)
    if callback.from_user.id != ADMIN_ID and user.get("is_blocked"):
        return await callback.message.answer("❌ Вы заблокированы в системе.")
    await callback.message.edit_text(
        "Главное меню:", reply_markup=await get_main_kb(callback.from_user.id)
    )


@dp.callback_query(F.data == "profile")
async def show_profile(callback: types.CallbackQuery, state: Optional[FSMContext] = None) -> None:
    if state:
        await state.clear()

    user = await get_user_data(callback.from_user.id)
    if callback.from_user.id != ADMIN_ID and user.get("is_blocked"):
        return await callback.message.answer("❌ Вы заблокированы в системе.")
    text = (
        "<b>🗂 Ваш профиль:</b>\n\n"
        f"💰 Доступно: <code>{user['balance']:.2f}</code> USDT\n"
        f"❄️ Заморожено: <code>{user['frozen']:.2f}</code> USDT\n"
        f"📱 Активных СИМ: <code>{user['sim_count']}</code> шт.\n"
        f"💵 Всего заработано: <code>{user['total_earned']:.2f}</code> USDT"
    )

    try:
        await callback.message.edit_text(
            text, parse_mode="HTML", reply_markup=get_profile_kb()
        )
    except Exception:
        # Если edit_text не сработал (например, то же самое сообщение) — отправим новое
        await callback.message.answer(
            text, parse_mode="HTML", reply_markup=get_profile_kb()
        )


# --- ЛОГИКА ЗАРАБОТКА (Разделение по статусу из main2.py) ---
@dp.callback_query(F.data == "earn_action")
async def earn_action(callback: types.CallbackQuery) -> None:
    """Выбор категории тарифа для выполнения заданий (раздел 'Заработать')."""
    if callback.from_user.id != ADMIN_ID:
        user = await get_user_data(callback.from_user.id)
        if user.get("is_blocked"):
            return await callback.answer("❌ Вы заблокированы.", show_alert=True)
    async with aiosqlite.connect(DB_PATH) as db:
        # Группируем открытые задания по названиям тарифов
        async with db.execute(
            "SELECT tariff_name, COUNT(*) FROM active_tasks WHERE status = 'open' GROUP BY tariff_name"
        ) as cursor:
            tariffs = await cursor.fetchall()

    if not tariffs:
        return await callback.answer("📭 Сейчас нет доступных заданий.", show_alert=True)

    builder = InlineKeyboardBuilder()
    for name, count in tariffs:
        builder.button(text=f"📋 {name} ({count} шт.)", callback_data=f"earn_list_{name}")

    # Ссылка на старый режим "все задания" + возврат в меню
    builder.button(text="📋 Все задания", callback_data="earn_all_action")
    builder.button(text="⬅️ В меню", callback_data="back_to_main")
    builder.adjust(1)

    await callback.message.edit_text(
        "<b>💰 Выберите тип задания для выполнения:</b>",
        parse_mode="HTML",
        reply_markup=builder.as_markup(),
    )


@dp.callback_query(F.data.startswith("earn_list_"))
async def earn_tasks_by_tariff(callback: types.CallbackQuery) -> None:
    """Список заданий конкретного тарифа."""
    tariff_name = callback.data[len("earn_list_") :]
    items_per_page = 5
    offset = 0  # в этом режиме пагинации нет (как в main2.py)

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """
            SELECT id, reward
            FROM active_tasks
            WHERE status = 'open' AND tariff_name = ?
            LIMIT ? OFFSET ?
            """,
            (tariff_name, items_per_page, offset),
        ) as cursor:
            tasks = await cursor.fetchall()

    if not tasks:
        return await callback.answer("📭 По этому тарифу пока нет заданий.", show_alert=True)

    builder = InlineKeyboardBuilder()
    for t_id, reward in tasks:
        builder.button(
            text=f"🛠 Задание #{t_id} | +{reward:.2f} USDT",
            callback_data=f"take_task_{t_id}",
        )

    builder.row(types.InlineKeyboardButton(text="⬅️ К тарифам", callback_data="earn_action"))
    builder.adjust(1)

    await callback.message.edit_text(
        f"<b>Доступные задания [{tariff_name}] (первые {items_per_page}):</b>",
        parse_mode="HTML",
        reply_markup=builder.as_markup(),
    )


@dp.callback_query(F.data == "earn_all_action")
async def earn_all_action(callback: types.CallbackQuery, page: int = 0) -> None:
    """Список доступных заданий для выполнения (полный список + пагинация)."""
    items_per_page = 5
    offset = page * items_per_page

    async with aiosqlite.connect(DB_PATH) as db:
        # Считаем открытые задания
        async with db.execute("SELECT COUNT(*) FROM active_tasks WHERE status = 'open'") as cursor:
            total_row = await cursor.fetchone()
            total_tasks = int(total_row[0]) if total_row else 0

        # Получаем задания
        async with db.execute(
            """
            SELECT id, tariff_name, reward
            FROM active_tasks
            WHERE status = 'open'
            LIMIT ? OFFSET ?
            """,
            (items_per_page, offset),
        ) as cursor:
            tasks = await cursor.fetchall()

    if not tasks:
        return await callback.answer(
            "📭 Пока нет доступных заданий для заработка.", show_alert=True
        )

    builder = InlineKeyboardBuilder()
    for task_id, tariff_name, reward in tasks:
        builder.button(
            text=f"🛠 {tariff_name} | +{reward:.2f} USDT",
            callback_data=f"take_task_{task_id}",
        )

    # Пагинация
    nav_buttons: list[types.InlineKeyboardButton] = []
    if page > 0:
        nav_buttons.append(
            types.InlineKeyboardButton(text="⬅️", callback_data=f"earn_page_{page - 1}")
        )
    if (page + 1) * items_per_page < total_tasks:
        nav_buttons.append(
            types.InlineKeyboardButton(text="➡️", callback_data=f"earn_page_{page + 1}")
        )
    if nav_buttons:
        builder.row(*nav_buttons)

    builder.row(types.InlineKeyboardButton(text="⬅️ В меню", callback_data="back_to_main"))
    builder.adjust(1)

    await callback.message.edit_text(
        (
            "<b>💰 Доступные задания:</b>\n"
            "Выбирайте задание, которое готовы выполнить.\n"
            f"Всего заданий: {total_tasks}"
        ),
        parse_mode="HTML",
        reply_markup=builder.as_markup(),
    )


@dp.callback_query(F.data.startswith("earn_page_"))
async def earn_pagination(callback: types.CallbackQuery) -> None:
    """Переключение страниц в списке заданий для заработка."""
    page = int(callback.data.split("_")[-1])
    await earn_all_action(callback, page=page)


@dp.callback_query(F.data.startswith("take_task_"))
async def take_task(callback: types.CallbackQuery) -> None:
    """Взять (выполнить) задание: переводим в in_progress и ждём отчёт."""
    task_id = int(callback.data.split("_")[-1])

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """
            SELECT tariff_name, reward, creator_id, sim_number, sim_pin
            FROM active_tasks
            WHERE id = ? AND status = 'open'
            """,
            (task_id,),
        ) as cursor:
            task = await cursor.fetchone()

        if not task:
            return await callback.answer(
                "❌ Задание уже взяли или оно недоступно.", show_alert=True
            )

        tariff_name, reward, creator_id, sim_number, sim_pin = task

        # Помечаем задание как взятое исполнителем.
        await db.execute(
            """
            UPDATE active_tasks
            SET worker_id = ?, status = 'in_progress'
            WHERE id = ?
            """,
            (callback.from_user.id, task_id),
        )
        await db.commit()

    # Уведомляем заказчика, что его заказ уже взят в работу.
    try:
        await bot.send_message(
            int(creator_id),
            f"🔔 Ваш заказ #{task_id} взят исполнителем.\n\n"
            "Ожидайте отчёт — после него заказ перейдёт на проверку.",
        )
    except Exception as e:
        logger.error(
            "Ошибка уведомления заказчика при взятии заказа #%s: %s", task_id, e
        )

    # После взятия задания исполнитель может: отчёт, чат с заказчиком или отмена (логика из main2.py).
    builder = InlineKeyboardBuilder()
    builder.button(text="📸 Отправить отчёт", callback_data=f"send_report_{task_id}")
    builder.button(
        text="💬 Написать заказчику",
        callback_data=f"chat_with_{creator_id}_{task_id}",
    )
    builder.button(
        text="❌ Отменить заказ",
        callback_data=f"cancel_task_{task_id}",
    )
    builder.button(text="⬅️ В профиль", callback_data="profile")
    builder.adjust(1)

    sim_number_text = sim_number if sim_number else "не указано"
    pin_text = sim_pin if sim_pin and sim_pin.strip() != "-" else "не требуется"
    sim_info = (
        "📋 <b>Данные СИМ:</b>\n"
        f"<code>Номер: {sim_number_text}\nPIN: {pin_text}</code>\n\n"
    )

    await callback.message.edit_text(
        (
            f"✅ Вы взяли задание <b>{tariff_name}</b>!\n\n"
            f"{sim_info}"
            "Теперь вы можете отправить отчёт или связаться с заказчиком."
        ),
        parse_mode="HTML",
        reply_markup=builder.as_markup(),
    )


@dp.callback_query(F.data.startswith("cancel_task_"))
async def cancel_task_by_worker(callback: types.CallbackQuery) -> None:
    """Отмена заказа исполнителем: возврат заказчику суммы тарифа (разморозка + баланс) — см. main2.py, расчёт исправлен под reward = price − commission."""
    try:
        task_id = int(callback.data.split("_")[-1])
    except ValueError:
        return await callback.answer("Некорректный номер заказа.", show_alert=True)

    worker_user_id = callback.from_user.id

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """
            SELECT creator_id, reward, status, worker_id
            FROM active_tasks
            WHERE id = ?
            """,
            (task_id,),
        ) as cursor:
            task = await cursor.fetchone()

        if not task:
            return await callback.answer("❌ Заказ не найден.", show_alert=True)

        creator_id, reward, status, worker_id_db = task

        if status == "completed":
            return await callback.answer(
                "❌ Заказ уже завершён, отмена невозможна.",
                show_alert=True,
            )

        if status != "in_progress" or worker_id_db is None:
            return await callback.answer(
                "❌ Этот заказ нельзя отменить в текущем статусе.",
                show_alert=True,
            )

        if int(worker_id_db) != worker_user_id:
            return await callback.answer(
                "❌ Отменить может только тот, кто взял заказ.",
                show_alert=True,
            )

        async with db.execute(
            "SELECT value FROM settings WHERE key = 'commission'"
        ) as cursor:
            row = await cursor.fetchone()
            commission = float(row[0]) if row else 0.0

        # При создании заявки: reward = цена_тарифа − commission → полный возврат = reward + commission
        refund_total = float(reward) + commission

        await db.execute(
            """
            UPDATE users
            SET balance = balance + ?,
                frozen_balance = frozen_balance - ?,
                sim_count = sim_count - 1
            WHERE user_id = ?
            """,
            (refund_total, refund_total, creator_id),
        )
        await db.execute("DELETE FROM active_tasks WHERE id = ?", (task_id,))
        await db.commit()

    try:
        await callback.message.edit_text(
            f"✅ Заказ #{task_id} отменён. Заказчику возвращено <b>{refund_total:.2f} USDT</b> (с заморозки на баланс).",
            parse_mode="HTML",
            reply_markup=None,
        )
    except Exception:
        await callback.message.answer(
            f"✅ Заказ #{task_id} отменён. Заказчику возвращено {refund_total:.2f} USDT.",
        )

    try:
        await bot.send_message(
            int(creator_id),
            f"⚠️ Исполнитель отказался от заказа #{task_id}.\n"
            f"На баланс возвращено: <b>{refund_total:.2f} USDT</b> (заморозка снята).",
            parse_mode="HTML",
        )
    except Exception as e:
        logger.error("Ошибка уведомления заказчика при отмене: %s", e)

    await callback.answer()


# --- ЛОГИКА ЗАДАНИЙ: чат, отчёты, подтверждение выплаты ---
@dp.callback_query(F.data.startswith("chat_with_"))
async def start_chat(callback: types.CallbackQuery, state: FSMContext) -> None:
    try:
        # chat_with_{target_id}_{task_id}
        _, _, target_id, task_id = callback.data.split("_", 3)
        chat_target = int(target_id)
        chat_task_id = int(task_id)
    except Exception:
        return await callback.answer("Ошибка формата данных.", show_alert=True)

    # Запрещаем заказчику писать в чат после закрытия заказа.
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT status, creator_id FROM active_tasks WHERE id = ?",
            (chat_task_id,),
        ) as cursor:
            row = await cursor.fetchone()

    if not row:
        return await callback.answer("Заказ не найден.", show_alert=True)

    status, creator_id = row
    if status == "completed" and int(creator_id) == callback.from_user.id:
        return await callback.answer(
            "❌ Заказ закрыт. Чат с исполнителем недоступен.",
            show_alert=True,
        )

    await state.update_data(chat_target=chat_target, chat_task_id=chat_task_id)
    await callback.message.answer(
        "✉️ Отправьте сообщение (текст, фото или видео) для второй стороны:",
    )
    await state.set_state(Form.waiting_for_chat_msg)


@dp.message(Form.waiting_for_chat_msg)
async def forward_chat_msg(message: types.Message, state: FSMContext) -> None:
    data = await state.get_data()
    target_id = int(data["chat_target"])
    task_id = int(data["chat_task_id"])

    # Заказчик не может писать после закрытия заказа.
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT status, creator_id FROM active_tasks WHERE id = ?",
            (task_id,),
        ) as cursor:
            row = await cursor.fetchone()

    if not row:
        await message.answer("❌ Заказ не найден.")
        await state.clear()
        return

    status, creator_id = row
    if status == "completed" and int(creator_id) == message.from_user.id:
        await message.answer("❌ Заказ закрыт. Чат недоступен.")
        await state.clear()
        return

    builder = InlineKeyboardBuilder()
    builder.button(
        text="↩️ Ответить",
        callback_data=f"chat_with_{message.from_user.id}_{task_id}",
    )

    header = f"📩 <b>Новое сообщение по заданию #{task_id}:</b>\n\n"

    # Пересылаем с той же сущностью (текст/фото/видео).
    if message.text:
        await bot.send_message(
            target_id,
            header + message.text,
            parse_mode="HTML",
            reply_markup=builder.as_markup(),
        )
    elif message.photo:
        await bot.send_photo(
            target_id,
            message.photo[-1].file_id,
            caption=header + (message.caption or ""),
            parse_mode="HTML",
            reply_markup=builder.as_markup(),
        )
    elif message.video:
        await bot.send_video(
            target_id,
            message.video.file_id,
            caption=header + (message.caption or ""),
            parse_mode="HTML",
            reply_markup=builder.as_markup(),
        )
    else:
        await message.answer("⚠️ Поддерживаются только текст/фото/видео.")
        return

    await message.answer("✅ Сообщение отправлено.")
    await state.clear()


@dp.callback_query(F.data.startswith("send_report_"))
async def report_start(callback: types.CallbackQuery, state: FSMContext) -> None:
    task_id = int(callback.data.split("_")[-1])
    await state.update_data(rep_task_id=task_id)
    await callback.message.answer(
        f"📸 Отправьте фото (скриншот) отчёта или напишите текстом.\n"
        f"Поддерживается только один отчёт — задание #{task_id} будет обновлено.",
    )
    await state.set_state(Form.waiting_for_report_photo)


def _looks_like_telegram_file_id(value: object) -> bool:
    """Эвристика: проверяем, похоже ли строка на file_id Telegram (фото/видео)."""
    if not isinstance(value, str) or not value:
        return False
    # Для большинства file_id фото Telegram начинается с "Ag", видео — с "BA"/"BQ"/"Cg" и т.п.
    return value.startswith(("Ag", "BA", "BQ", "CA", "Cg", "CQ"))


@dp.message(Form.waiting_for_report_photo, F.photo)
async def report_finish_photo(message: types.Message, state: FSMContext) -> None:
    data = await state.get_data()
    task_id = int(data["rep_task_id"])

    photo_file_id = message.photo[-1].file_id

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            UPDATE active_tasks
            SET report_text = ?, status = 'waiting_approval'
            WHERE id = ?
            """,
            (photo_file_id, task_id),
        )
        async with db.execute(
            "SELECT creator_id FROM active_tasks WHERE id = ?",
            (task_id,),
        ) as cursor:
            row = await cursor.fetchone()
            creator_id = int(row[0]) if row else None
        await db.commit()

    if creator_id is not None:
        queue_pos = await get_queue_position(task_id)
        builder = InlineKeyboardBuilder()
        builder.button(
            text="👁 Посмотреть отчет",
            callback_data=f"check_rep_{task_id}",
        )
        builder.adjust(1)

        await bot.send_message(
            creator_id,
            (
                f"🔔 <b>Исполнитель отправил фото-отчёт!</b>\n"
                f"Заказ: #{task_id}\n"
                f"Ваша заявка <b>#{queue_pos}</b> в очереди на проверку.\n\n"
                f"Проверьте отчёт и подтвердите выплату."
            ),
            parse_mode="HTML",
            reply_markup=builder.as_markup(),
        )

    # Кнопки после отправки отчёта исполнителем: быстрый арбитраж + профиль
    arb_builder = InlineKeyboardBuilder()
    arb_builder.button(
        text="🆘 Позвать админа (Арбитраж)",
        url=f"tg://user?id={ADMIN_ID}",
    )
    arb_builder.button(text="⬅️ Профиль", callback_data="profile")
    arb_builder.adjust(1)

    await message.answer(
        "✅ Фото-отчёт отправлен заказчику на проверку.",
        reply_markup=arb_builder.as_markup(),
    )
    await state.clear()


@dp.message(Form.waiting_for_report_photo, F.text)
async def report_finish_text(message: types.Message, state: FSMContext) -> None:
    data = await state.get_data()
    task_id = int(data["rep_task_id"])

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            UPDATE active_tasks
            SET report_text = ?, status = 'waiting_approval'
            WHERE id = ?
            """,
            (message.text, task_id),
        )
        async with db.execute(
            "SELECT creator_id FROM active_tasks WHERE id = ?",
            (task_id,),
        ) as cursor:
            row = await cursor.fetchone()
            creator_id = int(row[0]) if row else None
        await db.commit()

    if creator_id is not None:
        queue_pos = await get_queue_position(task_id)
        builder = InlineKeyboardBuilder()
        builder.button(
            text="👁 Посмотреть отчет",
            callback_data=f"check_rep_{task_id}",
        )
        builder.adjust(1)

        await bot.send_message(
            creator_id,
            (
                f"🔔 <b>Исполнитель отправил текстовый отчёт!</b>\n"
                f"Заказ: #{task_id}\n"
                f"Ваша заявка <b>#{queue_pos}</b> в очереди на проверку.\n\n"
                f"Проверьте отчёт и подтвердите выплату."
            ),
            parse_mode="HTML",
            reply_markup=builder.as_markup(),
        )

    arb_builder = InlineKeyboardBuilder()
    arb_builder.button(
        text="🆘 Позвать админа (Арбитраж)",
        url=f"tg://user?id={ADMIN_ID}",
    )
    arb_builder.button(text="⬅️ Профиль", callback_data="profile")
    arb_builder.adjust(1)

    await message.answer(
        "✅ Отчет отправлен заказчику на проверку.",
        reply_markup=arb_builder.as_markup(),
    )
    await state.clear()


@dp.message(Form.waiting_for_report_photo)
async def report_wrong_content(message: types.Message) -> None:
    # Защита от двойного срабатывания, если диспетчер вызовет этот хендлер после более точного.
    if message.photo or message.text:
        return
    await message.answer(
        "⚠️ Пожалуйста, отправьте <b>фотографию</b> (скриншот) или текст отчёта.",
        parse_mode="HTML",
    )


@dp.callback_query(F.data.startswith("check_rep_"))
async def check_report(callback: types.CallbackQuery) -> None:
    task_id = int(callback.data.split("_")[-1])

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT report_text, worker_id, reward FROM active_tasks WHERE id = ?",
            (task_id,),
        ) as cursor:
            row = await cursor.fetchone()

    if not row:
        return await callback.answer("Отчёт не найден.", show_alert=True)

    report, worker_id, reward = row

    builder = InlineKeyboardBuilder()
    builder.button(
        text="✅ Подтвердить и Выплатить",
        callback_data=f"confirm_pay_{task_id}",
    )
    builder.button(
        text="💬 Написать исполнителю",
        callback_data=f"chat_with_{worker_id}_{task_id}",
    )
    builder.adjust(1)

    # Если в report_text хранится file_id Telegram — показываем фото, иначе считаем это текстом.
    if _looks_like_telegram_file_id(report):
        try:
            try:
                await callback.message.delete()
            except Exception:
                # Если удаление невозможно (например, сообщение уже удалено) — просто отправим фото.
                pass

            await bot.send_photo(
                callback.from_user.id,
                report,
                caption=f"📋 <b>Отчет по заданию #{task_id}:</b>\nПроверьте скриншот.",
                parse_mode="HTML",
                reply_markup=builder.as_markup(),
            )
        except Exception:
            # Если эвристика ошиблась и это не file_id — показываем как текст.
            await bot.send_message(
                callback.from_user.id,
                f"📋 <b>Отчет по заданию #{task_id}:</b>\n\n{report or ''}",
                parse_mode="HTML",
                reply_markup=builder.as_markup(),
            )
    else:
        await callback.message.answer(
            f"📋 <b>Отчет по заданию #{task_id}:</b>\n\n{report or ''}",
            parse_mode="HTML",
            reply_markup=builder.as_markup(),
        )


@dp.callback_query(F.data.startswith("confirm_pay_"))
async def confirm_payment(callback: types.CallbackQuery) -> None:
    task_id = int(callback.data.split("_")[-1])

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """
            SELECT worker_id, reward, creator_id
            FROM active_tasks
            WHERE id = ? AND status = 'waiting_approval'
            """,
            (task_id,),
        ) as cursor:
            res = await cursor.fetchone()

        if not res:
            return await callback.answer("Платёж невозможен (задача уже обработана).", show_alert=True)

        worker_id, reward, creator_id = res

        reward = float(reward)
        # В active_tasks уже сохранена "чистая" сумма к выплате (комиссия учтена при создании заявки).
        final_payout = reward

        # "Заморозка" при создании заявки была price, а в reward хранится (price - commission).
        # Значит при закрытии нужно списать с заказчика frozen_balance сумму (reward + commission).
        async with db.execute(
            "SELECT value FROM settings WHERE key = 'commission'"
        ) as commission_cursor:
            row = await commission_cursor.fetchone()
            commission = float(row[0]) if row else 0.0

        frozen_to_release = final_payout + commission

        await db.execute(
            "UPDATE active_tasks SET status = 'completed' WHERE id = ?",
            (task_id,),
        )

        # balance заказчика уже уменьшался при создании заявки (balance -= price),
        # поэтому при подтверждении закрытия трогаем только frozen_balance.
        await db.execute(
            """
            UPDATE users
            SET frozen_balance = frozen_balance - ?
            WHERE user_id = ?
            """,
            (frozen_to_release, creator_id),
        )

        await db.execute(
            """
            UPDATE users
            SET balance = balance + ?, total_earned = total_earned + ?,
                sim_count = sim_count + 1
            WHERE user_id = ?
            """,
            (final_payout, final_payout, worker_id),
        )
        await db.commit()

    # Важно: подтверждение могло быть нажато на сообщении с фото.
    # Тогда `edit_text` часто падает ошибкой, поэтому удаляем и отправляем новое сообщение.
    try:
        await callback.message.delete()
    except Exception:
        pass

    await callback.message.answer(
        f"✅ Заказ <b>#{task_id}</b> закрыт!",
        parse_mode="HTML",
    )

    try:
        await bot.send_message(
            int(worker_id),
            f"💰 Заказчик подтвердил ваш отчет по заказу #{task_id}!\n"
            f"На ваш баланс зачислено: <b>{final_payout:.2f} USDT</b>.",
            parse_mode="HTML",
        )
    except Exception as e:
        logger.error(f"Не удалось отправить уведомление исполнителю {worker_id}: {e}")

    await callback.answer("Выплата выполнена")


# --- ЛОГИКА ЗАЯВОК "НАЧАТЬ ЗАРАБАТЫВАТЬ" ---
@dp.callback_query(F.data == "start_earn")
async def apply_earn(callback: types.CallbackQuery) -> None:
    user_id = callback.from_user.id
    if user_id != ADMIN_ID:
        user = await get_user_data(user_id)
        if user.get("is_blocked"):
            return await callback.answer("❌ Вы заблокированы.", show_alert=True)

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT status FROM applications WHERE user_id = ?",
            (user_id,),
        ) as cursor:
            row = await cursor.fetchone()

        if row:
            status = row[0]
            if status == "pending":
                return await callback.answer(
                    "⚠️ Ваша заявка уже на рассмотрении.", show_alert=True
                )
            if status == "approved":
                return await callback.answer(
                    "✅ Вы уже можете зарабатывать!", show_alert=True
                )

        username = (
            f"@{callback.from_user.username}"
            if callback.from_user.username
            else "Нет username"
        )
        await db.execute(
            "INSERT OR REPLACE INTO applications (user_id, username, status) VALUES (?, ?, 'pending')",
            (user_id, username),
        )
        await db.commit()

    await callback.message.answer(
        "✅ Заявка подана! Ожидайте ответа админа."
    )
    await callback.answer()


# --- ЛОГИКА ПОКУПКИ С ЗАМОРОЗКОЙ ---
@dp.callback_query(F.data == "buy_sim")
async def show_tariffs(callback: types.CallbackQuery) -> None:
    if callback.from_user.id != ADMIN_ID:
        user = await get_user_data(callback.from_user.id)
        if user.get("is_blocked"):
            return await callback.answer("❌ Вы заблокированы.", show_alert=True)
    tariffs = await get_tariffs()
    builder = InlineKeyboardBuilder()

    for tid, name, price in tariffs:
        builder.button(text=f"{name} — {price} USDT", callback_data=f"buy_{tid}")

    builder.button(text="⬅️ Назад", callback_data="back_to_main")
    builder.adjust(1)

    await callback.message.edit_text(
        "Выберите тариф (сумма будет заморожена):",
        reply_markup=builder.as_markup(),
    )


@dp.callback_query(F.data.startswith("buy_"))
async def process_buy_start(callback: types.CallbackQuery, state: FSMContext) -> None:
    """Выбор тарифа -> запрос номера СИМ."""
    t_id = int(callback.data.split("_")[1])
    user = await get_user_data(callback.from_user.id)
    if callback.from_user.id != ADMIN_ID and user.get("is_blocked"):
        return await callback.answer("❌ Вы заблокированы.", show_alert=True)

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT name, price FROM tariffs WHERE id = ?",
            (t_id,),
        ) as cursor:
            tariff = await cursor.fetchone()

    if not tariff:
        return await callback.answer("❌ Тариф не найден.", show_alert=True)

    name, price = tariff
    price = float(price)

    if user["balance"] < price:
        return await callback.answer(
            "❌ Недостаточно средств для заморозки!", show_alert=True
        )

    # Сохраняем данные о выбранном тарифе в FSM, чтобы после ввода СИМ заморозить баланс.
    await state.update_data(buy_t_id=t_id, buy_t_name=name, buy_t_price=price)
    await callback.message.answer(
        "📞 Введите Казахстанский номер телефона для этой заявки:"
    )
    await state.set_state(Form.waiting_for_sim_number)


@dp.message(Form.waiting_for_sim_number)
async def process_buy_number(message: types.Message, state: FSMContext) -> None:
    """Ввод номера СИМ -> запрос PIN-кода."""
    sim_number = (message.text or "").strip()
    if not sim_number:
        return await message.answer("❌ Номер не может быть пустым. Повторите ввод.")

    await state.update_data(sim_number=sim_number)
    await message.answer("🔑 Введите ПИН-код (или напишите '-', если он не нужен):")
    await state.set_state(Form.waiting_for_sim_pin)


@dp.message(Form.waiting_for_sim_pin)
async def process_buy_finish(message: types.Message, state: FSMContext) -> None:
    """Ввод PIN -> заморозка баланса и создание задания."""
    data = await state.get_data()

    sim_number = (data.get("sim_number") or "").strip()
    pin = (message.text or "-").strip() or "-"
    t_id = int(data["buy_t_id"])
    price = float(data["buy_t_price"])
    name = data["buy_t_name"]
    task_id: Optional[int] = None

    if not sim_number:
        await state.clear()
        return await message.answer("❌ Номер СИМ не найден. Начните покупку заново.")

    # Проверяем баланс ещё раз на случай, если он изменился во время ввода.
    user = await get_user_data(message.from_user.id)
    if user["balance"] < price:
        await state.clear()
        return await message.answer("❌ Недостаточно средств. Начните покупку заново.")

    # Замораживаем сумму и увеличиваем счётчик СИМ.
    await update_db(
        message.from_user.id,
        balance_change=-price,
        frozen_change=price,
        sim_change=1,
    )

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO user_tariffs (user_id, tariff_id, amount_frozen)
            VALUES (?, ?, ?)
            """,
            (message.from_user.id, t_id, price),
        )

        # Комиссия сервиса фиксированная и учитывается при создании заявки (а не при выплате).
        async with db.execute(
            "SELECT value FROM settings WHERE key = 'commission'"
        ) as cursor:
            row = await cursor.fetchone()
            fixed_commission = float(row[0]) if row else 0.0

        # Награда исполнителю = цена тарифа минус фиксированная комиссия сервиса.
        reward = price - fixed_commission
        create_cur = await db.execute(
            """
            INSERT INTO active_tasks
                (creator_id, tariff_name, reward, status, sim_number, sim_pin)
            VALUES (?, ?, ?, 'open', ?, ?)
            """,
            (message.from_user.id, name, reward, sim_number, pin),
        )
        task_id = int(create_cur.lastrowid) if getattr(create_cur, "lastrowid", None) else None
        await db.commit()

    pin_text = pin if pin != "-" else "не требуется"
    await message.answer(
        "✅ Заявка создана!\n"
        f"Тариф: {name}\n"
        f"Заморожено: {price:.2f} USDT\n"
        f"Номер: {sim_number}\n"
        f"PIN: {pin_text}",
    )

    await state.clear()

    # Показываем профиль после завершения покупки.
    user_data = await get_user_data(message.from_user.id)
    text = (
        "<b>🗂 Ваш профиль:</b>\n\n"
        f"💰 Доступно: <code>{user_data['balance']:.2f}</code> USDT\n"
        f"❄️ Заморожено: <code>{user_data['frozen']:.2f}</code> USDT\n"
        f"📱 Активных СИМ: <code>{user_data['sim_count']}</code> шт.\n"
        f"💵 Всего заработано: <code>{user_data['total_earned']:.2f}</code> USDT"
    )
    await message.answer(text, parse_mode="HTML", reply_markup=get_profile_kb())

    # Уведомляем исполнителей, что появился новый заказ.
    # Права исполнителя определяются статусом applications.status = 'approved'.
    if task_id is not None:
        try:
            worker_ids = await get_approved_worker_ids(exclude_user_id=message.from_user.id)
            if worker_ids:
                builder = InlineKeyboardBuilder()
                builder.button(
                    text=f"🛠 Взять заказ #{task_id}",
                    callback_data=f"take_task_{task_id}",
                )
                builder.button(text="⬅️ В меню", callback_data="profile")
                builder.adjust(1)

                for worker_id in worker_ids:
                    try:
                        await bot.send_message(
                            int(worker_id),
                            f"📦 Появился новый заказ!\n"
                            f"Тариф: <b>{name}</b>\n"
                            f"Вознаграждение: <b>+{reward:.2f} USDT</b>\n"
                            f"Заказ: <b>#{task_id}</b>\n\n"
                            "Нажмите кнопку, чтобы взять задание.",
                            parse_mode="HTML",
                            reply_markup=builder.as_markup(),
                        )
                    except Exception as e:
                        logger.error(
                            "Не удалось уведомить исполнителя %s о новом заказе #%s: %s",
                            worker_id,
                            task_id,
                            e,
                        )
        except Exception as e:
            logger.error(
                "Ошибка рассылки исполнителям о новом заказе #%s: %s",
                task_id,
                e,
            )


# --- АДМИН-ПАНЕЛЬ (расширена approve_* из main2.py) ---
@dp.callback_query(F.data == "admin_panel")
async def admin_main(callback: types.CallbackQuery) -> None:
    if not await _admin_only(callback):
        return
    await callback.message.edit_text(
        "⚙️ Панель управления:", reply_markup=get_admin_kb_admin_panel()
    )


@dp.callback_query(F.data == "admin_full_stats")
async def admin_full_stats(callback: types.CallbackQuery) -> None:
    """Сводка: пользователи, выручка (комиссия с закрытых заказов), активность за сегодня."""
    if not await _admin_only(callback):
        return

    today = date.today().isoformat()

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM users") as c:
            row = await c.fetchone()
            total_users = int(row[0]) if row and row[0] is not None else 0

        async with db.execute(
            "SELECT COUNT(*) FROM active_tasks WHERE status = 'completed'"
        ) as c:
            row = await c.fetchone()
            completed_orders = int(row[0]) if row and row[0] is not None else 0

        async with db.execute(
            "SELECT value FROM settings WHERE key = 'commission'"
        ) as c:
            row = await c.fetchone()
            commission = float(row[0]) if row and row[0] is not None else 0.0

        # Уникальные «клиенты за день» — оформили покупку тарифа (заказ СИМ) сегодня
        async with db.execute(
            """
            SELECT COUNT(DISTINCT user_id) FROM user_tariffs
            WHERE date(purchase_date) = date(?)
            """,
            (today,),
        ) as c:
            row = await c.fetchone()
            clients_today = int(row[0]) if row and row[0] is not None else 0

        # Новые регистрации (первый /start) за сегодня — только если столбец joined_at заполнен
        async with db.execute(
            """
            SELECT COUNT(*) FROM users
            WHERE joined_at IS NOT NULL AND date(joined_at) = date(?)
            """,
            (today,),
        ) as c:
            row = await c.fetchone()
            new_users_today = int(row[0]) if row and row[0] is not None else 0

    # Выручка бота: фиксированная комиссия × число успешно закрытых заказов
    total_revenue = completed_orders * commission

    text = (
        "<b>📈 Полная статистика бота</b>\n\n"
        f"👥 Зарегистрировано пользователей (в базе): <b>{total_users}</b>\n"
        f"💵 Выручка бота (комиссия): <b>{total_revenue:.2f}</b> USDT\n"
        f"   └ закрытых заказов: <b>{completed_orders}</b> × комиссия <b>{commission:.2f}</b> USDT\n\n"
        f"📅 За сегодня ({today}):\n"
        f"   • клиентов (оформили заказ): <b>{clients_today}</b>\n"
        f"   • новых пользователей (первый /start): <b>{new_users_today}</b>"
    )

    builder = InlineKeyboardBuilder()
    builder.button(text="⬅️ Назад в админку", callback_data="admin_panel")
    builder.adjust(1)

    try:
        await callback.message.edit_text(
            text, parse_mode="HTML", reply_markup=builder.as_markup()
        )
    except Exception:
        await callback.message.answer(
            text, parse_mode="HTML", reply_markup=builder.as_markup()
        )
    await callback.answer()


@dp.callback_query(F.data == "admin_block_user")
async def admin_block_start(callback: types.CallbackQuery, state: FSMContext) -> None:
    """Админ блокирует пользователя по @username."""
    if not await _admin_only(callback):
        return
    await callback.message.answer("Введите @username пользователя для блокировки:")
    await state.set_state(Form.waiting_for_block_username)


@dp.message(Form.waiting_for_block_username)
async def admin_block_process(message: types.Message, state: FSMContext) -> None:
    if message.from_user.id != ADMIN_ID:
        return await state.clear()

    username = (message.text or "").replace("@", "").strip()
    if not username:
        await message.answer("Введите корректный @username.")
        return

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT user_id FROM users WHERE username = ?",
            (username,),
        ) as cursor:
            row = await cursor.fetchone()

        if not row:
            return await message.answer(f"❌ Пользователь @{username} не найден в базе.")

        target_id = int(row[0])
        if target_id == ADMIN_ID:
            return await message.answer("Нельзя заблокировать админа.")

        await db.execute(
            "UPDATE users SET is_blocked = 1 WHERE user_id = ?",
            (target_id,),
        )
        await db.commit()

    await message.answer(f"🚫 Пользователь @{username} успешно заблокирован.")
    await state.clear()


@dp.callback_query(F.data == "admin_unblock_user")
async def admin_unblock_start(callback: types.CallbackQuery, state: FSMContext) -> None:
    """Админ разблокирует пользователя по @username."""
    if not await _admin_only(callback):
        return
    await callback.message.answer("Введите @username пользователя для разблокировки:")
    await state.set_state(Form.waiting_for_unblock_username)


@dp.message(Form.waiting_for_unblock_username)
async def admin_unblock_process(message: types.Message, state: FSMContext) -> None:
    if message.from_user.id != ADMIN_ID:
        return await state.clear()

    username = (message.text or "").replace("@", "").strip()
    if not username:
        await message.answer("Введите корректный @username.")
        return

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT user_id FROM users WHERE username = ?",
            (username,),
        ) as cursor:
            row = await cursor.fetchone()

        if not row:
            return await message.answer(f"❌ Пользователь @{username} не найден в базе.")

        target_id = int(row[0])
        if target_id == ADMIN_ID:
            return await message.answer("Админ всегда разблокирован.")

        await db.execute(
            "UPDATE users SET is_blocked = 0 WHERE user_id = ?",
            (target_id,),
        )
        await db.commit()

    await message.answer(f"✅ Пользователь @{username} успешно разблокирован.")
    await state.clear()


@dp.callback_query(F.data == "view_apps")
async def view_applications(callback: types.CallbackQuery, page: int = 0) -> None:
    """Просмотр заявок с пагинацией (по 5 шт. на страницу)."""
    if not await _admin_only(callback):
        return

    items_per_page = 5
    offset = page * items_per_page

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM applications WHERE status = 'pending'"
        ) as cursor:
            total_apps_row = await cursor.fetchone()
            total_apps = int(total_apps_row[0]) if total_apps_row else 0

        async with db.execute(
            """
            SELECT user_id, username
            FROM applications
            WHERE status = 'pending'
            LIMIT ? OFFSET ?
            """,
            (items_per_page, offset),
        ) as cursor:
            apps = await cursor.fetchall()

    if not apps and page == 0:
        return await callback.answer("Новых заявок пока нет", show_alert=True)

    builder = InlineKeyboardBuilder()
    for uid, u_name in apps:
        u_name = u_name or "Нет username"
        # Кнопка-ссылка на пользователя
        builder.button(text=f"👤 {u_name}", url=f"tg://user?id={uid}")
        # Действия по заявке: одобрить / отклонить / удалить из БД
        builder.button(text="✅ Одобрить", callback_data=f"approve_{uid}")
        builder.button(text="🚫 Отклонить", callback_data=f"rejectapp_{uid}")
        builder.button(text="🗑 Удалить", callback_data=f"delapp_{uid}")

    # Раскладка: строка с именем (1), затем три кнопки в ряд (3)
    rows: list[int] = []
    for _ in range(len(apps)):
        rows.append(1)
        rows.append(3)
    if rows:
        builder.adjust(*rows)

    # Навигация по страницам
    nav_buttons: list[types.InlineKeyboardButton] = []
    if page > 0:
        nav_buttons.append(
            types.InlineKeyboardButton(
                text="⬅️ Назад", callback_data=f"view_apps_page_{page - 1}"
            )
        )
    if (page + 1) * items_per_page < total_apps:
        nav_buttons.append(
            types.InlineKeyboardButton(
                text="Вперед ➡️", callback_data=f"view_apps_page_{page + 1}"
            )
        )
    if nav_buttons:
        builder.row(*nav_buttons)

    builder.row(
        types.InlineKeyboardButton(text="💎 Меню админа", callback_data="admin_panel")
    )

    text = (
        f"📩 <b>Заявки (Страница {page + 1})</b>\n"
        f"Всего ожидают: {total_apps}"
    )

    try:
        await callback.message.edit_text(
            text, parse_mode="HTML", reply_markup=builder.as_markup()
        )
    except Exception:
        await callback.message.answer(
            text, parse_mode="HTML", reply_markup=builder.as_markup()
        )


@dp.callback_query(F.data.startswith("view_apps_page_"))
async def view_applications_pagination(callback: types.CallbackQuery) -> None:
    """Переключение страниц в списке заявок."""
    if not await _admin_only(callback):
        return
    page = int(callback.data.split("_")[-1])
    await view_applications(callback, page=page)


@dp.callback_query(F.data.startswith("approve_"))
async def approve_user(callback: types.CallbackQuery) -> None:
    if not await _admin_only(callback):
        return

    uid = int(callback.data.split("_")[1])
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE applications SET status = 'approved' WHERE user_id = ?",
            (uid,),
        )
        await db.commit()

    try:
        await bot.send_message(
            uid,
            "🎉 Ваша заявка на заработок одобрена! Теперь вам доступна кнопка 'Заработать' в меню.",
        )
    except Exception:
        # Пользователь мог ограничить сообщения
        pass

    await callback.answer("Пользователь одобрен")
    await view_applications(callback)


@dp.callback_query(F.data.startswith("delapp_"))
async def delete_app(callback: types.CallbackQuery) -> None:
    if not await _admin_only(callback):
        return

    uid = int(callback.data.split("_")[1])
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM applications WHERE user_id = ?", (uid,))
        await db.commit()

    await callback.answer("Заявка удалена")
    await view_applications(callback)


@dp.callback_query(F.data.startswith("rejectapp_"))
async def reject_application(callback: types.CallbackQuery) -> None:
    """Отклонение заявки без удаления записи — пользователь сможет подать снова (INSERT OR REPLACE)."""
    if not await _admin_only(callback):
        return

    uid = int(callback.data.split("_")[1])
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE applications SET status = 'rejected' WHERE user_id = ?",
            (uid,),
        )
        await db.commit()

    try:
        await bot.send_message(
            uid,
            "❌ Ваша заявка на доступ к разделу «Заработать» отклонена. "
            "При необходимости подайте заявку снова через главное меню.",
        )
    except Exception as e:
        logger.warning("Не удалось уведомить пользователя %s об отклонении: %s", uid, e)

    await callback.answer("Заявка отклонена")
    await view_applications(callback)


def _apps_list_keyboard(
    apps: list[tuple],
    page: int,
    total: int,
    items_per_page: int,
    nav_prefix: str,
    title: str,
) -> tuple[str, types.InlineKeyboardMarkup]:
    """Общая разметка списка заявок для админки (текст + клавиатура)."""
    builder = InlineKeyboardBuilder()
    for row in apps:
        if len(row) == 3:
            uid, u_name, st = row[0], row[1], row[2]
        else:
            uid, u_name = row[0], row[1]
            st = "pending"
        u_name = u_name or "Нет username"
        builder.button(text=f"👤 {u_name} ({st})", url=f"tg://user?id={uid}")
        builder.button(text="✅", callback_data=f"approve_{uid}")
        builder.button(text="🚫", callback_data=f"rejectapp_{uid}")
        builder.button(text="🗑", callback_data=f"delapp_{uid}")
    rows: list[int] = []
    for _ in range(len(apps)):
        rows.extend((1, 3))
    if rows:
        builder.adjust(*rows)

    nav_buttons: list[types.InlineKeyboardButton] = []
    if page > 0:
        nav_buttons.append(
            types.InlineKeyboardButton(
                text="⬅️", callback_data=f"{nav_prefix}_{page - 1}"
            )
        )
    if (page + 1) * items_per_page < total:
        nav_buttons.append(
            types.InlineKeyboardButton(
                text="➡️", callback_data=f"{nav_prefix}_{page + 1}"
            )
        )
    if nav_buttons:
        builder.row(*nav_buttons)

    builder.row(
        types.InlineKeyboardButton(text="💎 Меню админа", callback_data="admin_panel")
    )

    text = f"{title}\nСтраница <b>{page + 1}</b>, всего записей: <b>{total}</b>"
    return text, builder.as_markup()


@dp.callback_query(F.data == "admin_view_approved_apps")
async def admin_view_approved_apps(callback: types.CallbackQuery) -> None:
    await _admin_applications_by_status(callback, status="approved", page=0)


@dp.callback_query(F.data.startswith("admin_approved_apps_page_"))
async def admin_view_approved_apps_page(callback: types.CallbackQuery) -> None:
    if not await _admin_only(callback):
        return
    page = int(callback.data.split("_")[-1])
    await _admin_applications_by_status(callback, status="approved", page=page)


@dp.callback_query(F.data == "admin_view_rejected_apps")
async def admin_view_rejected_apps(callback: types.CallbackQuery) -> None:
    """Список отклонённых заявок на доступ к разделу «Заработать»."""
    await _admin_applications_by_status(callback, status="rejected", page=0)


@dp.callback_query(F.data.startswith("admin_rejected_apps_page_"))
async def admin_view_rejected_apps_page(callback: types.CallbackQuery) -> None:
    if not await _admin_only(callback):
        return
    page = int(callback.data.split("_")[-1])
    await _admin_applications_by_status(callback, status="rejected", page=page)


@dp.callback_query(F.data == "admin_view_all_apps")
async def admin_view_all_apps(callback: types.CallbackQuery) -> None:
    await _admin_applications_all(callback, page=0)


@dp.callback_query(F.data.startswith("admin_all_apps_page_"))
async def admin_view_all_apps_page(callback: types.CallbackQuery) -> None:
    if not await _admin_only(callback):
        return
    page = int(callback.data.split("_")[-1])
    await _admin_applications_all(callback, page=page)


async def _admin_applications_by_status(
    callback: types.CallbackQuery,
    *,
    status: str,
    page: int,
) -> None:
    if not await _admin_only(callback):
        return

    items_per_page = 5
    offset = page * items_per_page

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM applications WHERE status = ?",
            (status,),
        ) as cursor:
            total_row = await cursor.fetchone()
            total = int(total_row[0]) if total_row else 0

        async with db.execute(
            """
            SELECT user_id, username, status
            FROM applications
            WHERE status = ?
            LIMIT ? OFFSET ?
            """,
            (status, items_per_page, offset),
        ) as cursor:
            apps = await cursor.fetchall()

    if not apps and page == 0:
        return await callback.answer(f"Заявок со статусом «{status}» нет.", show_alert=True)

    # Заголовок и префикс пагинации зависят от статуса (одобренные / отклонённые и т.д.)
    status_titles = {
        "approved": "✅ <b>Заявки: одобренные</b>",
        "rejected": "❌ <b>Заявки: отклонённые</b>",
    }
    nav_prefix_by_status = {
        "approved": "admin_approved_apps_page",
        "rejected": "admin_rejected_apps_page",
    }
    title = status_titles.get(status, f"📋 <b>Заявки: {status}</b>")
    nav_prefix = nav_prefix_by_status.get(status, "admin_approved_apps_page")

    text, markup = _apps_list_keyboard(
        apps,
        page,
        total,
        items_per_page,
        nav_prefix,
        title,
    )
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=markup)
    except Exception:
        await callback.message.answer(text, parse_mode="HTML", reply_markup=markup)


async def _admin_applications_all(callback: types.CallbackQuery, *, page: int) -> None:
    if not await _admin_only(callback):
        return

    items_per_page = 5
    offset = page * items_per_page

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM applications") as cursor:
            total_row = await cursor.fetchone()
            total = int(total_row[0]) if total_row else 0

        async with db.execute(
            """
            SELECT user_id, username, status
            FROM applications
            LIMIT ? OFFSET ?
            """,
            (items_per_page, offset),
        ) as cursor:
            apps = await cursor.fetchall()

    if not apps and page == 0:
        return await callback.answer("Таблица заявок пуста.", show_alert=True)

    title = "📜 <b>Все заявки</b>"
    text, markup = _apps_list_keyboard(
        apps,
        page,
        total,
        items_per_page,
        "admin_all_apps_page",
        title,
    )
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=markup)
    except Exception:
        await callback.message.answer(text, parse_mode="HTML", reply_markup=markup)


@dp.callback_query(F.data == "admin_tariffs_list")
async def admin_tariffs_list(callback: types.CallbackQuery) -> None:
    if not await _admin_only(callback):
        return

    tariffs = await get_tariffs()
    builder = InlineKeyboardBuilder()
    for tid, name, price in tariffs:
        builder.button(
            text=f"⚙️ {name} ({price} USDT)",
            callback_data=f"manage_{tid}",
        )
    builder.button(text="⬅️ Назад", callback_data="admin_panel")
    builder.adjust(1)

    await callback.message.edit_text(
        "⚙️ Выберите тариф для настройки:",
        reply_markup=builder.as_markup(),
    )


@dp.callback_query(F.data.startswith("manage_"))
async def manage_tariff_options(callback: types.CallbackQuery) -> None:
    if not await _admin_only(callback):
        return

    t_id = int(callback.data.split("_")[1])
    builder = InlineKeyboardBuilder()
    builder.button(text="📝 Изменить название", callback_data=f"editname_{t_id}")
    builder.button(text="💰 Изменить цену", callback_data=f"editprice_{t_id}")
    builder.button(text="⬅️ Назад", callback_data="admin_tariffs_list")
    builder.adjust(1)

    await callback.message.edit_text(
        "Что именно вы хотите изменить?",
        reply_markup=builder.as_markup(),
    )


@dp.callback_query(F.data.startswith("editname_"))
async def edit_name_start(callback: types.CallbackQuery, state: FSMContext) -> None:
    if not await _admin_only(callback):
        return

    t_id = int(callback.data.split("_")[1])
    await state.update_data(edit_t_id=t_id)
    await callback.message.edit_text("Введите новое название тарифа:")
    await state.set_state(Form.waiting_for_new_name)


@dp.message(Form.waiting_for_new_name)
async def process_new_name(message: types.Message, state: FSMContext) -> None:
    if message.from_user.id != ADMIN_ID:
        return

    data = await state.get_data()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE tariffs SET name = ? WHERE id = ?",
            (message.text, data["edit_t_id"]),
        )
        await db.commit()

    await message.answer(f"✅ Название изменено на: {message.text}")
    await state.clear()
    await cmd_start(message)


@dp.callback_query(F.data.startswith("editprice_"))
async def edit_price_start(callback: types.CallbackQuery, state: FSMContext) -> None:
    if not await _admin_only(callback):
        return

    t_id = int(callback.data.split("_")[1])
    await state.update_data(edit_t_id=t_id)
    await callback.message.edit_text("Введите новую цену (число):")
    await state.set_state(Form.waiting_for_new_price)


@dp.message(Form.waiting_for_new_price)
async def process_new_price(message: types.Message, state: FSMContext) -> None:
    if message.from_user.id != ADMIN_ID:
        return

    try:
        new_price = float(message.text.replace(",", "."))
    except Exception:
        return await message.answer("❌ Ошибка. Введите число.")

    data = await state.get_data()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE tariffs SET price = ? WHERE id = ?",
            (new_price, data["edit_t_id"]),
        )
        await db.commit()

    await message.answer(f"✅ Цена успешно изменена на {new_price} USDT")
    await state.clear()
    await cmd_start(message)


# --- ЛОГИКА ПОПОЛНЕНИЯ ---
@dp.callback_query(F.data == "admin_commission")
async def admin_commission(callback: types.CallbackQuery, state: FSMContext) -> None:
    if not await _admin_only(callback):
        return

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT value FROM settings WHERE key = 'commission'"
        ) as cursor:
            row = await cursor.fetchone()
            val = float(row[0]) if row else 0.0

    await callback.message.answer(
        f"💰 Текущая фиксированная комиссия: <b>{val} USDT</b>\n"
        f"Введите новую сумму (например 0.5):",
        parse_mode="HTML",
    )
    await state.set_state(Form.waiting_for_commission)


@dp.message(Form.waiting_for_commission)
async def process_new_commission(message: types.Message, state: FSMContext) -> None:
    if message.from_user.id != ADMIN_ID:
        return

    try:
        val = float(message.text.replace(",", "."))
    except Exception:
        return await message.answer("Введите корректное число.")

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE settings SET value = ? WHERE key = 'commission'",
            (val,),
        )
        await db.commit()

    await message.answer(
        f"✅ Фиксированная комиссия изменена на <b>{val} USDT</b>",
        parse_mode="HTML",
    )
    await state.clear()


# --- АРБИТРАЖ (принудительное закрытие заказа) ---
@dp.callback_query(F.data == "admin_force_close")
async def admin_force_close_start(callback: types.CallbackQuery, state: FSMContext) -> None:
    """Админ вручную закрывает заказ и выплачивает исполнителю."""
    if not await _admin_only(callback):
        return

    await callback.message.edit_text(
        "⚖️ Арбитраж: введите ID заказа для принудительного закрытия.\n"
        "Деньги будут выплачены исполнителю.",
    )
    await state.set_state(Form.waiting_for_force_task_id)


@dp.message(Form.waiting_for_force_task_id)
async def admin_force_close_process(message: types.Message, state: FSMContext) -> None:
    """Получаем ID заказа и закрываем его."""
    if message.from_user.id != ADMIN_ID:
        await state.clear()
        return

    try:
        task_id = int((message.text or "").strip())
    except Exception:
        return await message.answer("Введите корректный номер ID.")

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT worker_id, reward, creator_id, status FROM active_tasks WHERE id = ?",
            (task_id,),
        ) as cursor:
            res = await cursor.fetchone()

        if not res:
            await message.answer("Заказ не найден.")
            await state.clear()
            return

        worker_id, reward, creator_id, status = res

        if status == "completed":
            await message.answer("Заказ уже завершен.")
            await state.clear()
            return

        if worker_id is None:
            await message.answer("У заказа нет исполнителя — арбитраж невозможен.")
            await state.clear()
            return

        if creator_id is None:
            await message.answer("У заказа нет заказчика — арбитраж невозможен.")
            await state.clear()
            return

        reward = float(reward)
        # В active_tasks уже сохранена "чистая" сумма к выплате (комиссия учтена при создании заявки)
        final_payout = reward

        # "Заморозка" при создании заявки была price, а в reward хранится (price - commission).
        # Значит при закрытии нужно списать с заказчика frozen_balance сумму (reward + commission).
        async with db.execute(
            "SELECT value FROM settings WHERE key = 'commission'"
        ) as commission_cursor:
            row = await commission_cursor.fetchone()
            commission = float(row[0]) if row else 0.0
        frozen_to_release = final_payout + commission

        # Мини-защита от повторной выплаты: закрываем только незавершенные.
        await db.execute(
            "UPDATE active_tasks SET status = 'completed' WHERE id = ? AND status != 'completed'",
            (task_id,),
        )

        # balance заказчика уже уменьшался при создании заявки (balance -= price),
        # поэтому при подтверждении закрытия трогаем только frozen_balance.
        await db.execute(
            """
            UPDATE users
            SET frozen_balance = frozen_balance - ?
            WHERE user_id = ?
            """,
            (frozen_to_release, creator_id),
        )

        await db.execute(
            """
            UPDATE users
            SET balance = balance + ?, total_earned = total_earned + ?,
                sim_count = sim_count + 1
            WHERE user_id = ?
            """,
            (final_payout, final_payout, int(worker_id)),
        )
        await db.commit()

    await message.answer(f"✅ Заказ #{task_id} закрыт!")
    try:
        await bot.send_message(
            int(worker_id),
            f"⚖️ Администратор одобрил ваш отчет по заказу #{task_id} (арбитраж).\n"
            f"Зачислено: {final_payout:.2f} USDT.",
        )
    except Exception:
        pass

    await state.clear()


@dp.callback_query(F.data == "admin_arbitrage_list")
async def admin_arbitrage_list(callback: types.CallbackQuery) -> None:
    """Выводит список всех заказов, ожидающих подтверждения."""
    if not await _admin_only(callback):
        return

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT id, tariff_name, reward "
            "FROM active_tasks WHERE status = 'waiting_approval'"
        ) as cursor:
            tasks = await cursor.fetchall()

    if not tasks:
        return await callback.answer(
            "📭 Нет заказов, ожидающих подтверждения.", show_alert=True
        )

    builder = InlineKeyboardBuilder()
    for t_id, name, reward in tasks:
        builder.button(
            text=f"📦 #{t_id} | {name} | {reward} USDT",
            callback_data=f"admin_view_task_{t_id}",
        )

    builder.button(text="⬅️ Назад", callback_data="admin_panel")
    builder.adjust(1)

    await callback.message.edit_text(
        "<b>⚖️ Список на выплату:</b>\n"
        "Выберите заказ для проверки и принудительного закрытия.",
        parse_mode="HTML",
        reply_markup=builder.as_markup(),
    )


@dp.callback_query(F.data.startswith("admin_view_task_"))
async def admin_view_task(callback: types.CallbackQuery) -> None:
    """Просмотр конкретного отчёта админом."""
    if not await _admin_only(callback):
        return

    try:
        task_id = int(callback.data.split("_")[-1])
    except Exception:
        return await callback.answer("Некорректный ID заказа.", show_alert=True)

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT report_text, worker_id, reward, tariff_name "
            "FROM active_tasks WHERE id = ?",
            (task_id,),
        ) as cursor:
            task = await cursor.fetchone()

    if not task:
        return await callback.answer("Заказ не найден.", show_alert=True)

    report_text, worker_id, reward, name = task

    builder = InlineKeyboardBuilder()
    builder.button(
        text="💰 Выплатить принудительно",
        callback_data=f"confirm_pay_{task_id}",
    )
    builder.button(text="⬅️ К списку", callback_data="admin_arbitrage_list")
    builder.adjust(1)

    # Удаляем список с кнопками (иногда удаление может не сработать, это не критично).
    try:
        await callback.message.delete()
    except Exception:
        pass

    caption = (
        f"🔎 <b>Проверка заказа #{task_id}</b>\n\n"
        f"Тариф: {name}\n"
        f"Сумма к выплате: {float(reward):.2f} USDT\n"
        f"ID Исполнителя: <code>{worker_id}</code>"
    )

    # `report_text` может храниться и как Telegram `file_id` (фото), и как обычный текст.
    if _looks_like_telegram_file_id(report_text):
        await bot.send_photo(
            callback.from_user.id,
            report_text,
            caption=caption,
            parse_mode="HTML",
            reply_markup=builder.as_markup(),
        )
    else:
        await bot.send_message(
            callback.from_user.id,
            caption + f"\n\nОтчёт:\n{report_text or ''}",
            parse_mode="HTML",
            reply_markup=builder.as_markup(),
        )


@dp.callback_query(F.data == "deposit")
async def start_deposit(callback: types.CallbackQuery, state: FSMContext) -> None:
    if callback.from_user.id != ADMIN_ID:
        user = await get_user_data(callback.from_user.id)
        if user.get("is_blocked"):
            return await callback.answer("❌ Вы заблокированы.", show_alert=True)
    await callback.message.edit_text("💰 Введите сумму пополнения (USDT):")
    await state.set_state(Form.waiting_for_deposit_amount)


@dp.message(Form.waiting_for_deposit_amount)
async def process_deposit(message: types.Message, state: FSMContext) -> None:
    try:
        amount = float(message.text.replace(",", "."))
        if amount < 0.1:
            raise ValueError("Слишком мало")
    except Exception:
        return await message.answer("❌ Введите число больше 0.1:")

    if not crypto:
        return await message.answer("❌ Крипто-сервис ещё не инициализирован.")

    invoice = await crypto.create_invoice(asset="USDT", amount=amount)
    await state.clear()

    builder = InlineKeyboardBuilder()
    builder.button(text="💎 Оплатить", url=invoice.bot_invoice_url)
    builder.button(text="✅ Проверить", callback_data=f"check_{invoice.invoice_id}")
    builder.button(text="⬅️ Профиль", callback_data="profile")
    builder.adjust(1)

    await message.answer(
        f"💵 К оплате: <b>{amount} USDT</b>",
        parse_mode="HTML",
        reply_markup=builder.as_markup(),
    )


@dp.callback_query(F.data.startswith("check_"))
async def check_payment(callback: types.CallbackQuery) -> None:
    if not crypto:
        return await callback.answer("Крипто-сервис не доступен.", show_alert=True)

    invoice_id = int(callback.data.split("_")[1])
    invoices = await crypto.get_invoices(invoice_ids=invoice_id)

    if invoices and invoices.status == "paid":
        await update_db(
            callback.from_user.id,
            balance_change=float(invoices.amount),
        )
        await callback.answer("✅ Баланс пополнен!", show_alert=True)
        await show_profile(callback)
    else:
        await callback.answer("⚠️ Оплата не найдена.", show_alert=True)


# --- АВТОМАТИЧЕСКИЙ ВЫВОД (ЧЕК) ---
@dp.callback_query(F.data == "withdraw")
async def withdraw_start(callback: types.CallbackQuery, state: FSMContext) -> None:
    user = await get_user_data(callback.from_user.id)
    if callback.from_user.id != ADMIN_ID and user.get("is_blocked"):
        return await callback.answer("❌ Вы заблокированы.", show_alert=True)
    if user["balance"] < 0.1:
        return await callback.answer("❌ Минимум 0.1 USDT", show_alert=True)

    await callback.message.edit_text(
        f"📤 Вывод средств\nДоступно: {user['balance']:.2f} USDT\nВведите сумму:",
    )
    await state.set_state(Form.waiting_for_withdraw_amount)


@dp.message(Form.waiting_for_withdraw_amount)
async def process_withdraw_auto(message: types.Message, state: FSMContext) -> None:
    user = await get_user_data(message.from_user.id)

    try:
        amount = float(message.text.replace(",", "."))
        if amount > user["balance"] or amount < 0.1:
            raise ValueError("Неверная сумма")
    except Exception:
        return await message.answer("❌ Ошибка. Введите корректную сумму.")

    if not crypto:
        return await message.answer("❌ Крипто-сервис ещё не инициализирован.")

    check = await crypto.create_check(asset="USDT", amount=amount)
    await update_db(message.from_user.id, balance_change=-amount)
    await state.clear()

    builder = InlineKeyboardBuilder()
    builder.button(text="🎁 Забрать USDT", url=check.bot_check_url)
    builder.button(text="⬅️ Профиль", callback_data="profile")
    builder.adjust(1)

    await message.answer(
        f"✅ Чек на {amount} USDT создан!",
        reply_markup=builder.as_markup(),
    )


# --- ЗАПУСК ---
async def main() -> None:
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logger.info("Запуск бота...")

    if not BOT_TOKEN or not str(BOT_TOKEN).strip():
        raise RuntimeError(
            "Задайте переменную окружения BOT_TOKEN (токен Telegram-бота)."
        )
    if not CRYPTO_BOT_TOKEN or not str(CRYPTO_BOT_TOKEN).strip():
        logger.warning(
            "CRYPTO_BOT_TOKEN пуст: пополнение и вывод через Crypto Pay будут недоступны."
        )

    global bot, crypto
    bot = Bot(token=BOT_TOKEN)

    await init_db()

    crypto = (
        AioCryptoPay(token=CRYPTO_BOT_TOKEN, network=Networks.MAIN_NET)
        if CRYPTO_BOT_TOKEN
        else None
    )

    try:
        await dp.start_polling(bot)
    finally:
        if crypto:
            await crypto.close()
        if bot:
            await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот выключен")

