import logging
from typing import List, Dict
import re

from aiogram import F, Router
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from app.db.table_data import fetch_table
from config import Config
from app.services.broadcast import is_user_admin
from telegram.handlers.handler_base import start_navigation
from telegram.content import prepare_telegram_message

logger = logging.getLogger(__name__)

router = Router()


class ExitPulseStates(StatesGroup):
    waiting_for_name_search = State()
    waiting_for_confirmation = State()


def normalize_search_query(query: str) -> List[str]:
    """Нормализует поисковый запрос"""
    return re.split(r"\s+", query.strip().lower())


async def search_users_by_fio(search_query: str) -> List[Dict]:
    """Ищет пользователей по ФИО в таблице пользователей"""
    try:
        # Получаем данные пользователей
        users = await fetch_table(
            table_id=Config.AUTH_TABLE_ID,
            app='USER'
        )

        if not users:
            return []

        # Нормализуем запрос
        query_words = normalize_search_query(search_query)
        if not query_words:
            return []

        results = []

        for user in users:
            fio = user.get('FIO', '').lower()
            if not fio:
                continue

            # Для одного слова
            if len(query_words) == 1:
                if query_words[0] in fio:
                    results.append(user)

            # Для двух слов (имя + фамилия в любом порядке)
            elif len(query_words) >= 2:
                w1, w2 = query_words[0], query_words[1]
                if f"{w1} {w2}" in fio or f"{w2} {w1}" in fio:
                    results.append(user)

        logger.info(f"По запросу '{search_query}' найдено {len(results)} пользователей")
        return results

    except Exception as e:
        logger.error(f"Ошибка поиска пользователей: {e}")
        return []


async def get_leaving_poll_content() -> Dict:
    """Получает контент опроса при увольнении"""
    try:
        content_items = await fetch_table(
            table_id=Config.PULSE_CONTENT_ID,
            app='PULSE'
        )

        if not content_items:
            return {}

        # Ищем опрос с типом 'leaving'
        for item in content_items:
            if item.get('Type') == 'leaving':
                return item

        return {}

    except Exception as e:
        logger.error(f"Ошибка получения контента опроса: {e}")
        return {}


async def send_leaving_poll(user_id: int, fio: str, bot) -> bool:
    """Отправляет пульс-опрос при увольнении"""
    try:
        # Получаем контент опроса
        poll_content = await get_leaving_poll_content()
        if not poll_content:
            logger.error("Контент опроса 'leaving' не найден")
            return False

        # Подготавливаем контент - теперь передаем оба параметра
        content_text = poll_content.get('Content_text', '')
        content_image = poll_content.get('Content_image')
        prepared_content = prepare_telegram_message(content_text, content_image)

        # Отправляем сообщение (остается без изменений)
        if prepared_content.get('image_url'):
            await bot.send_photo(
                chat_id=int(user_id),
                photo=prepared_content['image_url'],
                caption=prepared_content.get('text', ''),
                parse_mode="HTML"
            )
        elif prepared_content.get('text'):
            await bot.send_message(
                chat_id=int(user_id),
                text=prepared_content['text'],
                parse_mode="HTML"
            )
        else:
            logger.error("Пустой контент для опроса 'leaving'")
            return False

        logger.info(f"Пульс-опрос при увольнении отправлен пользователю {user_id} ({fio})")
        return True

    except Exception as send_error:
        error_msg = str(send_error).lower()
        if "forbidden" in error_msg and ("bot was blocked" in error_msg or "bot blocked" in error_msg):
            logger.warning(f"Пользователь {user_id} заблокировал бота")
            return False
        elif "chat not found" in error_msg:
            logger.warning(f"Чат не найден для пользователя {user_id}")
            return False
        else:
            logger.error(f"Ошибка отправки пользователю {user_id}: {send_error}")
            return False


@router.message(F.text == "/send_exit_pulse")
async def handle_exit_pulse_start(message: Message, state: FSMContext):
    """Начинает процесс отправки пульс-опроса при увольнении"""
    user_id = message.from_user.id

    if not await is_user_admin(user_id):
        await message.answer("❌ У вас нет прав для выполнения этой команды")
        return

    # Клавиатура с отменой
    cancel_keyboard = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="❌ Отмена", callback_data="exit_pulse_cancel")
    ]])

    await state.set_state(ExitPulseStates.waiting_for_name_search)
    await message.answer(
        "Кому отправить опрос? Укажите, пожалуйста, фамилию и/или полное имя сотрудника, "
        "например: Иван Соколов или Соколов Иван, или Соколов, или просто Иван.",
        reply_markup=cancel_keyboard
    )


@router.message(ExitPulseStates.waiting_for_name_search)
async def handle_name_search(message: Message, state: FSMContext):
    """Обрабатывает поиск сотрудника по имени"""
    search_query = message.text.strip()

    if not search_query:
        await message.answer("Пожалуйста, введите ФИО сотрудника:")
        return

    # Ищем пользователей
    found_users = await search_users_by_fio(search_query)

    if not found_users:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="❌ Отмена", callback_data="exit_pulse_cancel")
        ]])
        await message.answer(
            "К сожалению, ничего не нашли. Попробуйте другой запрос:",
            reply_markup=keyboard
        )
        return

    # Сохраняем найденных пользователей в state с индексами
    user_dict = {}
    inline_keyboard = []

    for i, user in enumerate(found_users):
        fio = user.get('FIO', 'Неизвестный')
        messenger_id = user.get('ID_messenger')

        if messenger_id:
            # Используем индекс как ключ
            user_dict[str(i)] = {
                'messenger_id': messenger_id,
                'fio': fio
            }

            inline_keyboard.append([InlineKeyboardButton(
                text=fio,
                callback_data=f"exit_pulse_select:{i}"  # Только индекс
            )])

    # Сохраняем словарь пользователей в state
    await state.update_data(users_dict=user_dict)

    # Добавляем кнопку отмены
    inline_keyboard.append([
        InlineKeyboardButton(text="❌ Отмена", callback_data="exit_pulse_cancel")
    ])

    keyboard = InlineKeyboardMarkup(inline_keyboard=inline_keyboard)

    await state.set_state(ExitPulseStates.waiting_for_confirmation)

    await message.answer(
        "По вашему запросу найдены сотрудники:",
        reply_markup=keyboard
    )


@router.callback_query(F.data.startswith("exit_pulse_select:"))
async def handle_user_selection(callback: CallbackQuery, state: FSMContext):
    """Обрабатывает выбор сотрудника"""
    user_index = callback.data.split(":")[1]

    user_data = await state.get_data()
    users_dict = user_data.get('users_dict', {})

    if user_index not in users_dict:
        await callback.answer("Ошибка: пользователь не найден", show_alert=True)
        return

    selected_user = users_dict[user_index]
    messenger_id = selected_user['messenger_id']
    fio = selected_user['fio']

    await state.update_data(
        selected_messenger_id=messenger_id,
        selected_fio=fio,
        selected_index=user_index
    )

    # Создаем клавиатуру для подтверждения
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Отправить", callback_data="exit_pulse_confirm"),
            InlineKeyboardButton(text="❌ Отмена", callback_data="exit_pulse_cancel")
        ]
    ])

    await callback.message.edit_text(
        f"Отправить пульс-опрос при увольнении сотруднику {fio}?",
        reply_markup=keyboard
    )
    await callback.answer()


@router.callback_query(F.data == "exit_pulse_confirm")
async def handle_pulse_confirmation(callback: CallbackQuery, state: FSMContext, bot):
    """Подтверждает и отправляет пульс-опрос"""
    user_data = await state.get_data()
    messenger_id = user_data.get('selected_messenger_id')
    fio = user_data.get('selected_fio')

    if not messenger_id:
        await callback.message.edit_text("Ошибка: не выбран сотрудник")
        await state.clear()
        return

    # Отправляем опрос
    success = await send_leaving_poll(int(messenger_id), fio, bot)

    # Кнопка возврата в меню
    menu_keyboard = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="⬅️ В главное меню", callback_data="exit_pulse_back_to_menu")
    ]])

    if success:
        await callback.message.edit_text("Опрос успешно отправлен.", reply_markup=menu_keyboard)
    else:
        await callback.message.edit_text(
            f"Пульс-опрос не отправлен сотруднику {fio}. Вероятно, он (она) остановил бота. Свяжитесь, пожалуйста, с сотрудником по почте.",
            reply_markup=menu_keyboard
        )

    await state.clear()
    await callback.answer()


@router.callback_query(F.data == "exit_pulse_back_to_menu")
async def handle_back_to_menu(callback: CallbackQuery):
    """Возвращает в главное меню"""
    try:
        await start_navigation(message=callback.message)
        await callback.answer()

    except Exception as e:
        logger.error(f"Ошибка обработки кнопки возврата в меню: {str(e)}")
        await callback.answer("Ошибка возврата в меню", show_alert=True)


@router.callback_query(F.data == "exit_pulse_cancel")
async def handle_pulse_cancel(callback: CallbackQuery, state: FSMContext):
    """Отменяет отправку пульс-опроса"""
    await state.clear()

    # Кнопка возврата в меню
    menu_keyboard = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="⬅️ В главное меню", callback_data="exit_pulse_back_to_menu")
    ]])

    await callback.message.edit_text("Отправка пульс-опроса отменена.", reply_markup=menu_keyboard)
    await callback.answer()
