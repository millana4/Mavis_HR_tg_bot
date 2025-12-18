import asyncio
import logging
from typing import List, Dict

from aiogram import Router, types, F, Bot
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton

from config import Config
from app.services.fsm import state_manager, AppStates
from app.db.ats import give_employee_data, format_employee_text
from app.seatable_api.api_ats import get_employees, get_department_list

from telegram.handlers.filters import NameSearchFilter, SearchTypeFilter
from telegram.keyboards import SEARCH_TYPE_KEYBOARD
from telegram.utils import check_access


router = Router()
logger = logging.getLogger(__name__)

# Таймер, чтобы удалить на клиенте из истории имена сотрудников
AUTODELETE_TIMER = 3600


# Хендлер для кнопки со справочником сотрудников
@router.callback_query(lambda c: c.data.startswith('ats:'))
async def process_ats_callback(callback_query: types.CallbackQuery):
    """Обрабатывает нажатие на кнопку со справочником"""
    try:
        user_id = callback_query.from_user.id

        # Получаем и обновляем состояние
        ats_tag = callback_query.data.split(':')[1]
        await state_manager.navigate_to_menu(user_id, ats_tag)

        # Удаляем предыдущее сообщение с меню
        try:
            await callback_query.message.delete()
        except:
            pass

        # Получаем главное меню для текущей роли пользователя
        main_menu_id = await state_manager.get_main_menu_id(user_id)

        # Получаем данные главного меню
        from app.seatable_api.api_base import fetch_table
        main_menu_data = await fetch_table(main_menu_id)

        # Ищем строку со справочником
        ats_button_content = None
        ats_button_image_url = None

        for row in main_menu_data:
            name = row.get('Name')
            submenu_link = row.get('Submenu_link')

            if name and submenu_link:
                # Проверяем, ведет ли ссылка на справочник сотрудников
                if Config.SEATABLE_EMPLOYEE_BOOK_ID in submenu_link:
                    # Нашли нужную кнопку справочника
                    content_value = row.get('Content')
                    if content_value:
                        # Парсим контент для извлечения изображения
                        from telegram.content import prepare_telegram_message
                        prepared_content = prepare_telegram_message(content_value)
                        ats_button_image_url = prepared_content.get('image_url')
                        ats_button_content = prepared_content.get('text', '')
                    break

        # Отправляем сообщение с иллюстрацией (если есть)
        if ats_button_image_url:
            # Если есть изображение - отправляем фото с описанием
            await callback_query.message.answer_photo(
                photo=ats_button_image_url,
                caption="Как вы хотите найти сотрудника?",
                reply_markup=SEARCH_TYPE_KEYBOARD,
                parse_mode='HTML'
            )
        elif ats_button_content:
            # Если есть только текст (без изображения)
            await callback_query.message.answer(
                text=f"{ats_button_content}\n\nКак вы хотите найти сотрудника?",
                reply_markup=SEARCH_TYPE_KEYBOARD,
                parse_mode='HTML'
            )
        else:
            # Если нет ни изображения, ни текста - отправляем просто текст
            await callback_query.message.answer(
                "Как вы хотите найти сотрудника?",
                reply_markup=SEARCH_TYPE_KEYBOARD
            )

        # Устанавливаем состояние ожидания выбора типа поиска
        await state_manager.update_data(user_id, current_state=AppStates.WAITING_FOR_SEARCH_TYPE)
        logger.info(f"Установлено состояние: {AppStates.WAITING_FOR_SEARCH_TYPE}")

        await callback_query.answer()

    except Exception as e:
        logger.error(f"ATS callback error: {str(e)}", exc_info=True)
        await callback_query.answer("Ошибка открытия справочника", show_alert=True)


# Обработчик текстового ввода в состоянии выбора типа поиска — запускает поиск по ФИО
@router.message(F.text, F.content_type == 'text', SearchTypeFilter())
async def handle_text_input_during_search_selection(message: Message):
    """
    Обрабатывает неожидаемое действие пользователя.
    Когда бот ждет выбора типа поиска, пользователь не выбирает тип, а сразу вводит имя.
    Тогда эта функция автоматически запускает поиск по ФИО.
    """
    try:
        user_id = message.from_user.id

        # Игнорировать команды меню
        if message.text.startswith('/'):
            return

        # Убедимся, что пользователь находится в контексте справочника
        user_data = await state_manager.get_data(user_id)
        current_menu = user_data.get('current_menu')

        # Проверяем, что текущее меню - это справочник сотрудников
        if current_menu != Config.SEATABLE_EMPLOYEE_BOOK_ID:
            return  # Игнорируем, если не в справочнике

        # Автоматически запускаем поиск по ФИО
        search_query = message.text.strip()

        if not search_query:
            await message.answer("Пожалуйста, введите ФИО сотрудника:")
            return

        logger.info(f"Автоматический поиск по ФИО: {search_query}")

        # Получаем данные сотрудников
        employees = await get_employees()

        # Выполняем поиск
        searched_employees = await give_employee_data("Name/Department", search_query, employees)

        # Показываем результаты
        await show_employee(searched_employees, message)

    except Exception as e:
        logger.error(f"Auto name search error: {str(e)}", exc_info=True)
        await message.answer("Ошибка при обработке запроса")


# Обработчик выбора "Искать по ФИО"
@router.callback_query(lambda c: c.data == "search_by_name")
async def handle_name_search(callback_query: types.CallbackQuery):
    """Обрабатывает выбор поиска по ФИО"""
    try:
        user_id = callback_query.from_user.id

        # Убираем инлайн-клавиатуру
        await callback_query.message.edit_reply_markup(reply_markup=None)

        # Просим ввести ФИО
        await callback_query.message.answer(
            "Укажите, пожалуйста, фамилию и/или полное имя сотрудника, например: Иван Соколов или Соколов Иван, "
            "или Соколов, или просто Иван."
        )

        # Устанавливаем состояние ожидания ввода ФИО
        await state_manager.update_data(user_id, current_state=AppStates.WAITING_FOR_NAME_SEARCH)
        logger.info(f"Установлено состояние: {AppStates.WAITING_FOR_NAME_SEARCH}")

        await callback_query.answer()

    except Exception as e:
        logger.error(f"Ошибка поиска сотрудника по ФИО: {str(e)}", exc_info=True)
        await callback_query.answer("Ошибка при выборе поиска по ФИО")


# Обработчик ввода ФИО
@router.message(F.text, F.content_type == 'text', NameSearchFilter())
async def process_name_input(message: Message):
    """Обрабатывает ввод ФИО для поиска"""
    try:
        user_id = message.from_user.id

        # Проверяем, что пользователь в правильном состоянии
        user_data = await state_manager.get_data(user_id)
        if user_data.get('current_state') != AppStates.WAITING_FOR_NAME_SEARCH:
            return

        search_query = message.text.strip()

        # Если пустой запрос
        if not search_query:
            await message.answer("Пожалуйста, введите ФИО сотрудника:")
            return

        logger.info(f"Поиск по ФИО: {search_query}")

        # Обращается по АПИ в таблицу со справочником и возвращает json с данными всех сотрудников
        employees = await get_employees()

        # После поиска показываем результаты и кнопку Назад
        searched_employees = await give_employee_data("Name/Department", search_query, employees)

        # Выводит сообщение с результатами поиска и показывает его, пока пользователь не нажмет Назад
        await show_employee(searched_employees, message)

    except Exception as e:
        logger.error(f"Name input processing error: {str(e)}", exc_info=True)
        await message.answer("Ошибка при обработке запроса")


# Обработчик выбора "Искать по отделу"
@router.callback_query(lambda c: c.data == "search_by_department")
async def handle_department_search(callback_query: types.CallbackQuery):
    """Обрабатывает выбор поиска по отделу"""
    try:
        user_id = callback_query.from_user.id

        # Проверяем права доступа и выходим если нет доступа
        has_access = await check_access(callback_query=callback_query)
        if not has_access:
            return

        user_data = await state_manager.get_data(user_id)
        if user_data.get('current_state') != AppStates.WAITING_FOR_SEARCH_TYPE:
            return

        # Убираем инлайн-клавиатуру типа поиска
        await callback_query.message.edit_reply_markup(reply_markup=None)

        # Создаём инлайн-клавиатуру с отделами
        keyboard = await create_department_keyboard()

        # Устанавливаем состояние ожидания ввода отдела
        await state_manager.update_data(user_id, current_state=AppStates.WAITING_FOR_DEPARTMENT_SEARCH)
        logger.info(f"Установлено состояние: {AppStates.WAITING_FOR_DEPARTMENT_SEARCH}")

        # Отправляем инлайн-клавиатуру пользователю
        await callback_query.message.answer("Выберите, пожалуйста, отдел:", reply_markup=keyboard)

        await callback_query.answer()

    except Exception as e:
        logger.error(f"Department search callback error: {str(e)}", exc_info=True)
        await callback_query.answer("Ошибка при выборе поиска по отделу", show_alert=True)


async def create_department_keyboard() -> InlineKeyboardMarkup:
    """
    Создает клавиатуру со списком доступных отделов, по которым можно получить телефоны.
    Кнопки выводятся по 2 в строку.
    """
    # Получаем из справочника список отделов
    department_list = await get_department_list()

    inline_keyboard = []

    # Группируем по 2 кнопки в ряд
    row = []
    for i, department in enumerate(department_list, start=1):
        row.append(InlineKeyboardButton(
            text=department,
            callback_data=f"department:{department}"
        ))
        if i % 2 == 0:  # каждые 2 кнопки — новая строка
            inline_keyboard.append(row)
            row = []

    # если осталось "хвостиком" одна кнопка — добавляем её в отдельной строке
    if row:
        inline_keyboard.append(row)

    # Добавляем кнопку "Назад"
    inline_keyboard.append([InlineKeyboardButton(
        text="⬅️ Назад",
        callback_data="back"
    )])

    return InlineKeyboardMarkup(inline_keyboard=inline_keyboard)


# Обработчик ввода отдела
@router.callback_query(lambda c: c.data.startswith('department:'))
async def process_department_input(callback_query: types.CallbackQuery):
    """Обрабатывает ввод отдела для поиска"""
    try:
        user_id = callback_query.from_user.id

        # Проверяем, что пользователь в правильном состоянии
        user_data = await state_manager.get_data(user_id)
        if user_data.get('current_state') != AppStates.WAITING_FOR_DEPARTMENT_SEARCH:
            return

        # Убираем "часики" на кнопке
        await callback_query.answer()

        # Извлекаем выбранное значение (без префикса department:)
        search_query = callback_query.data.replace("department:", "")
        logger.info(f"Поиск телефонов по отделу: {search_query}")

        # Убираем инлайн-клавиатуру с отделами
        await callback_query.message.edit_reply_markup(reply_markup=None)

        # Получаем данные сотрудников
        employees = await get_employees()

        # Фильтруем по отделу
        searched_employees = await give_employee_data("Department", search_query, employees)

        # Показываем результат поиска
        await show_employee(searched_employees, callback_query.message)

    except Exception as e:
        logger.error(f"Department input processing error: {str(e)}", exc_info=True)
        await callback_query.message.answer("Ошибка при обработке запроса телефонов отдела")


async def show_employee(searched_employees: List[Dict], message: Message):
    """
    Формирует сообщение с результатами поиска сотрудников и выводит его в чат.
    """
    user_id = message.from_user.id
    chat_id = message.chat.id
    bot = message.bot
    sent_message = None

    # Проверяем права доступа и выходим если нет доступа
    has_access = await check_access(message=message)
    if not has_access:
        return

    # Если ничего не нашли
    if not searched_employees:
        # Возвращаем к выбору типа поиска
        sent_message = await message.answer(
            "К сожалению, ничего не нашли. Попробуйте другой запрос или выберите другой способ поиска:",
            reply_markup=SEARCH_TYPE_KEYBOARD
        )
        await state_manager.update_data(user_id, current_state=AppStates.WAITING_FOR_SEARCH_TYPE)
        return

    text_blocks = []

    # Если один результат и есть фото
    if len(searched_employees) == 1:
        emp = searched_employees[0]
        photo_urls = emp.get("Photo", [])
        text = format_employee_text(emp)

        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="search_back")]]
        )

        if photo_urls:
            sent_message = await message.answer_photo(
                photo=photo_urls[0],
                caption=text,
                parse_mode="HTML",
                reply_markup=keyboard
            )
        else:
            sent_message = await message.answer(
                text,
                parse_mode="HTML",
                reply_markup=keyboard
            )

    else:
        # Несколько сотрудников — фото не показываем
        for emp in searched_employees:
            text_blocks.append(format_employee_text(emp))

        full_text = "\n\n".join(text_blocks)

        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="search_back")]]
        )

        sent_message = await message.answer(
            full_text,
            parse_mode="HTML",
            reply_markup=keyboard
        )

    # Ставим таймер, чтобы удалить из истории данные сотрудников
    if sent_message and searched_employees:
        logger.info(f"Вызываю таймер для сообщения {sent_message.message_id}")
        asyncio.create_task(delete_personal_data(bot, chat_id, sent_message.message_id, AUTODELETE_TIMER))
    else:
        logger.warning(
            f"Ошибка: таймер НЕ установлен: sent_message={sent_message is not None}, searched_employees={len(searched_employees) if searched_employees else 0}")


# Обработчик кнопки "Назад" из результатов поиска
@router.callback_query(F.data == "search_back")
async def handle_search_back(callback: types.CallbackQuery):
    """Обрабатывает кнопку Назад из результатов поиска — возвращает к выбору типа поиска"""
    try:
        user_id = callback.from_user.id

        logger.info(f"Сработал «Назад» к типу поиска handle_search_back")

        # Проверяем права доступа и выходим если нет доступа
        has_access = await check_access(callback_query=callback)
        if not has_access:
            return

        # Удаляем сообщение с результатами
        try:
            await callback.message.delete()
        except:
            pass

        # Возвращаем к выбору типа поиска
        await callback.message.answer(
            "Как вы хотите найти сотрудника?",
            reply_markup=SEARCH_TYPE_KEYBOARD
        )

        # Устанавливаем состояние ожидания выбора типа поиска
        await state_manager.update_data(user_id, current_state=AppStates.WAITING_FOR_SEARCH_TYPE)

        await callback.answer()

    except Exception as e:
        logger.error(f"Search back (inline) error: {str(e)}", exc_info=True)
        await callback.answer("Ошибка при возврате", show_alert=True)


async def delete_personal_data(bot: Bot, chat_id: int, message_id: int, delay_seconds: int):
    """Удаляет сообщение через указанное количество секунд"""
    try:
        logger.info(f"Стартует таймер {delay_seconds} секунд для сообщения с данными сотрудников {message_id}")
        await asyncio.sleep(delay_seconds)

        # Пытаемся удалить сообщение с результатами
        try:
            await bot.delete_message(chat_id=chat_id, message_id=message_id)
            logger.info(f"По таймеру удален контент сообщения {message_id}")

            # Обновляем состояние
            await state_manager.update_data(
                chat_id,
                current_state=AppStates.WAITING_FOR_SEARCH_TYPE,
                current_menu=Config.SEATABLE_EMPLOYEE_BOOK_ID
            )

            # Отправляем сообщение с выбором типа поиска
            await bot.send_message(
                chat_id=chat_id,
                text="Как вы хотите найти сотрудника?",
                reply_markup=SEARCH_TYPE_KEYBOARD
            )

            logger.info(f"Пользователь {chat_id} возвращен к выбору типа поиска после удаления контента")

        except Exception as delete_error:
            logger.info(f"Сообщение {message_id} уже удалено: {delete_error}")

    except Exception as e:
        logger.error(f"Ошибка удаления сообщения в таймере {message_id}: {e}")