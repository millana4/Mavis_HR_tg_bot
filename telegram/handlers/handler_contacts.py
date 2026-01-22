import asyncio
import logging
from typing import List, Dict

from aiogram import Router, types, F, Bot
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton

from app.seatable_api.api_base import fetch_table
from config import Config
from app.services.fsm import state_manager, AppStates
from app.db.contacts import give_employee_data, format_employee_text, give_unit_data, format_unit_text
from app.seatable_api.api_contacts import get_employees, get_department_list

from telegram.handlers.filters import NameSearchFilter, SearchTypeFilter, ShopSearchFilter, DrugstoreSearchFilter
from telegram.keyboards import SEARCH_TYPE_KEYBOARD, SEARCH_COMPANY_GROUP, BACK_TO_SEARCH_TYPE, BACK_TO_DEPARTMENT_TYPE
from telegram.utils import check_access


router = Router()
logger = logging.getLogger(__name__)

# Таймер, чтобы удалить на клиенте из истории имена сотрудников
AUTODELETE_TIMER = 3600

# Изображения для справочника
BANNER_CONTACTS=""


# Хендлер для кнопки с контактами
@router.callback_query(lambda c: c.data.startswith('contacts:'))
async def process_contacts_callback(callback_query: types.CallbackQuery):
    """Обрабатывает нажатие на кнопку с контактами"""
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

        # Отправляем сообщение с иллюстрацией (если есть)
        if BANNER_CONTACTS != "":
            # Если есть изображение - отправляем его с описанием
            await callback_query.message.answer_photo(
                photo=BANNER_CONTACTS,
                caption="Выберите область поиска:",
                reply_markup=SEARCH_TYPE_KEYBOARD,
                parse_mode='HTML'
            )
        else:
            # Если нет изображения, отправляем просто текст
            await callback_query.message.answer(
                text="Выберите область поиска:",
                reply_markup=SEARCH_TYPE_KEYBOARD,
            )

        # Устанавливаем состояние ожидания выбора типа поиска
        await state_manager.update_data(user_id, current_state=AppStates.WAITING_FOR_SEARCH_TYPE)
        logger.info(f"Установлено состояние: {AppStates.WAITING_FOR_SEARCH_TYPE}")

        await callback_query.answer()

    except Exception as e:
        logger.error(f"Contacts callback error: {str(e)}", exc_info=True)
        await callback_query.answer("Ошибка открытия контактов", show_alert=True)


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

        # Проверяем, что текущее меню - это сводная таблица
        if current_menu != Config.SEATABLE_PIVOT_TABLE_ID:
            return

        # Автоматически запускаем поиск по ФИО
        search_query = message.text.strip()

        if not search_query:
            await message.answer("Пожалуйста, введите ФИО сотрудника:")
            return

        logger.info(f"Автоматический поиск по ФИО: {search_query}")

        # Получаем данные сотрудников
        employees = await get_employees(Config.SEATABLE_PIVOT_TABLE_ID)

        # Выполняем поиск
        searched_employees = await give_employee_data("FIO", search_query, employees)

        # Показываем результаты
        await show_employee(searched_employees, message)

    except Exception as e:
        logger.error(f"Auto name search error: {str(e)}", exc_info=True)
        await message.answer("Ошибка при обработке запроса")


# Обработчик выбора "Сотрудники"
@router.callback_query(lambda c: c.data == "search_by_name")
async def handle_name_search(callback_query: types.CallbackQuery):
    """Обрабатывает выбор поиска по ФИО"""
    try:
        user_id = callback_query.from_user.id

        # Убираем инлайн-клавиатуру
        await callback_query.message.edit_reply_markup(reply_markup=None)

        # Просим ввести ФИО
        await callback_query.message.answer(
            text="Укажите, пожалуйста, фамилию и/или полное имя сотрудника, например: Иван Соколов или Соколов Иван, "
            "или Соколов, или просто Иван.",
            reply_markup=BACK_TO_SEARCH_TYPE,
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
        employees = await get_employees(Config.SEATABLE_PIVOT_TABLE_ID)

        # После поиска показываем результаты и кнопку Назад
        searched_employees = await give_employee_data("FIO", search_query, employees)

        # Выводит сообщение с результатами поиска и показывает его, пока пользователь не нажмет Назад
        await show_employee(searched_employees, message)

    except Exception as e:
        logger.error(f"Name input processing error: {str(e)}", exc_info=True)
        await message.answer("Ошибка при обработке запроса")


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
                text=text,
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
            text=full_text,
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


# Обработчик выбора "Подразделения"
@router.callback_query(lambda c: c.data == "search_company_group")
async def handle_company_group_search(callback_query: types.CallbackQuery):
    """Обрабатывает выбор поиска по подразделениям"""
    try:
        user_id = callback_query.from_user.id

        # Удаляем предыдущее сообщение с меню
        try:
            await callback_query.message.delete()
        except:
            pass

        # Отправляем сообщение
        await callback_query.message.answer(
            text="Какие подразделения нужны?",
            reply_markup=SEARCH_COMPANY_GROUP
        )

        # Устанавливаем состояние ожидания выбора подразделения
        await state_manager.update_data(user_id, current_state=AppStates.WAITING_FOR_COMPANY_GROUP_SEARCH)
        logger.info(f"Установлено состояние: {AppStates.WAITING_FOR_COMPANY_GROUP_SEARCH}")

        await callback_query.answer()

    except Exception as e:
        logger.error(f"Contacts callback error: {str(e)}", exc_info=True)
        await callback_query.answer("Ошибка при выборе подразделения в справочнике", show_alert=True)


# Обработчик выбора "Телефоны отделов"
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
        if user_data.get('current_state') != AppStates.WAITING_FOR_COMPANY_GROUP_SEARCH:
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
        callback_data="department_back"
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
        employees = await get_employees(Config.SEATABLE_ATS_BOOK_ID)

        # Фильтруем по отделу
        searched_employees = await give_employee_data("Department", search_query, employees)

        # Показываем результат поиска
        await show_employee(searched_employees, callback_query.message)

    except Exception as e:
        logger.error(f"Department input processing error: {str(e)}", exc_info=True)
        await callback_query.message.answer("Ошибка при обработке запроса телефонов отдела")


# Обработчик выбора "Магазины «Вотоня»"
@router.callback_query(lambda c: c.data == "search_shop")
async def handle_shop_search(callback_query: types.CallbackQuery):
    """Обрабатывает выбор поиска по магазинам"""
    try:
        user_id = callback_query.from_user.id

        # Убираем инлайн-клавиатуру
        await callback_query.message.edit_reply_markup(reply_markup=None)

        # Просим ввести адрес в текстовое поле
        await callback_query.message.answer(
            text="Укажите, пожалуйста, часть адреса магазина, например: «Сертолово» или «Варшавская»",
            reply_markup=BACK_TO_DEPARTMENT_TYPE,
        )

        # Устанавливаем состояние ожидания ввода названия магазина
        await state_manager.update_data(user_id, current_state=AppStates.WAITING_FOR_SHOP_TITLE_SEARCH)
        logger.info(f"Установлено состояние: {AppStates.WAITING_FOR_SHOP_TITLE_SEARCH}")

        await callback_query.answer()

    except Exception as e:
        logger.error(f"Ошибка поиска в справочнике магазинов: {str(e)}", exc_info=True)
        await callback_query.answer("Ошибка поиска в справочнике магазинов")


# Обработчик ввода адреса магазина
@router.message(F.text, F.content_type == 'text', ShopSearchFilter())
async def process_shop_input(message: Message):
    """Обрабатывает ввод адреса для поиска"""
    try:
        user_id = message.from_user.id

        search_query = message.text.strip()

        # Если пустой запрос
        if not search_query:
            await message.answer("Пожалуйста, введите часть адреса магазина:")
            return

        logger.info(f"Поиск по магазина по адресу: {search_query}")

        # Обращается по АПИ в таблицу со справочником магазинов и возвращает список магазинов
        shops_data = await fetch_table(table_id=Config.SEATABLE_SHOP_TABLE_ID, app='USER')

        # После поиска показываем результаты и кнопку Назад
        searched_shop = await give_unit_data(search_query, shops_data)

        # Выводит сообщение с результатами поиска и показывает его, пока пользователь не нажмет Назад
        await show_unit(searched_shop, message)

    except Exception as e:
        logger.error(f"Name input processing error: {str(e)}", exc_info=True)
        await message.answer("Ошибка при обработке запроса")


# Обработчик выбора "Аптеки «Имбирь»"
@router.callback_query(lambda c: c.data == "search_drugstore")
async def handle_drugstore_search(callback_query: types.CallbackQuery):
    """Обрабатывает выбор поиска по аптекам"""
    try:
        user_id = callback_query.from_user.id

        # Убираем инлайн-клавиатуру
        await callback_query.message.edit_reply_markup(reply_markup=None)

        await callback_query.message.answer(
            text="Укажите, пожалуйста, часть адреса аптеки, например, «Савушкина»",
            reply_markup=BACK_TO_DEPARTMENT_TYPE,
        )

        # Устанавливаем состояние ожидания ввода
        await state_manager.update_data(user_id, current_state=AppStates.WAITING_FOR_DRUGSTORE_TITLE_SEARCH)
        logger.info(f"Установлено состояние: {AppStates.WAITING_FOR_DRUGSTORE_TITLE_SEARCH}")

        await callback_query.answer()

    except Exception as e:
        logger.error(f"Ошибка поиска в справочнике аптек: {str(e)}", exc_info=True)
        await callback_query.answer("Ошибка поиска в справочнике аптек")


# Обработчик ввода адреса аптеки
@router.message(F.text, F.content_type == 'text', DrugstoreSearchFilter())
async def process_drugstore_input(message: Message):
    """Обрабатывает ввод адреса для поиска"""
    try:
        user_id = message.from_user.id

        search_query = message.text.strip()

        # Если пустой запрос
        if not search_query:
            await message.answer("Пожалуйста, введите часть адреса аптеки:")
            return

        logger.info(f"Поиск по аптеки по адресу: {search_query}")

        # Обращается по АПИ в таблицу со справочником магазинов и возвращает список магазинов
        drugstore_data = await fetch_table(table_id=Config.SEATABLE_DRUGSTORE_TABLE_ID, app='USER')

        # После поиска показываем результаты и кнопку Назад
        searched_drugstore = await give_unit_data(search_query, drugstore_data)

        # Выводит сообщение с результатами поиска и показывает его, пока пользователь не нажмет Назад
        await show_unit(searched_drugstore, message)

    except Exception as e:
        logger.error(f"Name input processing error: {str(e)}", exc_info=True)
        await message.answer("Ошибка при обработке запроса")


async def show_unit(searched_unit: List[Dict], message: Message):
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
    if not searched_unit:
        # Возвращаем к выбору типа поиска
        sent_message = await message.answer(
            "К сожалению, ничего не нашли. Попробуйте другой запрос.",
            reply_markup=SEARCH_TYPE_KEYBOARD
        )
        await state_manager.update_data(user_id, current_state=AppStates.WAITING_FOR_SEARCH_TYPE)
        return

    text_blocks = []

    for unit in searched_unit:
        text_blocks.append(format_unit_text(unit))

    full_text = "\n\n".join(text_blocks)

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="search_back")]]
    )

    sent_message = await message.answer(
        text=full_text,
        parse_mode="HTML",
        reply_markup=keyboard
    )


# Обработчик кнопки "Назад", который возвращает к выбору типа поиска Сотрудники/Подразделения
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
        if BANNER_CONTACTS != "":
            # Если есть изображение - отправляем его с описанием
            await callback.message.answer_photo(
                photo=BANNER_CONTACTS,
                caption="Выберите область поиска:",
                reply_markup=SEARCH_TYPE_KEYBOARD,
                parse_mode='HTML'
            )
        else:
            # Если нет изображения, отправляем просто текст
            await callback.message.answer(
                text="Выберите область поиска:",
                reply_markup=SEARCH_TYPE_KEYBOARD,
            )

        # Устанавливаем состояние ожидания выбора типа поиска
        await state_manager.update_data(user_id, current_state=AppStates.WAITING_FOR_SEARCH_TYPE)

        await callback.answer()

    except Exception as e:
        logger.error(f"Search back (inline) error: {str(e)}", exc_info=True)
        await callback.answer("Ошибка при возврате", show_alert=True)


# Обработчик кнопки "Назад", который возвращает к выбору подразделения Телефоны отделов/Магазины/Аптеки
@router.callback_query(F.data == "department_back")
async def handle_department_back(callback: types.CallbackQuery):
    """
    Обрабатывает кнопку Назад — возвращает к выбору типа подразделения Телефоны отделов/Магазины/Аптеки.
    Кнопка нужна, если пользователь передумал искать внутри типа подразделения.
    """
    try:
        user_id = callback.from_user.id

        logger.info(f"Сработал «Назад» к типу поиска по подразделениям handle_department_back")

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
        if BANNER_CONTACTS != "":
            # Если есть изображение - отправляем его с описанием
            await callback.message.answer_photo(
                photo=BANNER_CONTACTS,
                caption="Какие подразделения нужны?",
                reply_markup=SEARCH_COMPANY_GROUP,
                parse_mode='HTML'
            )
        else:
            # Если нет изображения, отправляем просто текст
            await callback.message.answer(
                text="Какие подразделения нужны?",
                reply_markup=SEARCH_COMPANY_GROUP,
            )

        # Устанавливаем состояние ожидания выбора типа поиска
        await state_manager.update_data(user_id, current_state=AppStates.WAITING_FOR_COMPANY_GROUP_SEARCH)

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
                current_menu=Config.SEATABLE_PIVOT_TABLE_ID
            )

            # Отправляем сообщение с выбором типа поиска
            await bot.send_message(
                chat_id=chat_id,
                text="Что вы хотите найти?",
                reply_markup=SEARCH_TYPE_KEYBOARD
            )

            logger.info(f"Пользователь {chat_id} возвращен к выбору типа поиска после удаления контента")

        except Exception as delete_error:
            logger.info(f"Сообщение {message_id} уже удалено: {delete_error}")

    except Exception as e:
        logger.error(f"Ошибка удаления сообщения в таймере {message_id}: {e}")