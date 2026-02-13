import pprint
import re
import logging
from typing import List, Dict, Optional, Tuple

from aiogram import Router, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message

from app.services.utils import contains_restricted_emails
from config import Config
from app.services.fsm import state_manager
from app.services.forms import is_form
from app.db.table_data import fetch_table

from telegram.handlers.handler_form import process_form
from telegram.utils import check_access
from telegram.content import prepare_telegram_message, download_and_send_file, process_content_part


router = Router()
logger = logging.getLogger(__name__)

async def handle_table_menu(table_id: str, user_id: str, message: Message = None):
    """
    Обрабатывает данные таблицы и создает Telegram-сообщение с меню или формой
    """
    logger.info(f"Начало обработки меню для table_id={table_id}")

    table_data = await fetch_table(table_id, app='HR')

    if not table_data:
        logger.warning(f"Не удалось загрузить данные для table_id={table_id}")
        return {"text": "Не удалось загрузить данные"}, None

    # Ветвление — обрабатывается форма или обычное меню
    if is_form(table_data):

        logger.info(f"Таблица {table_id} идентифицирована как форма")
        if message:
            return await process_form(table_data, message)
        else:
            logger.error(f"Ошибка инициализации формы")
            return {"text": "Ошибка инициализации формы"}, None

    else:
        # Логика обработки меню
        logger.info(f"Таблица {table_id} - обычное меню")

        content_part = await process_content_part(table_data)

        keyboard = await create_menu_keyboard(table_data, table_id, user_id=user_id)

        if 'parse_mode' not in content_part:
            content_part['parse_mode'] = 'HTML'

        # Если контента нет - возвращаем пустой текст
        if not content_part.get('text') and not content_part.get('image_url'):
            content_part['text'] = ''

        return content_part, keyboard


async def create_menu_keyboard(table_data: List[Dict], current_table_id: str, user_id: str) -> InlineKeyboardMarkup:
    """Создает инлайн-клавиатуру с кнопками для NocoDB"""
    inline_keyboard = []

    for row in table_data:
        section = row.get('Section')

        # Пропускаем Info-секцию (это контент меню, не кнопка)
        if not section or section == 'Info':
            continue

        # ПРИОРИТЕТ 1: Подменю (Submenu_id)
        if row.get('Submenu_id'):
            # Проверяем, это справочник сотрудников (PIVOT_TABLE_ID) или обычное меню
            if row.get('Submenu_id') == Config.PIVOT_TABLE_ID:
                inline_keyboard.append([InlineKeyboardButton(
                    text=section,  # Используем Section как текст кнопки
                    callback_data=f"contacts:{row['Submenu_id']}"
                )])
            else:
                inline_keyboard.append([InlineKeyboardButton(
                    text=section,  # Используем Section как текст кнопки
                    callback_data=f"menu:{row['Submenu_id']}"
                )])

        # ПРИОРИТЕТ 2: Внешняя ссылка (External_link)
        elif row.get('External_link'):
            inline_keyboard.append([InlineKeyboardButton(
                text=section,  # Используем Section как текст кнопки
                url=row['External_link']
            )])

        # ПРИОРИТЕТ 3: Контентная кнопка (Content_text или Content_image)
        elif row.get('Content_text') or row.get('Content_image'):
            inline_keyboard.append([InlineKeyboardButton(
                text=section,  # Используем Section как текст кнопки
                callback_data=f"content:{current_table_id}:{row['Id']}"
            )])

    # Добавляем кнопку "Назад" только если это не главное меню
    if current_table_id not in [Config.MAIN_MENU_NEWCOMER_ID, Config.MAIN_MENU_EMPLOYEE_ID]:
        inline_keyboard.append([InlineKeyboardButton(
            text="⬅️ Назад",
            callback_data="back"
        )])

    return InlineKeyboardMarkup(inline_keyboard=inline_keyboard)


# Хендлер для кнопок меню
@router.callback_query(lambda c: c.data.startswith('menu:'))
async def process_menu_callback(callback_query: types.CallbackQuery):
    """Обработчик перехода между меню"""
    try:
        user_id = callback_query.from_user.id

        # Проверяем права доступа и выходим если нет доступа
        has_access = await check_access(callback_query=callback_query)
        if not has_access:
            return

        # Получаем и обновляем состояние
        new_table_id = callback_query.data.split(':')[1]
        print(new_table_id)
        await state_manager.navigate_to_menu(user_id, new_table_id)

        # Получаем данные меню
        state = await state_manager.get_current_menu(user_id)
        print(state)
        content, keyboard = await handle_table_menu(
            new_table_id,
            str(user_id),
            message=callback_query.message,
        )

        # Удаляем предыдущее сообщение и создаем новое
        try:
            await callback_query.message.delete()
        except:
            pass

        # Отправляем новое сообщение с учетом типа контента
        kwargs = {
            'reply_markup': keyboard,
            'parse_mode': 'HTML'
        }

        if content:
            if content.get('image_url'):
                await callback_query.message.answer_photo(
                    photo=content['image_url'],
                    caption=content.get('text', ' '),
                    **kwargs
                )
            elif content.get('text'):
                await callback_query.message.answer(
                    text=content['text'],
                    **kwargs
                )

        await callback_query.answer()

    except Exception as e:
        logger.error(f"Menu navigation error: {str(e)}", exc_info=True)
        await callback_query.answer("Ошибка навигации", show_alert=True)


@router.callback_query(lambda c: c.data.startswith('content:'))
async def process_content_callback(callback_query: types.CallbackQuery):
    """Обработчик контентных кнопок (постит в чат)"""
    try:
        user_id = callback_query.from_user.id

        # Проверяем права доступа и выходим если нет доступа
        has_access = await check_access(callback_query=callback_query)
        if not has_access:
            return

        # Получаем параметры контента
        _, table_id, row_id = callback_query.data.split(':')
        content_key = f"content:{table_id}:{row_id}"

        # Обновляем историю
        await state_manager.navigate_to_menu(user_id, content_key)

        # Получаем данные контента
        table_data = await fetch_table(table_id=table_id, app="HR")
        row = next((r for r in table_data if r['_id'] == row_id), None)

        if not row:
            await callback_query.answer("Контент не найден", show_alert=True)
            return

        # Удаляем предыдущее меню
        try:
            await callback_query.message.delete()
        except:
            pass

        # Отправляем вложение (если есть)
        if row.get('Attachment'):
            await download_and_send_file(
                file_url=row['Attachment'],
                callback_query=callback_query
            )

        # Отправляем основной контент
        content, keyboard = await handle_content_button(table_id, row_id)

        if content.get('image_url'):
            await callback_query.message.answer_photo(
                photo=content['image_url'],
                caption=content.get('text', "Информация"),  # Гарантированный текст
                reply_markup=keyboard,
                parse_mode="HTML"
            )
        else:
            await callback_query.message.answer(
                text=content.get('text', "Информация"),
                reply_markup=keyboard,
                parse_mode="HTML"
            )

        await callback_query.answer()

    except Exception as e:
        logger.error(f"Content error: {str(e)}", exc_info=True)
        await callback_query.answer("Ошибка загрузки контента", show_alert=True)


async def handle_content_button(table_id: str, row_id: str, should_post_back: bool = True) -> Tuple[Dict, Optional[InlineKeyboardMarkup]]:
    """
    Обрабатывает нажатие на кнопку контента
    :return: Кортеж (контент, клавиатура "Назад")
    """
    logger.info(f"Обработка контента для table_id={table_id}, row_id={row_id}")

    table_data = await fetch_table(table_id, app="HR")
    if not table_data:
        logger.error(f"Ошибка загрузки данных таблицы {table_id}")
        return {"text": "Ошибка загрузки контента"}, None

    row = next((r for r in table_data if r['_id'] == row_id), None)
    if not row:
        logger.error(f"Строка с row_id={row_id} не найдена в таблице {table_id}")
        return {"text": "Контент не найден"}, None

    logger.info(f"Найдена строка контента: {row.get('Name', 'Без названия')}")

    # Подготавливаем контент
    content = {}
    if row.get('Button_content'):
        content.update(prepare_telegram_message(row['Button_content']))
        logger.info("Контент подготовлен")

        # Проверяем наличие email с доменами .ru
        if not should_post_back:
            content_text = content.get('text', '')
            if contains_restricted_emails(content_text):
                # Если содержит персональные данные и это возврат назад - не постим
                logger.info("Контент содержит персональные данные, не постим в чат при возврате назад")
                content = {'text': ''}  # Пустой контент

    # Создаем клавиатуру "Назад" (теперь без параметров)
    keyboard = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="⬅️ Назад",
            callback_data="back"  # Убрали параметры
        )
    ]])
    logger.info("Создана клавиатура 'Назад'")

    return content, keyboard