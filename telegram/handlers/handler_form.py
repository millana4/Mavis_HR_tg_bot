import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple

from aiogram import Router, types, F
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton

from app.db.roles import UserRole
from config import Config

from app.services.forms import start_form_questions, complete_form
from app.services.fsm import state_manager, AppStates
from app.seatable_api.api_forms import fetch_table, save_form_answers

from telegram.handlers.filters import FormFilter
from telegram.utils import check_access
from telegram.content import prepare_telegram_message


router = Router()
logger = logging.getLogger(__name__)


async def process_form(table_data: List[Dict], message: Message) -> Tuple[Dict, None]:
    """Обрабатывает данные формы"""
    logger.info("Начало обработки формы обратной связи")

    # Проверяем права доступа и выходим если нет доступа
    has_access = await check_access(message=message)
    if not has_access:
        return

    info_row = next((row for row in table_data if row.get('Name') == 'Info'), None)

    if not info_row:
        logger.error("Форма не содержит строки с Name='Info'")
        return {"text": "Ошибка: форма не настроена правильно"}, None

    # Инициализируем состояние формы через state_manager
    form_data = await start_form_questions(table_data)
    await state_manager.update_data(
        message.chat.id,
        form_data=form_data,
        current_state=AppStates.FORM_DATA  # устанавливаем состояние формы
    )

    # Подготавливаем контент
    form_content = prepare_telegram_message(info_row.get('Content', ''))

    # Сначала отправляем контент Info и ждём завершения
    if form_content.get('image_url'):
        await message.answer_photo(
            photo=form_content['image_url'],
            caption=form_content.get('text', ''),
            parse_mode=form_content.get('parse_mode', 'HTML')
        )
    elif form_content.get('text'):
        await message.answer(
            text=form_content['text'],
            parse_mode=form_content.get('parse_mode', 'HTML')
        )

    # Задержка перед первым вопросом, чтобы постился после инфо
    await asyncio.sleep(0.5)
    await ask_next_question(message, form_data)

    return {"text": ""}, None  # Возвращаем пустой словарь


async def get_form_question(form_state: Dict) -> Tuple[str, Optional[InlineKeyboardMarkup]]:
    """Возвращает текущий вопрос формы и клавиатуру (если нужно)"""
    question_data = form_state['questions'][form_state['current_question']]
    question_text = question_data['Name']

    # Определяем, есть ли варианты ответа (ищем все ключи, начинающиеся на Answer_option_)
    answer_options = {
        k: v for k, v in question_data.items()
        if k.startswith('Answer_option_') and v is not None
    }

    keyboard_buttons = []

    # Если есть варианты ответа, добавляем их
    if not question_data.get('Free_input', False) and answer_options:
        for opt in answer_options.values():
            keyboard_buttons.append([InlineKeyboardButton(text=opt, callback_data=f"form_opt:{opt}")])

    keyboard_buttons.append([InlineKeyboardButton(
        text="❌ Отменить обращение",
        callback_data="form_cancel"
    )])

    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)

    # Если Free_input явно указан как True или есть варианты ответа
    if question_data.get('Free_input', False) is True or not answer_options:
        return question_text, keyboard

    return question_text, keyboard


async def ask_next_question(message: Message, form_data: Dict):
    """Задает следующий вопрос формы"""
    user_id = message.chat.id
    question_text, keyboard = await get_form_question(form_data)

    # Отправляем вопрос в чат
    if keyboard:
        sent_message = await message.answer(question_text, reply_markup=keyboard)
    else:
        sent_message = await message.answer(question_text)

    # Сохраняем ID отправленного вопроса для последующего редактирования
    form_data['last_question_message_id'] = sent_message.message_id

    await state_manager.update_data(user_id, form_data=form_data)
    return sent_message


@router.message(F.text, F.content_type == 'text', FormFilter('form_data'))
async def handle_text_answer(message: types.Message):
    """Обрабатывает текстовые ответы в форме обратной связи"""
    user_id = message.chat.id
    logger.info(f"Обрабатывается текстовый ответ пользователя {user_id} в handle_text_answer")

    # Проверяем состояние через state_manager
    user_data = await state_manager.get_data(user_id)

    # Дополнительная проверка на наличие form_data
    if 'form_data' not in user_data:
        return

    form_data = user_data['form_data'].copy()
    current_question = form_data['current_question']

    # Проверяем, ожидаем ли мы текстовый ответ
    if current_question >= len(form_data['questions']):
        return

    question_data = form_data['questions'][current_question]
    answer_options = {
        k: v for k, v in question_data.items()
        if k.startswith('Answer_option_') and v is not None
    }

    if question_data.get('Free_input', False) is False and answer_options:
        return  # Пропускаем, если это вопрос с вариантами

    # Сохраняем ответ
    form_data['answers'].append(message.text)
    form_data['current_question'] += 1

    # Обновляем данные через state_manager
    await state_manager.update_data(user_id, form_data=form_data)

    # Удаляем предыдущее сообщение с кнопками (если есть)
    if form_data.get('last_question_message_id'):
        try:
            await message.bot.edit_message_reply_markup(
                chat_id=user_id,
                message_id=form_data['last_question_message_id'],
                reply_markup=None
            )
        except:
            pass

    if form_data['current_question'] >= len(form_data['questions']):
        await finish_form(message, form_data)
        await state_manager.update_data(user_id, form_data=None, current_state=AppStates.CURRENT_MENU)
    else:
        await ask_next_question(message, form_data)


@router.callback_query(lambda c: c.data.startswith('form_opt:'))
async def handle_form_option(callback: types.CallbackQuery):
    """Обрабатывает выбор варианта в форме"""
    user_id = callback.from_user.id
    logger.info(f"Обрабатывается выбор варианта пользователя {user_id} в handle_form_option")

    # Проверяем состояние через state_manager
    user_data = await state_manager.get_data(user_id)
    if user_data.get('current_state') != AppStates.FORM_DATA or 'form_data' not in user_data:
        await callback.answer()
        return

    form_data = user_data['form_data'].copy()
    answer = callback.data.split(':', 1)[1]

    # Отправляем выбранный ответ в чат
    question_text = form_data['questions'][form_data['current_question']]['Name']
    await callback.message.answer(f"Ваш ответ: «{answer}»")

    # Сохраняем ответ
    form_data['answers'].append(answer)
    form_data['current_question'] += 1

    # Обновляем данные через state_manager
    await state_manager.update_data(user_id, form_data=form_data)

    # Удаляем клавиатуру у вопроса
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except:
        pass

    # Переходим к следующему вопросу или завершаем
    if form_data['current_question'] >= len(form_data['questions']):
        await finish_form(callback.message, form_data)
        await state_manager.update_data(user_id, form_data=None, current_state=AppStates.CURRENT_MENU)
    else:
        await ask_next_question(callback.message, form_data)
    await callback.answer()


async def finish_form(message: Message, form_data: Dict):
    """Завершает форму, сохраняет результат и показывает кнопку меню"""
    user_id = message.chat.id

    # Проверяем наличие обязательных полей
    required_fields = ['answers', 'answers_table']
    if any(field not in form_data for field in required_fields):
        logger.error(
            f"Некорректные данные формы. Отсутствуют поля: {[f for f in required_fields if f not in form_data]}")
        await message.answer("Произошла ошибка при обработке формы")
        return

    # Нормализуем answers если нужно
    if isinstance(form_data['answers'], str):
        try:
            import json
            form_data['answers'] = json.loads(form_data['answers'])
        except json.JSONDecodeError as e:
            logger.error(f"Не удалось преобразовать answers: {e}")
            await message.answer("Ошибка обработки ответов")
            return

    # Завершаем форму и сохраняем результат
    result = await complete_form(form_data, message.from_user.id)
    logger.info(f"Форма завершена: {result}")

    # Сохраняем ответы в таблицу
    save_success = await save_form_answers({
        **form_data,
        "user_id": message.chat.id  # id пользователя в телеграме, не бота
    })

    # Уведомляем администратора, что пришло новое обращение
    await notify_feedback_admins(message.bot, message.from_user.id, form_data)

    # Подготавливаем финальное сообщение
    final_text = "Спасибо за обращение!"
    parse_mode = None

    if form_data.get('final_message'):
        content = prepare_telegram_message(form_data['final_message'])
        final_text = content.get('text', final_text)
        parse_mode = content.get('parse_mode')

    # Создаем клавиатуру для возврата
    user_state = await state_manager.get_data(user_id=user_id)
    user_role = user_state.get('role')
    if user_role == UserRole.NEWCOMER.value:
        main_menu_id = Config.SEATABLE_MAIN_MENU_NEWCOMER_ID
    else:
        main_menu_id = Config.SEATABLE_MAIN_MENU_EMPLOYEE_ID

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(
                text="⬅️ В главное меню",
                callback_data=f"menu:{main_menu_id}"
            )]
        ]
    )

    # Отправляем сообщение с кнопкой
    await message.answer(
        text=final_text,
        reply_markup=keyboard,
        parse_mode=parse_mode
    )

    # Очищаем состояние формы
    await state_manager.update_data(user_id, form_data=None, current_state=AppStates.CURRENT_MENU)


async def notify_feedback_admins(bot, user_id: int, form_data: Dict):
    """Уведомляет администраторов о новом обращении через форму"""
    try:
        from app.seatable_api.api_base import fetch_table
        from config import Config
        from datetime import datetime

        admins = await fetch_table(
            table_id=Config.SEATABLE_ADMIN_TABLE_ID,
            app='USER'
        )

        if not admins:
            logger.warning("Нет данных администраторов для уведомления")
            return

        users = await fetch_table(
            table_id=Config.SEATABLE_USERS_TABLE_ID,
            app='USER'
        )

        if not users:
            logger.warning("Нет данных пользователей для уведомления")
            return

        # Получаем данные отправителя
        sender_data = None
        for user in users:
            if str(user.get('ID_messenger')) == str(user_id):
                sender_data = user
                break

        sender_name = sender_data.get('FIO', 'Неизвестный') if sender_data else 'Неизвестный'

        # Создаем маппинг: user_row_id -> telegram_id
        user_id_to_telegram = {}
        for user in users:
            user_row_id = user.get('_id')
            telegram_id = user.get('ID_messenger')
            if user_row_id and telegram_id:
                user_id_to_telegram[user_row_id] = telegram_id

        feedback_admins = []

        # Ищем админов с правами Feedback_admin
        for admin in admins:
            if admin.get('Feedback_admin') is True:
                messenger_ids = admin.get('ID_messenger', [])
                if isinstance(messenger_ids, list):
                    for user_row_id in messenger_ids:
                        telegram_id = user_id_to_telegram.get(user_row_id)
                        if telegram_id:
                            feedback_admins.append({
                                'row_id': user_row_id,
                                'telegram_id': telegram_id,
                                'fio': admin.get('FIO', 'Администратор')
                            })
                        else:
                            logger.warning(f"Не найден Telegram ID для админа с row_id: {user_row_id}")

        if not feedback_admins:
            logger.info("Нет администраторов с правами Feedback_admin для уведомления")
            return

        # ФОРМИРУЕМ СОДЕРЖАНИЕ ОБРАЩЕНИЯ
        feedback_content = []
        if form_data.get('questions') and form_data.get('answers'):
            for i, (question, answer) in enumerate(zip(form_data['questions'], form_data['answers']), 1):
                question_text = question.get('Name', f'Вопрос {i}')
                feedback_content.append(f"{question_text} — {answer}")

        feedback_text = "\n".join(feedback_content) if feedback_content else "Содержание не доступно"

        # Формируем сообщение
        message_text = (
            f"📩 Поступило новое обращение через форму обратной связи в HR-боте\n\n"
            f"<b>Отправитель:</b> {sender_name}\n"
            f"<b>Время отправки:</b> {datetime.now().strftime('%d.%m.%Y %H:%M')}\n\n"
            f"<b>Содержание обращения:</b>\n{feedback_text}\n\n"
            f"Подробности в таблице «ОС ответы_ДН»"
        )

        # Отправляем сообщение каждому админу
        for admin in feedback_admins:
            telegram_id = admin.get('telegram_id')
            if telegram_id:
                try:
                    await bot.send_message(
                        chat_id=int(telegram_id),
                        text=message_text,
                        parse_mode="HTML"
                    )
                    logger.info(f"Уведомление отправлено админу {admin.get('fio')} (ID: {telegram_id})")
                except Exception as e:
                    logger.error(f"Ошибка отправки уведомления админу {telegram_id}: {e}")
            else:
                logger.warning(f"У админа {admin.get('row_id')} нет Telegram ID")

    except Exception as e:
        logger.error(f"Ошибка при уведомлении администраторов: {e}")

@router.callback_query(F.data == "form_cancel")
async def handle_form_cancel(callback: types.CallbackQuery):
    """Обрабатывает отмену формы"""
    user_id = callback.from_user.id
    logger.info(f"Пользователь {user_id} отменил форму")

    # Получаем данные пользователя
    user_data = await state_manager.get_data(user_id)

    # Очищаем состояние формы
    await state_manager.update_data(
        user_id,
        form_data=None,
        current_state=AppStates.CURRENT_MENU
    )

    # Получаем главное меню для возврата
    user_state = await state_manager.get_data(user_id=user_id)
    user_role = user_state.get('role')
    if user_role == UserRole.NEWCOMER.value:
        main_menu_id = Config.SEATABLE_MAIN_MENU_NEWCOMER_ID
    else:
        main_menu_id = Config.SEATABLE_MAIN_MENU_EMPLOYEE_ID

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(
                text="⬅️ В главное меню",
                callback_data=f"menu:{main_menu_id}"
            )]
        ]
    )


    # Создаем клавиатуру для возврата в главное меню
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(
                text="⬅️ В главное меню",
                callback_data=f"menu:{main_menu_id}"
            )]
        ]
    )

    # Удаляем клавиатуру у предыдущего сообщения
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except:
        pass

    # Отправляем сообщение об отмене
    await callback.message.answer(
        "Обращение отменено. Ваши ответы не сохранены.",
        reply_markup=keyboard
    )

    await callback.answer("Форма отменена")