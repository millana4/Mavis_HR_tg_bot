import logging

from aiogram import Router, types, F
from aiogram.filters import CommandStart
from aiogram.types import ReplyKeyboardRemove

from app.db.nocodb_client import NocoDBClient
from app.db.roles import RoleChecker
from app.services.cache import clear_user_auth, get_user_access_and_role
from app.services.utils import normalize_phone, contains_restricted_emails, mask_pii
from app.services.fsm import state_manager, AppStates
from app.db.auth import register_id_messenger, check_id_messenger
from app.db.table_data import fetch_table
from config import Config
from telegram.bot_menu import update_user_commands

from telegram.keyboards import share_contact_kb
from telegram.handlers.handler_table import handle_content_button, handle_table_menu
from telegram.utils import check_access
from telegram.content import prepare_telegram_message


# Создаем роутер
router = Router()
logger = logging.getLogger(__name__)


@router.message(CommandStart())
async def cmd_start(message: types.Message):
    """Обработчик нажатия кнопки Старт"""
    user_id = message.from_user.id
    logger.info(f"Пользователь {user_id} нажал кнопку Старт")

    # Проверяем, есть ли пользователь с таким id_telegram
    has_access, current_role = await check_id_messenger(str(user_id))
    logger.debug(f"Пользователь {user_id} авторизован: {has_access}, роль: {current_role}")

    if has_access:
        # Получаем данные из FSM
        user_data = await state_manager.get_data(user_id)
        previous_role = user_data.get("role")

        # ВАЖНО: Если роль изменилась - сбрасываем навигацию
        if previous_role and previous_role != current_role:
            logger.info(f"Роль изменилась при старте: {previous_role} -> {current_role}, сбрасываем навигацию")
            await state_manager.clear(user_id)

        # Обновляем роль в FSM
        await state_manager.update_data(user_id, role=current_role)

        # Проверяем, какое отдать меню — обычное или админское
        await update_user_commands(message.bot, user_id)

        # Если пользователь есть в таблице, инициализируем навигацию
        await start_navigation(message=message, current_role=current_role)
    else:
        # Иначе просим поделиться контактом
        await message.answer(
            "Поделитесь, пожалуйста, вашим номером телефона, чтобы войти 👇",
            reply_markup=share_contact_kb,
        )


@router.message(F.contact)
async def handle_contact(message: types.Message):
    """Обработка контакта для авторизации"""
    contact = message.contact
    user_id = message.from_user.id

    normalized_phone = normalize_phone(contact.phone_number)
    logger.info(
        "Пользователь %s прислал номер: %s (нормализован: %s)",
        user_id,
        mask_pii(contact.phone_number),
        mask_pii(normalized_phone)
    )

    # 1. Регистрируем id_messenger в Seatable
    success = await register_id_messenger(normalized_phone, str(user_id))

    if not success:
        await message.answer(
            "🚫 Ваш номер телефона не найден в системе. "
            "Чтобы получить доступ в бот, обратитесь, пожалуйста, к администратору.",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    # 2. Очищаем кеш авторизации (на случай повторной регистрации)
    clear_user_auth(user_id)

    # 3. Получаем доступ и роль через единый auth-сервис
    has_access, current_role = await get_user_access_and_role(user_id)

    if not has_access or not current_role:
        await message.answer(
            "🚫 Не удалось подтвердить доступ. Обратитесь к администратору.",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    # 4. Успешная авторизация
    await message.answer(
        "🎉 Вы успешно авторизовались!",
        reply_markup=ReplyKeyboardRemove()
    )

    # 5. Запускаем навигацию с актуальной ролью
    await start_navigation(message=message, current_role=current_role)


async def start_navigation(message: types.Message, current_role: str = None):
    """Инициализирует FSM и показывает главное меню"""
    try:
        user_id = message.chat.id

        # Очищаем состояние на случай перезапуска
        await state_manager.clear(user_id)

        # Проверяем права доступа и выходим если нет доступа
        has_access = await check_access(message=message)
        if not has_access:
            return

        # Если роль не передана - определяем роль пользователя по таблице Seatable
        if current_role is None:
            user_role = await RoleChecker.get_role(user_id)
        else:
            user_role = current_role

        # Записываем роль в FSM. Если функция определения не сработала и вернула None, то устанавливаем действующего.
        if user_role is not None:
            await state_manager.update_data(user_id, role=user_role)
        else:
            await state_manager.update_data(user_id, role="employee")

        # Получаем ID главного меню для роли пользователя
        if user_role == "newcomer":
            main_menu_id = Config.MAIN_MENU_NEWCOMER_ID
        else:
            main_menu_id = Config.MAIN_MENU_EMPLOYEE_ID
        logger.info(f"Main menu ID for user {user_id}: {main_menu_id}")

        # Инициализация состояния для переходов по меню
        await state_manager.update_data(
            user_id,
            current_menu=main_menu_id,
            navigation_history=[],
            current_state=AppStates.CURRENT_MENU,
            user_role=user_role if user_role else "employee"
        )

        # Получаем контент и клавиатуру для главного меню
        content, keyboard = await handle_table_menu(main_menu_id, str(user_id), message)

        kwargs = {
            'reply_markup': keyboard,
            'parse_mode': 'HTML'
        }

        # Отправляем контент в чат в зависимости от типа
        if content.get('image_url'):
            await message.answer_photo(
                photo=content['image_url'],
                caption=content.get('text', ''),
                **kwargs
            )
        elif content.get('video_url'):
            await message.answer_video(
                video=content['video_url'],
                caption=content.get('text', ''),
                **kwargs
            )
        elif content.get('text'):
            await message.answer(
                text=content['text'],
                **kwargs
            )
        elif keyboard:
            # Если есть только кнопки, отправляем пустое сообщение с ними
            await message.answer("Выберите раздел:", **kwargs)
        else:
            # На случай, если меню пустое
            await message.answer("Главное меню", **kwargs)

    except Exception as e:
        logger.error(f"Error in start_navigation for user {message.from_user.id}: {str(e)}", exc_info=True)
        await message.answer("⚠️ Произошла ошибка при загрузке меню. Попробуйте позже.")


# Хендлер кнопки "Назад"
@router.callback_query(lambda c: c.data == 'back')
async def process_back_callback(callback_query: types.CallbackQuery):
    """Обработчик кнопки 'Назад'"""
    try:
        user_id = callback_query.from_user.id

        logger.debug(f"Сработал обычный «Назад» из process_back_callback")

        # Проверяем права доступа и выходим если нет доступа
        has_access = await check_access(callback_query=callback_query)
        if not has_access:
            return

        # Получаем текущее меню
        current_menu = await state_manager.get_current_menu(user_id)

        # Выполняем возврат и получаем предыдущее меню
        previous_menu = await state_manager.navigate_back(user_id)

        if not previous_menu:
            await start_navigation(message=callback_query.message)
            await callback_query.answer()
            return

        # Получаем контент текущего меню
        button_content = None
        if current_menu and current_menu.startswith('content:'):
            _, current_table_id, current_row_id = current_menu.split(':')

            async with NocoDBClient() as nocodb:
                rows = await nocodb.get_all(
                    table_id=current_table_id,
                    where=f"(Id,eq,{current_row_id})"
                )
                if rows:
                    current_row = rows[0]
                    if current_row.get('Content_text') or current_row.get('Content_image'):
                        button_content = prepare_telegram_message(
                            text_content=current_row.get('Content_text', ''),
                            image_url=current_row.get('Content_image')
                        )

        # Удаляем текущее сообщение
        try:
            await callback_query.message.delete()
        except:
            pass

        # Если был контент - постим его перед возвратом
        if button_content:
            content_text = button_content.get('text', '')
            if not contains_restricted_emails(content_text):
                if button_content.get('image_url'):
                    await callback_query.message.answer_photo(
                        photo=button_content['image_url'],
                        caption=button_content.get('text', ''),
                        parse_mode="HTML"
                    )
                elif button_content.get('text'):
                    await callback_query.message.answer(
                        text=button_content['text'],
                        parse_mode="HTML"
                    )
            else:
                logger.debug(f"Контент содержит персональные данные, не постим в чат")

        # Возвращаемся к предыдущему экрану
        if previous_menu.startswith('content:'):
            _, table_id, row_id = previous_menu.split(':')
            content, keyboard = await handle_content_button(table_id, row_id)

            caption = content.get('text', '')
            if content.get('image_url'):
                await callback_query.message.answer_photo(
                    photo=content['image_url'],
                    caption=caption,
                    reply_markup=keyboard,
                    parse_mode="HTML"
                )
            else:
                if caption:
                    await callback_query.message.answer(
                        text=caption,
                        reply_markup=keyboard,
                        parse_mode="HTML"
                    )
                elif keyboard:
                    await callback_query.message.answer(
                        text=' ',
                        reply_markup=keyboard
                    )
        else:
            content, keyboard = await handle_table_menu(table_id=previous_menu, user_id=str(user_id))

            menu_text = content.get('text', '')
            if content and content.get('image_url'):
                await callback_query.message.answer_photo(
                    photo=content['image_url'],
                    caption=menu_text if menu_text else ' ',
                    reply_markup=keyboard,
                    parse_mode="HTML"
                )
            else:
                if menu_text:
                    await callback_query.message.answer(
                        text=menu_text,
                        reply_markup=keyboard,
                        parse_mode="HTML"
                    )
                elif keyboard:
                    await callback_query.message.answer(
                        text=' ',
                        reply_markup=keyboard
                    )

        await callback_query.answer()

    except Exception as e:
        logger.error(f"Back error: {str(e)}", exc_info=True)
        await callback_query.answer("Ошибка возврата", show_alert=True)