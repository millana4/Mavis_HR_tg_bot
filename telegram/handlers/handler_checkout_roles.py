from aiogram import F, Router
from aiogram.types import Message
import logging

from app.db.roles import RoleChecker
from app.services.fsm import state_manager
from app.services.cache import clear_user_auth
from config import Config
from telegram.handlers.handler_table import handle_table_menu

logger = logging.getLogger(__name__)

# Создаем роутер
router = Router()


@router.message(F.text == "/checkout_newcomer")
async def handle_checkout_newcomer(message: Message):
    """Переключает пользователя в режим новичка"""
    user_id = message.chat.id

    # Проверяем права админа
    from app.services.broadcast import is_user_admin
    if not await is_user_admin(user_id):
        await message.answer("❌ У вас нет прав для выполнения этой команды")
        return

    # Меняем роль в Seatable
    success = await RoleChecker.change_user_role(user_id, "newcomer")

    if success:
        # Очищаем кеш авторизации пользователя
        clear_user_auth(user_id)

        # Очищаем состояние FSM (включая историю навигации)
        await state_manager.clear(user_id)

        # Устанавливаем новую роль в FSM
        await state_manager.update_data(user_id, role="newcomer")

        # Устанавливаем главное меню для новичка
        main_menu_id = Config.MAIN_MENU_NEWCOMER_ID
        await state_manager.navigate_to_menu(user_id, main_menu_id)

        # Получаем и показываем контент меню
        from telegram.handlers.handler_table import handle_table_menu
        content, keyboard = await handle_table_menu(main_menu_id, str(user_id), message=message)

        kwargs = {'reply_markup': keyboard, 'parse_mode': 'HTML'}
        if content.get('image_url'):
            await message.answer_photo(photo=content['image_url'], caption=content.get('text', ''), **kwargs)
        elif content.get('text'):
            await message.answer(text=content['text'], **kwargs)
        else:
            await message.answer("Режим новичка активирован", **kwargs)
    else:
        await message.answer("Ошибка при смене роли на новичка")


@router.message(F.text == "/checkout_employee")
async def handle_checkout_employee(message: Message):
    """Переключает пользователя в режим действующего сотрудника"""
    user_id = message.chat.id

    # Проверяем права админа
    from app.services.broadcast import is_user_admin
    if not await is_user_admin(user_id):
        await message.answer("❌ У вас нет прав для выполнения этой команды")
        return

    # Меняем роль в Seatable
    success = await RoleChecker.change_user_role(user_id, "employee")

    if success:
        # Очищаем кеш авторизации пользователя
        clear_user_auth(user_id)

        # Очищаем состояние FSM (включая историю навигации)
        await state_manager.clear(user_id)

        # Устанавливаем новую роль в FSM
        await state_manager.update_data(user_id, role="employee")

        # Устанавливаем главное меню для сотрудника
        main_menu_id = Config.MAIN_MENU_EMPLOYEE_ID
        await state_manager.navigate_to_menu(user_id, main_menu_id)

        # Получаем и показываем контент меню
        content, keyboard = await handle_table_menu(main_menu_id, str(user_id), message=message)

        kwargs = {'reply_markup': keyboard, 'parse_mode': 'HTML'}
        if content.get('image_url'):
            await message.answer_photo(photo=content['image_url'], caption=content.get('text', ''), **kwargs)
        elif content.get('text'):
            await message.answer(text=content['text'], **kwargs)
        else:
            await message.answer("Режим сотрудника активирован", **kwargs)
    else:
        await message.answer("Ошибка при смене роли на действующего сотрудника")


@router.message(F.text == "/support")
async def handle_support(message: Message):
    """Обрабатывает команду поддержки"""
    user_id = message.chat.id

    support_text = """
    <b>Поддержка</b>
Если у вас возникли проблемы с работой бота или есть вопросы, напишите администратору: @kit_it_company
    """

    await message.answer(support_text, parse_mode='HTML')