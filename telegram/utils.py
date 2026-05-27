import pprint
import logging

from aiogram import types
from aiogram.types import ReplyKeyboardRemove

from app.services.cache import get_user_access_and_role, clear_user_auth
from app.services.fsm import state_manager

logger = logging.getLogger(__name__)

RESTRICTING_MESSAGE = "🚫 Извините, у вас больше нет доступа. Чтобы вернуть доступ, обратитесь, пожалуйста, к администратору."


async def check_access(message: types.Message = None, callback_query: types.CallbackQuery = None) -> bool:
    """Функция отвечает, если ли доступ у пользователя. Если нет, выводит сообщение"""
    user_id = None

    if callback_query:
        user_id = callback_query.from_user.id

        # Получаем доступ и роль из кеша/Seatable
        has_access, role = await get_user_access_and_role(user_id)

        if not has_access:
            await callback_query.answer(RESTRICTING_MESSAGE, show_alert=True)
            logger.info(f"У пользователя {user_id} больше нет доступа.")
            # Очищаем данные пользователя в FSM
            await state_manager.clear(user_id)
            return False

        # Получаем текущие данные из FSM
        user_data = await state_manager.get_data(user_id)
        previous_role = user_data.get("role")

        # Обновляем роль в FSM
        await state_manager.update_data(user_id, role=role)

        # Если роль изменилась - сбрасываем навигацию и отправляем в главное меню новой роли
        if previous_role and previous_role != role:
            logger.info(f"Роль изменилась: {previous_role} -> {role}, сбрасываем навигацию")
            # Сбрасываем историю навигации
            await state_manager.update_data(user_id, navigation_history=[])

            # Отправляем пользователя в главное меню новой роли через start_navigation
            from telegram.handlers.handler_base import start_navigation
            if callback_query.message:
                await start_navigation(message=callback_query.message)
            return False  # Прерываем текущее действие

        logger.debug(f"Доступ пользователя {user_id} подтвержден, роль: {role}")
        return True

    elif message:
        user_id = message.chat.id

        # Получаем доступ и роль из кеша/Seatable
        has_access, role = await get_user_access_and_role(user_id)

        if not has_access:
            await message.answer(RESTRICTING_MESSAGE, reply_markup=ReplyKeyboardRemove())
            logger.info(f"У пользователя {user_id} больше нет доступа.")
            # Очищаем данные пользователя в FSM
            await state_manager.clear(user_id)
            return False

        # Получаем текущие данные из FSM
        user_data = await state_manager.get_data(user_id)
        previous_role = user_data.get("role")

        # Обновляем роль в FSM
        await state_manager.update_data(user_id, role=role)

        # Если роль изменилась - сбрасываем навигацию и отправляем в главное меню новой роли
        if previous_role and previous_role != role:
            logger.info(f"Роль изменилась: {previous_role} -> {role}, сбрасываем навигацию")
            # Сбрасываем историю навигации
            await state_manager.update_data(user_id, navigation_history=[])

            # Отправляем пользователя в главное меню новой роли
            from telegram.handlers.handler_base import start_navigation
            await start_navigation(message=message)
            return False  # Прерываем текущее действие

        logger.debug(f"Доступ пользователя {user_id} подтвержден, роль: {role}")
        return True

    return False
