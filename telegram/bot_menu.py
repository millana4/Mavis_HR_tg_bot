import logging
from aiogram import Bot
from aiogram.types import BotCommand, BotCommandScopeChat

from config import Config
from app.services.broadcast import is_user_admin
from app.db.table_data import fetch_table


logger = logging.getLogger(__name__)

# Команды для администраторов
admin_commands = [
    BotCommand(command="/start", description="Перезапустить бота"),
    BotCommand(command="/checkout_newcomer", description="Режим новичка"),
    BotCommand(command="/checkout_employee", description="Режим действующего сотрудника"),
    BotCommand(command="/broadcast", description="Рассылка уведомлений"),
    BotCommand(command="/scheduled_broadcasts", description="Посмотреть отложенные рассылки"),
    BotCommand(command="/send_exit_pulse", description="Назначить пульс-опрос при увольнении"),
]

# Команды для обычных пользователей
user_commands = [
    BotCommand(command="/start", description="Перезапустить бота"),
    BotCommand(command="/support", description="Написать админу")
]


async def set_main_menu(bot: Bot):
    """Устанавливает главное меню команд в зависимости от роли пользователя"""
    # Устанавливаем команды по умолчанию для всех пользователей
    await bot.set_my_commands(user_commands)

    # Для админов установим отдельные команды
    try:
        # Получаем список всех пользователей для установки команд админам
        users = await fetch_table(table_id=Config.AUTH_TABLE_ID, app='USER')

        for user in users:
            user_id = user.get('ID_messenger')
            if user_id:
                try:
                    if await is_user_admin(int(user_id)):
                        # Устанавливаем админские команды
                        await bot.set_my_commands(admin_commands, scope=BotCommandScopeChat(chat_id=int(user_id)))
                        logger.info(f"Admin commands set for user {user_id}")
                    else:
                        # Устанавливаем обычные команды для не-админов
                        await bot.set_my_commands(user_commands, scope=BotCommandScopeChat(chat_id=int(user_id)))
                        logger.info(f"User commands set for user {user_id}")
                except Exception as e:
                    logger.error(f"Error setting commands for {user_id}: {e}")

    except Exception as e:
        logger.error(f"Error setting admin commands: {e}")


async def update_user_commands(bot: Bot, user_id: int):
    """
    Обновляет меню команд для конкретного пользователя
    Вызывается при старте или при изменении прав
    """
    try:
        # Проверяем, является ли пользователь админом
        is_admin = await is_user_admin(user_id)

        if is_admin:
            await bot.set_my_commands(admin_commands, scope=BotCommandScopeChat(chat_id=user_id))
            logger.info(f"Admin commands updated for user {user_id}")
        else:
            await bot.set_my_commands(user_commands, scope=BotCommandScopeChat(chat_id=user_id))
            logger.info(f"User commands updated for user {user_id}")

    except Exception as e:
        logger.error(f"Error updating commands for user {user_id}: {e}")