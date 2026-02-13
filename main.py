import asyncio
import logging
from aiogram import Bot, Dispatcher

from app.services.fsm import state_manager
from config import Config
from app.services.pulse_sender import start_pulse_sender_scheduler


from telegram import custom_logging
from telegram.bot_menu import set_main_menu
from telegram.handlers import handler_contacts, handler_form, handler_table, handler_base, handler_broadcast, \
    handler_checkout_roles, handler_bc_schedule, handler_exit_pulse


async def main():
    # Инициализация логирования
    custom_logging.setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("Запуск бота...")

    # Инициализация бота и диспетчера
    bot = Bot(token=Config.BOT_TOKEN)
    dp = Dispatcher()

    # Планировщик синхронизации бота с данными пользователей из 1С + рассылки пульс-опросов
    scheduler_tasks = [
        # asyncio.create_task(start_sync_scheduler()),
        asyncio.create_task(start_pulse_sender_scheduler(bot))
    ]

    # Регистрация роутеров
    dp.include_router(handler_checkout_roles.router)
    dp.include_router(handler_broadcast.router)
    dp.include_router(handler_bc_schedule.router)
    dp.include_router(handler_base.router)
    dp.include_router(handler_contacts.router)
    dp.include_router(handler_form.router)
    dp.include_router(handler_table.router)
    dp.include_router(handler_exit_pulse.router)
    dp.startup.register(set_main_menu)

    # Запуск бота
    try:
        await dp.start_polling(bot)
    finally:
        # Останавливаем планировщики
        for task in scheduler_tasks:
            task.cancel()
        logger.info("Планировщики остановлены")

        # Сохраняем состояние FSM в БД
        state_manager.save_to_db()
        logger.info("Состояние FSM сохранено в SQLite")

        logger.info("Бот остановлен")

if __name__ == "__main__":
    asyncio.run(main())