import asyncio
import logging
from datetime import datetime, time, timedelta

from app.services.process_1c import get_unprocessed_1c_users, process_1c_user
from app.services.roles import check_user_roles_daily

logger = logging.getLogger(__name__)

# Время запуска
sync_times = [time(12, 00), time(16, 0)]

# Время проверки ролей
roles_check_time = time(14, 00)


async def sync_1c_to_users():
    """
    Основная функция синхронизации
    """
    logger.info("Синхронизация 1С начата")

    # Получаем необработанных пользователей из 1С
    unprocessed_users = await get_unprocessed_1c_users()

    if not unprocessed_users:
        logger.info("Нет необработанных пользователей")
        return

    success_count = 0

    # Обрабатываем каждого пользователя
    for user in unprocessed_users:
        try:
            success = await process_1c_user(user)
            if success:
                success_count += 1

        except Exception as e:
            logger.error(f"Ошибка обработки {user.fio}: {str(e)}")

    logger.info(f"Синхронизация завершена. Обработано: {success_count}/{len(unprocessed_users)}")


async def start_sync_scheduler():
    """
    Запускает планировщик синхронизации и проверки ролей
    """
    logger.info("Планировщик синхронизации с 1С запущен")
    logger.info(f"Синхронизация с 1С будет в {', '.join([t.strftime('%H:%M') for t in sync_times])} МСК")
    logger.info(f"Проверка ролей будет в {roles_check_time.strftime('%H:%M')} МСК")

    # Запускаем обе задачи параллельно
    await asyncio.gather(
        _run_sync_scheduler(),
        _run_roles_checker()
    )


async def _run_sync_scheduler():
    """Запускает планировщик синхронизации"""
    while True:
        # Находим ближайшее время синхронизации
        now_utc = datetime.utcnow()
        moscow_offset = timedelta(hours=3)
        now_msk = now_utc + moscow_offset

        nearest_time = None
        nearest_datetime = None

        for sync_time in sync_times:
            sync_datetime = datetime.combine(now_msk.date(), sync_time)

            if sync_datetime < now_msk:
                sync_datetime = sync_datetime + timedelta(days=1)

            if nearest_datetime is None or sync_datetime < nearest_datetime:
                nearest_datetime = sync_datetime
                nearest_time = sync_time

        if nearest_time:
            await _wait_until(nearest_time)

            try:
                await sync_1c_to_users()
            except Exception as e:
                logger.error(f"Ошибка синхронизации: {e}")

            await asyncio.sleep(5)


async def _run_roles_checker():
    """Запускает планировщик проверки ролей"""
    while True:
        await _wait_until(roles_check_time)

        try:
            await check_user_roles_daily()
        except Exception as e:
            logger.error(f"Ошибка проверки ролей: {e}")

        await asyncio.sleep(5)


async def _wait_until(target_time: time):
    """
    Ждет до указанного времени по Москве
    """
    now_utc = datetime.utcnow()
    moscow_offset = timedelta(hours=3)
    now_msk = now_utc + moscow_offset

    next_run = datetime.combine(now_msk.date(), target_time)

    if next_run < now_msk:
        next_run = next_run + timedelta(days=1)

    wait_seconds = (next_run - now_msk).total_seconds()

    await asyncio.sleep(wait_seconds)