import logging
from datetime import datetime, time, timedelta
from typing import Dict, List, Optional
import asyncio
from aiogram import Bot

from app.db.nocodb_client import NocoDBClient
from config import Config
from app.db.table_data import fetch_table
from telegram.content import prepare_telegram_message


logger = logging.getLogger(__name__)

sending_time = time(10, 47)


class PulseSender:
    """Отправляет пульс-опросы пользователям"""

    def __init__(self, bot: Bot):
        self.bot = bot

    async def send_daily_pulses(self) -> None:
        """
        Основная функция: отправляет пульс-опросы которые нужно отправить сегодня
        """
        logger.info("Начало отправки пульс-опросов")

        try:
            # Получаем задачи которые нужно отправить сегодня
            tasks_to_send = await self._get_tasks_for_today()

            if not tasks_to_send:
                logger.info("Нет пульс-опросов для отправки сегодня")
                return

            logger.info(f"Найдено {len(tasks_to_send)} задач для отправки")

            # Получаем контент опросов
            poll_content = await self._get_poll_content()

            if not poll_content:
                logger.error("Не удалось получить контент опросов")
                return

            # Получаем список админов для уведомлений
            admins = await self._get_pulse_admins()
            logger.info(f"Админы для отправки уведомлений {admins}")

            # Отправляем каждую задачу
            sent_tasks = []
            failed_tasks = []

            for task in tasks_to_send:
                try:
                    success = await self._send_single_pulse(task, poll_content)
                    if success:
                        sent_tasks.append(task)
                        # Обновляем статус задачи на "sent"
                        await self._update_task_status(task.get('Id'), 'sent')
                    else:
                        failed_tasks.append(task)
                        # Обновляем статус задачи на "declined"
                        await self._update_task_status(task.get('Id'), 'declined')

                except Exception as e:
                    logger.error(f"Ошибка отправки задачи {task.get('Id')}: {e}")
                    failed_tasks.append(task)
                    await self._update_task_status(task.get('Id'), 'declined')

            # Уведомляем админов о неудачных отправках
            if failed_tasks and admins:
                await self._notify_admins_about_failed_tasks(admins, failed_tasks)

            logger.info(f"Отправка завершена. Успешно: {len(sent_tasks)}, Не удалось: {len(failed_tasks)}")

        except Exception as e:
            logger.error(f"Ошибка при отправке пульс-опросов: {e}")


    async def _get_tasks_for_today(self) -> List[Dict]:
        """Получает задачи которые нужно отправить сегодня"""
        try:
            async with NocoDBClient() as client:
                tasks = await client.get_all(table_id=Config.PULSE_TASKS_ID)

            logger.info(f'Найденные задачи в таблице {tasks}')

            if not tasks:
                logger.info("Нет задач в таблице пульс-опросов")
                return []

            today = datetime.now().date()
            today_str = today.isoformat()

            tasks_for_today = []
            for task in tasks:
                task_date = task.get('Data_poll')
                task_status = task.get('Status')

                # Проверяем что задача сегодня и статус waiting
                if (task_date == today_str and task_status == 'waiting'):
                    tasks_for_today.append(task)

            logger.info(f"Найдено задач для отправки сегодня: {len(tasks_for_today)}")
            return tasks_for_today

        except Exception as e:
            logger.error(f"Ошибка получения задач: {e}", exc_info=True)
            return []


    async def _get_poll_content(self) -> Dict[str, Dict]:
        """
        Получает контент всех опросов и группирует по типу
        """
        try:
            content = await fetch_table(
                table_id=Config.PULSE_CONTENT_ID,
                app='PULSE'
            )

            if not content:
                return {}

            # Группируем контент по типу опроса
            content_by_type = {}
            for item in content:
                poll_type = item.get('Type')
                if poll_type:
                    content_by_type[poll_type] = item

            return content_by_type

        except Exception as e:
            logger.error(f"Ошибка получения контента опросов: {e}")
            return {}

    async def _get_pulse_admins(self) -> List[Dict]:
        """
        Получает список админов с правами Pulse_admin
        Возвращает список словарей с Telegram ID админов
        """
        try:
            # Получаем таблицу админов
            admins = await fetch_table(
                table_id=Config.ADMIN_TABLE_ID,
                app='USER'
            )

            if not admins:
                return []

            pulse_admins = []

            for admin in admins:
                if admin.get('Pulse_admin') is True:
                    telegram_id = admin.get('ID_messenger')
                    if telegram_id:
                        pulse_admins.append({
                            'telegram_id': telegram_id,
                            'fio': admin.get('FIO', 'Администратор')
                        })
                    else:
                        logger.warning(f"У админа {admin.get('FIO')} нет ID_messenger")

            logger.info(f"Найдено админов с Pulse_admin - {len(pulse_admins)}: {pulse_admins}")
            return pulse_admins

        except Exception as e:
            logger.error(f"Ошибка получения админов: {e}")
            return []

    async def _get_user_messenger_id(self, snils: str) -> Optional[str]:
        """
        Получает ID_messenger пользователя по СНИЛС через фильтрацию NocoDB
        """
        try:
            async with NocoDBClient() as client:
                # Фильтруем по SNILS, получаем только одну запись
                where_filter = f"(SNILS,eq,{snils})"
                users = await client.get_all(
                    table_id=Config.AUTH_TABLE_ID,
                    where=where_filter,
                    limit=1
                )

                if not users:
                    logger.info(f"Пользователь с СНИЛС {snils} не найден")
                    return None

                user = users[0]
                messenger_id = user.get('ID_messenger')
                logger.info(f"Найден новичок для отправки {messenger_id}")
                return str(messenger_id) if messenger_id else None

        except Exception as e:
            logger.error(f"Ошибка получения ID_messenger для {snils}: {e}")
            return None


    async def _send_single_pulse(self, task: Dict, poll_content: Dict[str, Dict]) -> bool:
        """Отправляет один пульс-опрос пользователю"""
        try:
            logger.info(f"Начинаем отправку задачи {task.get('Id')}: {task}")

            snils = task.get('SNILS')
            messenger_id = await self._get_user_messenger_id(snils)

            if not messenger_id:
                logger.warning(f"У пользователя нет ID_messenger для задачи {task.get('_id')}")
                return False

            logger.info(f"Найден ID_messenger: {messenger_id} для пульс-опроса")

            # Получаем контент опроса
            poll_type = task.get('Type')
            logger.info(f"Тип опроса: {poll_type}")

            content_item = poll_content.get(poll_type)

            if not content_item:
                logger.error(f"Нет контента для типа опроса: {poll_type}")
                logger.error(f"Доступные типы контента: {list(poll_content.keys())}")
                return False

            # Подготавливаем контент для отправки
            content_text = content_item.get('Content_text', '')
            image_url = content_item.get('Content_image', None)
            prepared_content = prepare_telegram_message(content_text, image_url)

            logger.info(f"Контент подготовлен: {'есть текст' if prepared_content.get('text') else 'нет текста'}, "
                        f"{'есть изображение' if prepared_content.get('image_url') else 'нет изображения'}")

            # Отправляем сообщение
            try:
                if prepared_content.get('image_url'):
                    await self.bot.send_photo(
                        chat_id=int(messenger_id),
                        photo=prepared_content['image_url'],
                        caption=prepared_content.get('text', ''),
                        parse_mode="HTML"
                    )
                elif prepared_content.get('text'):
                    await self.bot.send_message(
                        chat_id=int(messenger_id),
                        text=prepared_content['text'],
                        parse_mode="HTML"
                    )
                else:
                    logger.error(f"Пустой контент для опроса {poll_type}")
                    return False

                logger.info(f"Пульс-опрос {poll_type} отправлен пользователю {messenger_id}")
                return True

            except Exception as send_error:
                # Проверяем тип ошибки по тексту сообщения
                error_msg = str(send_error).lower()

                # Проверяем, заблокирован ли бот
                if "forbidden" in error_msg and ("bot was blocked" in error_msg or "bot blocked" in error_msg):
                    logger.warning(f"Пользователь {messenger_id} заблокировал бота")
                    return False

                # Проверяем другие ошибки Telegram
                elif "bad request" in error_msg:
                    logger.error(f"Ошибка Telegram для пользователя {messenger_id}: {error_msg}")
                    return False

                # Проверяем ошибку чата не найден
                elif "chat not found" in error_msg:
                    logger.warning(f"Чат не найден для пользователя {messenger_id}")
                    return False

                # Проверяем ошибку неверного ID чата
                elif "chat_id is empty" in error_msg or "invalid chat id" in error_msg:
                    logger.error(f"Неверный chat_id для пользователя {messenger_id}")
                    return False

                # Проверяем ошибку доступа
                elif "have no rights" in error_msg or "not enough rights" in error_msg:
                    logger.error(f"Нет прав для отправки пользователю {messenger_id}")
                    return False

                # Все остальные ошибки
                else:
                    logger.error(f"Ошибка отправки пользователю {messenger_id}: {send_error}", exc_info=True)
                    return False

        except Exception as e:
            logger.error(f"Ошибка отправки пульс-опроса: {e}", exc_info=True)
            return False


    async def _update_task_status(self, task_id: str, status: str) -> bool:
        """
        Обновляет статус задачи в NocoDB
        """
        try:
            if not task_id:
                return False
            # Подготавливаем данные для обновления
            update_data = {
                "Status": status,
                "Sent_date": datetime.now().isoformat() if status == 'sent' else None
            }

            async with NocoDBClient() as client:
                await client.update_record(
                    table_id=Config.PULSE_TASKS_ID,
                    record_id=task_id,
                    data=update_data
                )

                logger.info(f"Статус задачи {task_id} обновлен на {status}")
                return True

        except Exception as e:
            logger.error(f"Ошибка при обновлении статуса задачи: {e}")
            return False


    async def _notify_admins_about_failed_tasks(self, admins: List[Dict], failed_tasks: List[Dict]) -> None:
        """
        Уведомляет админов о неудачных отправках
        Теперь admins содержит словари с 'telegram_id' вместо 'ID_messenger'
        """
        try:
            # Группируем неудачные задачи по типам
            failed_by_type = {}
            for task in failed_tasks:
                poll_type = task.get('Type', 'неизвестный')
                if poll_type not in failed_by_type:
                    failed_by_type[poll_type] = []
                failed_by_type[poll_type].append(task)

            # Формируем сообщение для админов
            message_lines = ["❌ Пульс-опросы не отправлены:"]

            for poll_type, tasks in failed_by_type.items():
                message_lines.append(f"\n<b>{poll_type}:</b>")
                for task in tasks:
                    message_lines.append(f"• {task.get('FIO', 'Неизвестный')}")

            message_lines.append("\nСвяжитесь, пожалуйста, с этими сотрудниками по почте.")
            message = "\n".join(message_lines)

            # Отправляем сообщение каждому админу
            for admin in admins:
                telegram_id = admin.get('telegram_id')
                if telegram_id:
                    try:
                        await self.bot.send_message(
                            chat_id=int(telegram_id),
                            text=message,
                            parse_mode="HTML"
                        )
                        logger.info(f"Уведомление отправлено админу {admin.get('fio')} (ID: {telegram_id})")
                    except Exception as e:
                        logger.error(f"Ошибка отправки уведомления админу {telegram_id}: {e}")
                else:
                    logger.warning(f"У админа {admin.get('row_id')} нет Telegram ID")

        except Exception as e:
            logger.error(f"Ошибка уведомления админов: {e}")


async def start_pulse_sender_scheduler(bot: Bot):
    """
    Запускает планировщик отправки пульс-опросов
    """
    logger.info("Планировщик отправки пульс-опросов запущен")

    sender = PulseSender(bot)

    while True:
        await _wait_until(sending_time)

        # Запускаем отправку
        try:
            await sender.send_daily_pulses()
        except Exception as e:
            logger.error(f"Ошибка отправки пульс-опросов: {e}")

        # Пауза перед следующим днем
        await asyncio.sleep(60)


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

    hours = wait_seconds // 3600
    minutes = (wait_seconds % 3600) // 60

    logger.info(f"Ждем до {target_time.strftime('%H:%M')} МСК для отправки пульс-опросов")
    logger.info(f"Осталось {int(hours)} часов {int(minutes)} минут")

    await asyncio.sleep(wait_seconds)