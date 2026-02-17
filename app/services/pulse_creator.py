import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple

from config import Config
from app.db.nocodb_client import NocoDBClient
from app.db.users import User


logger = logging.getLogger(__name__)


class HolidayChecker:
    """Проверяет праздники и выходные в России"""

    # Праздники (день-месяц)
    FIXED_HOLIDAYS = [
        (1, 1),  # 1 января
        (2, 1),  # 2 января
        (3, 1),  # 3 января
        (4, 1),  # 4 января
        (5, 1),  # 5 января
        (6, 1),  # 6 января
        (8, 1),  # 8 января
        (7, 1),  # 7 января - Рождество
        (23, 2),  # 23 февраля
        (8, 3),  # 8 марта
        (1, 5),  # 1 мая
        (9, 5),  # 9 мая
        (12, 6),  # 12 июня
        (4, 11),  # 4 ноября
        (31, 12),  # 31 декабря
    ]


    @classmethod
    def is_holiday(cls, check_date: date) -> bool:
        """Проверяет, является ли дата праздником"""
        # Проверяем праздники
        for day, month in cls.FIXED_HOLIDAYS:
            if check_date.day == day and check_date.month == month:
                return True

        # Дополнительно: проверяем новогодние каникулы 7 января уже есть
        # Проверим период с 1 по 8 января (кроме 7 уже проверено)
        if check_date.month == 1 and 1 <= check_date.day <= 8:
            return True

        return False


    @classmethod
    def is_weekend(cls, check_date: date) -> bool:
        """Проверяет, является ли дата выходным (суббота или воскресенье)"""
        # Понедельник = 0, Воскресенье = 6
        return check_date.weekday() >= 5  # 5=суббота, 6=воскресенье


    @classmethod
    def is_non_working_day(cls, check_date: date) -> bool:
        """Проверяет, является ли дата нерабочим днем (праздник или выходной)"""
        return cls.is_holiday(check_date) or cls.is_weekend(check_date)


    @classmethod
    def get_next_working_day(cls, check_date: date) -> date:
        """Находит ближайший рабочий день после указанной даты"""
        next_day = check_date

        # Пока день нерабочий - ищем следующий
        while cls.is_non_working_day(next_day):
            next_day += timedelta(days=1)

        return next_day


    @classmethod
    def adjust_poll_date(cls, poll_date: date) -> date:
        """Корректирует дату опроса, если она выпадает на нерабочий день"""
        if cls.is_non_working_day(poll_date):
            return cls.get_next_working_day(poll_date)
        return poll_date


class PulseTaskCreator:
    """Создатель задач для пульс-опросов"""

    # Типы опросов и их периоды (дни)
    POLL_TYPES = {
        '1_week': {'days': 7, 'name': 'Через 1 неделю'},
        '1_month': {'days': 30, 'name': 'Через 1 месяц'},
        '3_months': {'days': 91, 'name': 'Через 3 месяца'},
        '6_months': {'days': 183, 'name': 'Через 6 месяцев'},
        '1_year': {'days': 365, 'name': 'Через 1 год'}
    }


    def __init__(self):
        self.holiday_checker = HolidayChecker()


    async def create_tasks(self, user_data: Dict) -> bool:
        """
        Создает задачи пульс-опросов для пользователя
        """
        try:
            # Парсим дату устройства
            employment_date = self._parse_date(user_data.get('Date_employment'))
            if not employment_date:
                logger.warning(f"Нет даты устройства для пользователя {user_data.get('FIO')}")
                return False

            # Определяем, какие опросы нужны
            needed_polls = self._get_needed_polls(employment_date)

            if not needed_polls:
                logger.info(f"Нет опросов для пользователя {user_data.get('FIO')}")
                return True  # Это нормально - просто нет опросов

            # Создаем задачи для каждого опроса
            success_count = 0
            for poll_type in needed_polls:
                try:
                    success = await self._create_single_task(user_data, employment_date, poll_type)
                    if success:
                        success_count += 1
                except Exception as e:
                    logger.error(f"Ошибка создания задачи {poll_type} для {user_data.get('FIO')}: {e}")

            logger.info(f"Создано {success_count}/{len(needed_polls)} задач для {user_data.get('FIO')}")
            return success_count > 0

        except Exception as e:
            logger.error(f"Ошибка создания пульс-опросов: {e}")
            return False

    def _parse_date(self, date_str: Optional[str]) -> Optional[date]:
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, '%Y-%m-%d').date()
        except (ValueError, TypeError):
            return None


    def _get_needed_polls(self, employment_date: date) -> List[str]:
        """
        Определяет, какие опросы нужны пользователю
        """
        if not employment_date:
            return []

        # Проверяем, работает ли меньше года
        today = datetime.now().date()
        one_year_later = employment_date + timedelta(days=365)
        if today > one_year_later:
            # Работает больше года - не создаем опросы
            return []

        needed_polls = []

        for poll_type, poll_info in self.POLL_TYPES.items():
            poll_date = employment_date + timedelta(days=poll_info['days'])

            # Если дата опроса еще не наступила - создаем задачу
            if poll_date > today:
                needed_polls.append(poll_type)

        return needed_polls


    def _calculate_and_adjust_poll_date(self, employment_date: date, poll_type: str) -> Tuple[date, bool]:
        """
        Рассчитывает дату опроса и корректирует если нужно
        Возвращает: (скорректированная_дата, была_ли_корректировка)
        """
        if poll_type not in self.POLL_TYPES:
            raise ValueError(f"Неизвестный тип опроса: {poll_type}")

        days = self.POLL_TYPES[poll_type]['days']
        poll_date = employment_date + timedelta(days=days)

        # Проверяем и корректируем дату
        original_date = poll_date
        adjusted_date = self.holiday_checker.adjust_poll_date(poll_date)

        was_adjusted = original_date != adjusted_date

        if was_adjusted:
            logger.info(f"Дата опроса скорректирована: {original_date} -> {adjusted_date}")

        return adjusted_date, was_adjusted

    @staticmethod
    async def task_exists(snils: str, poll_type: str) -> bool:
        """
        Проверяет, существует ли уже задача для данного пользователя и типа опроса в NocoDB
        """
        try:
            async with NocoDBClient() as client:
                # Создаем фильтр: snils AND Type == poll_type
                where_filter = f"(SNILS,eq,{snils})~and(Type,eq,{poll_type})"

                tasks = await client.get_all(
                    table_id=Config.PULSE_TASKS_ID,
                    where=where_filter,
                    limit=1  # Нужна только проверка существования
                )

                exists = len(tasks) > 0

                if exists:
                    logger.info(f"Задача уже существует: {snils} - {poll_type}")

                return exists

        except Exception as e:
            logger.error(f"Ошибка проверки существования задачи: {e}")
            return False


    async def _create_single_task(self, user_data: Dict, employment_date: date, poll_type: str) -> bool:
        """
        Создает одну задачу пульс-опроса
        """
        # Проверяем, не существует ли уже такая задача
        snils = user_data.get('SNILS')
        if await self.task_exists(snils, poll_type):
            logger.info(f"Задача уже существует, пропускаем: {snils} - {poll_type}")
            return True  # Считаем успехом, т.к. задача уже есть

        # Рассчитываем и корректируем дату опроса
        poll_date, was_adjusted = self._calculate_and_adjust_poll_date(employment_date, poll_type)

        # Подготавливаем данные для записи
        task_data = {
            'FIO': user_data.get('FIO'),
            'SNILS': snils,  # СНИЛС
            'Department': user_data.get('Department'),
            'Position': user_data.get('Position'),
            'Email': user_data.get('Email'),
            'Phone_private': user_data.get('Phone_private'),
            'Main_company': user_data.get('Main_company'),
            'Companies': user_data.get('Companies', []),
            'Data_employment': employment_date.isoformat(),
            'Data_poll': poll_date.isoformat(),
            'Type': poll_type,
            'Status': 'waiting',
            'ID_messenger': '',  # Пусто, заполнится позже
            'Created_at': datetime.now().isoformat(),
            'Date_adjusted': was_adjusted  # Флаг корректировки даты
        }

        # Записываем в таблицу через NocoDB API. Пытаемся сохранить с повторными попытками, если сразе не удалось
        max_retries = 3
        retry_delay = 5  # секунд

        for attempt in range(1, max_retries + 1):
            try:
                async with NocoDBClient() as client:
                    result = await client.create_record(table_id=Config.PULSE_TASKS_ID, data=task_data)
                    if result:
                        logger.info(f"Задача на пульс-опрос создана: {task_data.get('FIO')} - {task_data.get('Type')}")
                        return True
                    else:
                        logger.error(f"Ошибка создания задачи: {task_data.get('FIO')} - {task_data.get('Type')}")
                        return False
            except Exception as e:
                if attempt < max_retries:
                    logger.warning(
                        f"Попытка {attempt}/{max_retries} не удалась для {snils} - {poll_type}: {e}. Повтор через {retry_delay} сек.")
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error(f"Все {max_retries} попыток не удались для {snils} - {poll_type}: {e}")
                    return False

        return False


    async def _create_pulse_for_user(user: User) -> bool:
        """
        Создает пульс-опросы для пользователя
        """
        # Конвертируем User в dict для передачи
        user_dict = {
            'FIO': user.fio,
            'SNILS': user.snils,
            'Department': user.department,
            'Position': user.position,
            'Data_employment': user.employment_date.isoformat() if user.employment_date else None
        }

        try:
            return await create_pulse_all_tasks(user_dict)
        except Exception as e:
            logger.error(f"Ошибка создания пульс-опросов для {user.fio}: {e}")
            return False


# Глобальный экземпляр
pulse_task_creator = PulseTaskCreator()


async def create_pulse_all_tasks(user_data: Dict) -> bool:
    """
    Основная функция для создания пульс-опросов
    """
    logger.info(f"Создание пульс-опросов для {user_data.get('FIO')}")
    return await pulse_task_creator.create_tasks(user_data)