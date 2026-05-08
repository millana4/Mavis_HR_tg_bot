import asyncio
import logging
import pprint
import re
from collections import defaultdict
from datetime import datetime, time, timedelta
from typing import List, Dict, Iterable, Callable, Awaitable

from dateutil.relativedelta import relativedelta

from app.db.organization import Company, Department, CompanySegment
from app.db.roles import check_user_roles_daily, UserRole
from app.db.sync_db_executor import create_pivot, update_pivot, archive_pivot, get_auth, create_auth, update_auth, \
    delete_auth
from app.db.users import User, Employment
from app.db.table_data import fetch_table
from app.services.pulse_creator import PulseTaskCreator
from app.services.utils import normalize_phones_string, values_to_set, phones_to_set, surname_to_str
from config import Config

logger = logging.getLogger(__name__)


# Добавить создание пуль-опросов для новых пользователей а тоя его удалила

# Время синхронизации сводной таблицы с авторизационной
sync_auth_times = [time(8, 15), time(8, 30)]

# Время проверки ролей
roles_check_time = [time(9, 00)]


#__________________________________________________
#          ПОДГОТОВКА К СИНХРОНИЗАЦИИ

async def get_pivot_table_users() -> Dict[str, Dict]:
    """
    Получает всех пользователей из сводной таблицы
    Возвращает словарь {snils: row_data}
    """
    try:
        pivot_users = await fetch_table(table_id=Config.PIVOT_TABLE_ID, app='USER')

        if not pivot_users:
            return {}

        return {user.get('SNILS'): user for user in pivot_users if user.get('SNILS')}

    except Exception as e:
        logger.error(f"Ошибка получения пользователей из сводной таблицы: {e}")
        return {}


# __________________________________________________________
#            СИНХРОНИЗАЦИЯ С ВЫГРУЗКОЙ 1С

async def sync_auth():
    """
    Синхронизация таблицы авторизации на основе данных из сводной таблицы.
    Только активные пользователи (не архивные).
    """
    logger.info("Начало синхронизации таблицы авторизации")

    try:
        pivot_users = await get_pivot_table_users()
        logger.info(f"Получено {len(pivot_users)} пользователей из сводной таблицы")

        # Фильтруем активных и архивных пользователей отдельно
        active_pivot_users = {}
        archived_pivot_users = {}

        for snils, user_data in pivot_users.items():
            # Если ключа 'Is_archived' нет вообще - значит пользователь активный
            if 'Is_archived' not in user_data:
                active_pivot_users[snils] = user_data
            # Если ключ есть и он True - пользователь архивный
            elif user_data.get('Is_archived') is True:
                archived_pivot_users[snils] = user_data
            # Если ключ есть и он False - пользователь активный
            else:
                active_pivot_users[snils] = user_data

        logger.info(f"Найдено активных пользователей: {len(active_pivot_users)}")
        logger.info(f"Найдено архивных пользователей: {len(archived_pivot_users)}")


        # Получаем текущих пользователей из авторизационной таблицы
        auth_users = await get_auth()
        logger.info(f"В авторизационной таблице найдено {len(auth_users)} пользователей")

        created_count = 0
        updated_count = 0
        skipped_count = 0

        # Обрабатываем каждого активного пользователя
        for snils, pivot_user in active_pivot_users.items():
            try:
                # Дата устройства
                date_employment_str = pivot_user.get('Date_employment')
                date_employment = None
                if date_employment_str:
                    try:
                        date_employment = datetime.strptime(date_employment_str, '%Y-%m-%d').date()
                    except Exception as e:
                        logger.warning(f"Некорректный формат даты устройства: {date_employment_str}, ошибка: {e}")

                # Определяем роль
                role = UserRole.EMPLOYEE
                if date_employment:
                    three_months_ago = datetime.now().date() - relativedelta(months=3)
                    if date_employment > three_months_ago:
                        role = UserRole.NEWCOMER

                fio = pivot_user.get('FIO', '')
                # Получаем и нормализуем телефоны
                phones_raw = pivot_user.get('Phones', '')

                # Нормализуем строку и получаем список телефонов
                all_normalized_phones = normalize_phones_string(phones_raw) if phones_raw else []

                # Фильтруем только мобильные (начинаются с +7 и имеют 11 цифр после +)
                mobile_phones = [phone for phone in all_normalized_phones if
                                 phone.startswith('+7') and len(re.sub(r'\D', '', phone)) == 11]

                if not mobile_phones:
                    logger.info(f"Пропускаем {fio} (СНИЛС: {snils}) - нет мобильных телефонов")
                    skipped_count += 1
                    continue

                if snils not in auth_users:
                    # Пользователь еще отсутствует в авторизационной таблице - создаем записи по каждому МОБИЛЬНОМУ телефону
                    for phone in mobile_phones:
                        auth_record = {
                            'SNILS': snils,
                            'FIO': fio,
                            'Phone': phone,
                            'Role': role.value,
                            'ID_messenger': ''
                        }
                        logger.debug(f"Создание записи: телефон={phone}, роль={role.value}")
                        success = await create_auth(auth_record)
                        if success:
                            created_count += 1
                    logger.info(f"Созданы {len(mobile_phones)} записи(ей) для {fio}")
                else:
                    logger.info(f"Существующий пользователь {fio} (СНИЛС: {snils}) - проверяем обновления")
                    existing_records = auth_users[snils]

                    # Обновляем FIO и роль во ВСЕХ существующих записях
                    records_to_update = []
                    for record in existing_records:
                        if record.get('FIO') != fio or record.get('Role') != role.value:
                            records_to_update.append(record)

                    if records_to_update:
                        logger.info(f"Обновляем {len(records_to_update)} записи(ей) для {fio}")
                        for record in records_to_update:
                            logger.debug(
                                f"Обновление записи FIO={record.get('FIO')}→{fio}, Role={record.get('Role')}→{role.value}")
                            success = await update_auth(record['Id'], {'FIO': fio, 'Role': role.value})
                            if success:
                                updated_count += 1
                    else:
                        logger.info(f"Не требуется обновление")

                    # Добавляем новые мобильные телефоны
                    existing_phones = {r.get('Phone', '') for r in existing_records}

                    new_phones = [phone for phone in mobile_phones if phone and phone not in existing_phones]
                    if new_phones:
                        logger.info(f"Добавляем {len(new_phones)} новых телефонов для {fio}")
                        for phone in new_phones:
                            auth_record = {
                                'SNILS': snils,
                                'FIO': fio,
                                'Phone': phone,
                                'Role': role.value,
                                'ID_messenger': ''
                            }
                            logger.info(f"Создание новой записи с телефоном: {phone}")
                            success = await create_auth(auth_record)
                            if success:
                                created_count += 1

            except Exception as e:
                logger.error(f"Ошибка обработки пользователя {snils} ({pivot_user.get('FIO', 'нет ФИО')}): {e}",
                             exc_info=True)

        # Удаляем записи архивных пользователей
        auth_users_updated = await get_auth()
        deleted_count = 0

        for snils, pivot_user in archived_pivot_users.items():
            if snils in auth_users_updated:
                try:
                    records_to_delete = auth_users_updated[snils]
                    logger.info(f"Удаление {len(records_to_delete)} записей архивного пользователя: СНИЛС={snils}")
                    for record in records_to_delete:
                        success = await delete_auth(record['Id'])
                        if success:
                            deleted_count += 1
                except Exception as e:
                    logger.error(f"Ошибка удаления архивного пользователя {snils}: {e}", exc_info=True)

        logger.info("Синхронизация авторизации завершена")
        logger.info(f"ИТОГО: создано={created_count}, обновлено={updated_count}, удалено={deleted_count}, пропущено={skipped_count}")

    except Exception as e:
        logger.error(f"Критическая ошибка синхронизации таблицы авторизации: {e}")


async def create_pulse(user: User):
    """
    Создает пульс-опросы для нового сотрудника, если дата устройства меньше года назад.
    """
    # Вот тут надо дописать, для каких сотрудников создавать - для вчерашникх, а лучше за всю неделю, у тех у кого нет
    # Берем дату устройства из User
    if not user.date_employment:
        logger.info(f"Нет даты устройства для {user.fio}, пульс-опросы не созданы")
        return

    one_year_ago = datetime.now().date() - relativedelta(years=1)
    if user.date_employment <= one_year_ago:
        logger.info(f"{user.fio} старше года в компании, пульс-опросы не создаем")
        return

    user_dict = {
        'SNILS': user.id,
        'FIO': user.fio,
        'Department': (
            user.employments[0].department.title
            if user.employments and user.employments[0].department
            else None
        ),
        'Date_employment': user.date_employment.isoformat(),
    }

    try:
        creator = PulseTaskCreator()
        created = await creator.create_tasks(user_dict)

        if created:
            logger.info(f"Созданы пульс-опросы для {user.fio}")
        else:
            logger.info(f"Пульс-опросы не требуются для {user.fio}")

    except Exception as e:
        logger.error(f"Ошибка создания пульс-опросов для {user.fio}: {e}")

# _________________________________________________
#              ЗАПУСКИ СИНХРОНИЗАЦИЙ

async def start_sync_scheduler():
    logger.info("Планировщик задач запущен")

    await asyncio.gather(
        run_daily_task(
            name="Синхронизация авторизационной таблицы",
            times=sync_auth_times,
            task=sync_auth,
        ),
        run_daily_task(
            name="Проверка ролей",
            times=roles_check_time,
            task=check_user_roles_daily,
        ),
    )


async def run_daily_task(
    *,
    name: str,
    times: Iterable[time],
    task: Callable[[], Awaitable[None]],
):
    logger.info(
        f"{name} будет запускаться в "
        f"{', '.join(t.strftime('%H:%M') for t in times)} МСК"
    )

    while True:
        now_utc = datetime.utcnow()
        now_msk = now_utc + timedelta(hours=3)

        # ищем ближайшее время запуска
        next_run = min(
            (
                datetime.combine(now_msk.date(), t)
                + (timedelta(days=1) if datetime.combine(now_msk.date(), t) <= now_msk else timedelta())
                for t in times
            )
        )

        await asyncio.sleep((next_run - now_msk).total_seconds())

        try:
            logger.info(f"Запуск задачи: {name}")
            await task()
        except Exception:
            logger.exception(f"Ошибка в задаче {name}")

        await asyncio.sleep(5)  # защита от повторного запуска
