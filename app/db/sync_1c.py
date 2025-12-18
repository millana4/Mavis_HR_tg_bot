import asyncio
import logging
import pprint
from collections import defaultdict
from datetime import datetime, time, timedelta, date
from typing import List, Dict, Iterable, Callable, Awaitable

from dateutil.relativedelta import relativedelta

from app.db.organization import Company, CompanySegmentDetector, Department, CompanySegment
from app.db.roles import check_user_roles_daily
from app.db.roles import UserRole
from app.db.users import User, Employment
from app.seatable_api.api_base import fetch_table
from app.services.pulse_tasks import PulseTaskCreator
from app.seatable_api.api_sync_1c import update_pivot, create_pivot, archive_pivot, delete_auth, get_auth, create_auth, update_auth
from app.services.utils import normalize_phones_string, values_to_set, phones_to_set, surname_to_str
from config import Config

logger = logging.getLogger(__name__)


# Время запуска синхронизации с выгрузкой 1С
sync_times = [time(11, 49), time(16, 0)]

# Время проверки ролей
roles_check_time = time(14, 00)


#__________________________________________________
#          ПОДГОТОВКА К СИНХРОНИЗАЦИИ

async def get_all_1c_users() -> List[Dict]:
    """
    Получает всех пользователей из 1С - два запроса
    """
    logger.info("Получение данных из 1С")

    # Первый запрос - первые 1000 записей
    first_batch = await fetch_table(table_id=Config.SEATABLE_1C_TABLE_ID, app='USER', limit=1000, start=0)

    if not first_batch:
        return []

    # Второй запрос - остальные записи
    second_batch = await fetch_table(table_id=Config.SEATABLE_1C_TABLE_ID, app='USER', start=1000)

    if second_batch:
        result = first_batch + second_batch
        logger.info(f"Всего получено {len(result)} записей")
        return result
    else:
        return first_batch


def aggregate_1c_users(users_data: List[Dict]) -> Dict[str, User]:
    users_by_snils = defaultdict(list)

    for row in users_data:
        snils = row.get('Name')
        if snils:
            users_by_snils[snils].append(row)

    aggregated_users = {}

    for snils, rows in users_by_snils.items():
        if not rows:
            continue

        # создаём пользователя из первой строки
        user = User.from_1c_data(rows[0])
        if not user:
            continue

        all_phones = set(user.phones)
        previous_surnames = set()
        earliest_date = user.date_employment
        employment_strings = set()
        employments: list[Employment] = []

        for row in rows:
            # предыдущие фамилии
            prev_surname = row.get('Previous_surname')
            if prev_surname:
                previous_surnames.add(prev_surname)

            # дата трудоустройства
            row_date = None
            date_str = row.get('Date_employment')
            if date_str:
                try:
                    row_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                    if not earliest_date or row_date < earliest_date:
                        earliest_date = row_date
                except:
                    pass

            # телефоны
            phone_private = row.get('Phone_private', '')
            if phone_private:
                phones_from_row = normalize_phones_string(phone_private)
                all_phones.update(phones_from_row)
            else:
                phones_from_row = []

            # создаем трудоустройство
            company_name = row.get('Company', '').strip()
            dept_name = row.get('Department', '').strip()
            pos_name = row.get('Position', '').strip()

            company = None
            if company_name:
                company = Company(
                    id=company_name.lower().replace(' ', '_'),
                    title=company_name,
                    segment=CompanySegment.BOTH
                )

            department = None
            if dept_name:
                department = Department(
                    id=dept_name.lower().replace(' ', '_'),
                    title=dept_name
                )

            employment = Employment(
                company=company,
                department=department,
                position=pos_name or None,
                date_employment=row_date,
                is_main=row.get('Is_main') == 'Да'
            )

            employments.append(employment)

            # строки для сравнения (одна на каждое сочетание)
            for phone in phones_from_row or ['']:
                emp_str = (
                    f"{company_name}|{dept_name}|{pos_name}|"
                    f"{row.get('FIO','').strip()}|"
                    f"{row.get('Previous_surname','').strip()}|{phone}"
                )
                employment_strings.add(emp_str)

        # финальная сборка пользователя
        user.employments = employments
        user.employment_strings = employment_strings
        user.previous_surname = list(previous_surnames) if previous_surnames else None
        user.date_employment = earliest_date
        user.phones = list(all_phones)

        aggregated_users[snils] = user

    return aggregated_users


async def get_pivot_table_users() -> Dict[str, Dict]:
    """
    Получает всех пользователей из сводной таблицы
    Возвращает словарь {snils: row_data}
    """
    try:
        pivot_users = await fetch_table(table_id=Config.SEATABLE_PIVOT_TABLE_ID, app='USER')

        if not pivot_users:
            return {}

        return {user.get('Name'): user for user in pivot_users if user.get('Name')}

    except Exception as e:
        logger.error(f"Ошибка получения пользователей из сводной таблицы: {e}")
        return {}


def pivot_data_changed(existing: Dict, new: Dict) -> bool:
    # 1. Простые поля — строгое сравнение
    if (existing.get('FIO') or '') != (new.get('FIO') or ''):
        return True

    if surname_to_str(existing.get('Previous_surname')) != surname_to_str(new.get('Previous_surname')):
        return True

    # 2. Поля-множества
    set_fields = ('Companies', 'Departments', 'Positions')

    for field in set_fields:
        old_set = values_to_set(existing.get(field))
        new_set = values_to_set(new.get(field))
        if old_set != new_set:
            pprint.pprint(f'{field}: {old_set}: {new}')
            return True

    # 3. Телефоны
    old_phones = phones_to_set(existing.get('Phones'))
    new_phones = phones_to_set(new.get('Phones'))

    if old_phones != new_phones:
        return True

    return False

# __________________________________________________________
#            СИНХРОНИЗАЦИЯ С ВЫГРУЗКОЙ 1С

async def process_1c_sync():
    """
    Основная функция синхронизации c 1С
    """
    logger.info("Начало синхронизации c 1С")
    try:
        all_1c_data = await get_all_1c_users()
        if not all_1c_data:
            logger.warning("Нет данных из 1С")
            return

        users_from_1c = aggregate_1c_users(all_1c_data)
        users_in_pivot = await get_pivot_table_users()

        # Вызов синхронизации со сводной таблицей
        await sync_pivot(users_from_1c, users_in_pivot)

        # Синхронизация авторизационной таблицы
        await sync_auth()

        logger.info("Синхронизация 1С завершена")

    except Exception as e:
        logger.error(f"Ошибка синхронизации 1С: {e}")


async def sync_pivot(users_from_1c: Dict[str, User], users_in_pivot: Dict[str, Dict]):
    """Синхронизация пользователей со сводной таблицей по ключевым данным"""

    for snils, user in users_from_1c.items():
        try:
            new_data = user.to_pivot_table_format()
            date_was_added_now = False

            if snils in users_in_pivot:
                existing_data = users_in_pivot[snils]

                # Сохраняем существующую дату устройства
                existing_date = existing_data.get('Date_employment')
                if existing_date:
                    new_data['Date_employment'] = existing_date
                else:
                    # Даты не было — берем из 1С
                    if user.date_employment:
                        new_data['Date_employment'] = user.date_employment.strftime('%Y-%m-%d')
                        date_was_added_now = True
                    else:
                        new_data['Date_employment'] = None

                # Проверка изменений только по ключевому сету
                if pivot_data_changed(existing_data, new_data) or date_was_added_now:
                    await update_pivot(existing_data['_id'], new_data)
                    logger.info(f"Обновлён: {user.fio}")

                    # Если дата добавилась и она меньше года — создаём пульс-опросы
                    if date_was_added_now:
                        one_year_ago = datetime.now().date() - relativedelta(years=1)
                        if user.date_employment and user.date_employment >= one_year_ago:
                            await create_pulse(user)
                else:
                    logger.info(f"Без изменений: {user.fio}")

            else:
                # Новый пользователь
                await create_pivot(new_data)
                logger.info(f"Создан: {user.fio}")

                # Пульс-опросы для новых сотрудников с датой меньше года работы
                if user.date_employment:
                    one_year_ago = datetime.now().date() - relativedelta(years=1)
                    if user.date_employment >= one_year_ago:
                        await create_pulse(user)

        except Exception as e:
            logger.error(f"Ошибка обработки {user.fio}: {e}")

    # Архивирование пользователей, которых больше нет в 1С
    users_to_archive = {
        snils for snils in users_in_pivot.keys()
        if snils not in users_from_1c
           and not users_in_pivot[snils].get('Is_archived', False)
    }

    for snils in users_to_archive:
        try:
            await archive_pivot(users_in_pivot[snils]['_id'], users_in_pivot[snils])
            logger.info(f"Архивирован: {snils}")
        except Exception as e:
            logger.error(f"Ошибка архивации {snils}: {e}")


async def sync_auth():
    """
    Синхронизация таблицы авторизации на основе данных из сводной таблицы.
    Только активные пользователи (не архивные).
    """
    logger.info("Начало синхронизации таблицы авторизации")

    try:
        # Получаем всех пользователей из сводной таблицы
        pivot_users = await get_pivot_table_users()

        # Фильтруем только активных пользователей
        active_pivot_users = {
            snils: user_data
            for snils, user_data in pivot_users.items()
            if not user_data.get('Is_archived', False)
        }

        # Получаем текущих пользователей из авторизационной таблицы
        auth_users = await get_auth()

        # Обрабатываем каждого активного пользователя
        # for snils, pivot_user in active_pivot_users.items():
        for snils, pivot_user in list(active_pivot_users.items())[:10]:
            try:
                # Дата устройства
                date_employment_str = pivot_user.get('Date_employment')
                date_employment = None
                if date_employment_str:
                    try:
                        date_employment = datetime.strptime(date_employment_str, '%Y-%m-%d').date()
                    except Exception:
                        pass

                # Определяем роль
                role = UserRole.EMPLOYEE
                if date_employment:
                    three_months_ago = datetime.now().date() - relativedelta(months=3)
                    if date_employment > three_months_ago:
                        role = UserRole.NEWCOMER

                fio = pivot_user.get('FIO', '')
                phones = pivot_user.get('Phones', [])
                if not isinstance(phones, list):
                    phones = [phones] if phones else []

                if snils not in auth_users:
                    # Пользователь отсутствует - создаем записи по каждому телефону
                    for phone in phones:
                        auth_record = {
                            'Name': snils,
                            'FIO': fio,
                            'Phone': phone,
                            'Role': role.value,
                            'ID_messenger': ''
                        }
                        await create_auth(auth_record)
                    logger.info(f"Созданы записи в авторизационной таблице для: {fio}")
                else:
                    # Пользователь есть - обновляем FIO, телефоны и роль при изменениях
                    existing_records = auth_users[snils]

                    # Обновляем FIO и роль
                    for record in existing_records:
                        if record.get('FIO') != fio or record.get('Role') != role.value:
                            await update_auth(record['_id'], {'FIO': fio, 'Role': role.value})

                    # Добавляем новые телефоны
                    existing_phones = {r.get('Phone', '') for r in existing_records}
                    for phone in phones:
                        if phone and phone not in existing_phones:
                            auth_record = {
                                'Name': snils,
                                'FIO': fio,
                                'Phone': phone,
                                'Role': role.value,
                                'ID_messenger': ''
                            }
                            await create_auth(auth_record)

            except Exception as e:
                logger.error(f"Ошибка обработки пользователя {snils}: {e}")

        # Удаляем записи архивных пользователей
        archived_snils = {snils for snils, user_data in pivot_users.items() if user_data.get('Is_archived', False)}
        for snils in archived_snils:
            if snils in auth_users:
                try:
                    for record in auth_users[snils]:
                        await delete_auth(record['_id'])
                    logger.info(f"Удалены записи архивного пользователя: {snils}")
                except Exception as e:
                    logger.error(f"Ошибка удаления архивного пользователя {snils}: {e}")

        logger.info("Синхронизация авторизации завершена")

    except Exception as e:
        logger.error(f"Ошибка синхронизации таблицы авторизации: {e}")


async def create_pulse(user: User):
    """
    Создает пульс-опросы для нового сотрудника, если дата устройства меньше года назад.
    """
    # Берем дату устройства из User
    if not user.date_employment:
        logger.info(f"Нет даты устройства для {user.fio}, пульс-опросы не созданы")
        return

    one_year_ago = datetime.now().date() - relativedelta(years=1)
    if user.date_employment <= one_year_ago:
        logger.info(f"{user.fio} старше года в компании, пульс-опросы не создаем")
        return

    user_dict = {
        'Name': user.id,  # СНИЛС
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
            name="Синхронизация с 1С",
            times=sync_times,
            task=process_1c_sync,
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


async def wait_until_msk(target_time: time):
    now_utc = datetime.utcnow()
    now_msk = now_utc + timedelta(hours=3)

    next_run = datetime.combine(now_msk.date(), target_time)
    if next_run <= now_msk:
        next_run += timedelta(days=1)

    await asyncio.sleep((next_run - now_msk).total_seconds())