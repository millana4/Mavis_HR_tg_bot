import asyncio
import logging
import pprint
import re
from collections import defaultdict
from datetime import datetime, time, timedelta
from typing import List, Dict, Iterable, Callable, Awaitable

from dateutil.relativedelta import relativedelta

from app.db.nocodb_client import NocoDBClient
from app.db.organization import Company, Department, CompanySegment
from app.db.roles import check_user_roles_daily, UserRole
from app.db.users import User, Employment
from app.db.table_data import fetch_table
from app.services.pulse_creator import PulseTaskCreator
from app.services.utils import normalize_phones_string, values_to_set, phones_to_set, surname_to_str
from config import Config

logger = logging.getLogger(__name__)


# Время запуска синхронизации с выгрузкой 1С
sync_times = [time(12, 00), time(16, 0)]

# Время проверки ролей
roles_check_time = [time(14, 0)]


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
            print("ybctuj")
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
#            МЕТОДЫ ДЛЯ БАЗЫ ДАННЫХ

# Методы для сводной таблицы

async def create_pivot(user_data: Dict) -> bool:
    """
    Создает пользователя в сводной таблице NocoDB
    """
    try:
        async with NocoDBClient() as client:
            result = await client.create_record(table_id=Config.PIVOT_TABLE_ID, data=user_data)

            if result:
                logger.info(f"Пользователь создан в сводной таблице: {user_data.get('FIO')}")
                return True
            else:
                logger.error(f"Ошибка создания пользователя в сводной таблице: {user_data.get('FIO')}")
                return False

    except Exception as e:
        logger.error(f"Ошибка при создании пользователя в сводной таблице: {str(e)}")
        return False


async def update_pivot(record_id: str, user_data: Dict) -> bool:
    """
    Обновляет пользователя в сводной таблице NocoDB
    """
    try:
        async with NocoDBClient() as client:
            await client.update_record(
                table_id=Config.PIVOT_TABLE_ID,
                record_id=record_id,
                data=user_data
            )

            logger.info(f"Пользователь обновлен в сводной таблице: {user_data.get('FIO')}")
            return True

    except Exception as e:
        logger.error(f"Ошибка при обновлении пользователя в сводной таблице: {str(e)}")
        return False


async def archive_pivot(record_id: str, user_data: Dict) -> bool:
    """
    Архивирует пользователя в сводной таблице NocoDB (оставляет только СНИЛС и дату устройства).
    Дата устройства должна оставаться на случай, если сотрудник переоформится в другую компанию,
    чтобы ему потом не создавались пульс-опросы как новому сотруднику от новой даты.
    """
    try:
        # Сохраняем только СНИЛС и дату устройства
        archived_data = {
            'Name': user_data.get('Name'),  # СНИЛС
            'Date_employment': user_data.get('Date_employment'),  # Дата устройства
            'FIO': None,
            'Previous_surname': None,
            'Company_segment': None,
            'Companies': None,
            'Departments': None,
            'Positions': None,
            'Internal_numbers': None,
            'Email_mavis': None,
            'Email_other': None,
            'Email_votonia': None,
            'Phones': None,
            'Location': None,
            'Photo': None,
            'Is_archived': True  # Флаг архивации
        }

        async with NocoDBClient() as client:
            await client.update_record(
                table_id=Config.PIVOT_TABLE_ID,
                record_id=record_id,
                data=archived_data
            )

            logger.info(f"Пользователь архивирован в сводной таблице (СНИЛС: {user_data.get('Name')})")
            return True

    except Exception as e:
        logger.error(f"Ошибка при архивации пользователя: {str(e)}")
        return False


# Методы для авторизационной таблицы

async def get_auth() -> Dict[str, List[Dict]]:
    """
    Получает всех пользователей из таблицы авторизации NocoDB
    Возвращает словарь {snils: [записи_по_телефонам]}
    """
    try:
        async with NocoDBClient() as client:
            auth_users = await client.get_all(table_id=Config.AUTH_TABLE_ID)

        if not auth_users:
            return {}

        # Группируем по СНИЛС, так как у одного пользователя может быть несколько записей
        grouped_by_snils = {}
        for user in auth_users:
            snils = user.get('Name')
            if snils:
                if snils not in grouped_by_snils:
                    grouped_by_snils[snils] = []
                grouped_by_snils[snils].append(user)

        return grouped_by_snils

    except Exception as e:
        logger.error(f"Ошибка получения пользователей из таблицы авторизации: {e}")
        return {}


async def create_auth(auth_record: Dict) -> bool:
    """
    Создает запись пользователя в таблице авторизации NocoDB
    """
    try:
        async with NocoDBClient() as client:
            result = await client.create_record(
                table_id=Config.AUTH_TABLE_ID,
                data=auth_record
            )

            if result:
                logger.info(f"Создана запись в авторизационной таблице: {auth_record.get('FIO')}")
                return True
            else:
                logger.error(f"Ошибка создания записи в авторизационной таблице: {auth_record.get('FIO')}")
                return False

    except Exception as e:
        logger.error(f"Ошибка создания записи в авторизационной таблице: {e}")
        return False


async def update_auth(record_id: str, auth_record: Dict) -> bool:
    """
    Обновляет запись пользователя в таблице авторизации NocoDB
    """
    try:
        async with NocoDBClient() as client:
            await client.update_record(
                table_id=Config.AUTH_TABLE_ID,
                record_id=record_id,
                data=auth_record
            )

            logger.info(f"Обновлена запись в авторизационной таблице: {record_id}")
            return True

    except Exception as e:
        logger.error(f"Ошибка обновления записи в авторизационной таблице: {e}")
        return False


async def delete_auth(record_id: str) -> bool:
    """
    Удаляет запись пользователя из таблицы авторизации NocoDB
    """
    try:
        async with NocoDBClient() as client:
            deleted = await client.delete_record(
                table_id=Config.AUTH_TABLE_ID,
                record_id=record_id
            )

            if deleted:
                logger.info(f"Удалена запись из авторизационной таблицы: {record_id}")
                return True
            else:
                logger.error(f"Ошибка удаления записи из авторизационной таблицы: {record_id}")
                return False

    except Exception as e:
        logger.error(f"Ошибка удаления записи из авторизационной таблицы: {e}")
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

        # Добавляем паузу секунды перед следующей синхронизацией, так как в авторизации используется сводная таблица
        await asyncio.sleep(5)

        # Повторно получаю актуальные данные
        updated_users_in_pivot = await get_pivot_table_users()

        # Синхронизация авторизационной таблицы
        await sync_auth(updated_users_in_pivot)

        logger.info("Синхронизация 1С завершена")

    except Exception as e:
        logger.error(f"Ошибка синхронизации 1С: {e}")


async def sync_pivot(users_from_1c: Dict[str, User], users_in_pivot: Dict[str, Dict]):
    """Синхронизация пользователей со сводной таблицей по ключевым данным"""

    logger.info("Начало синхронизация пользователей со сводной таблицей")
    logger.info(f"Получено из 1С: {len(users_from_1c)} пользователей")
    logger.info(f"В сводной таблице: {len(users_in_pivot)} пользователей")

    created_count = 0
    updated_count = 0
    unchanged_count = 0
    archived_count = 0
    error_count = 0

    # Обработка пользователей из 1С
    for snils, user in users_from_1c.items():
        try:
            logger.debug(f"Обработка пользователя: СНИЛС={snils}, ФИО={user.fio}")

            new_data = user.to_pivot_table_format()
            date_was_added_now = False

            if snils in users_in_pivot:
                existing_data = users_in_pivot[snils]
                logger.debug(f"Пользователь существует в сводной таблице")

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
                    logger.info(f"Обновление данных пользователя: {user.fio} (СНИЛС: {snils})")
                    new_data['Is_archived'] = False
                    success = await update_pivot(existing_data['_id'], new_data)
                    if success:
                        updated_count += 1
                        logger.info(f"Обновлён: {user.fio}")

                        # Если дата добавилась и она меньше года — создаём пульс-опросы
                        if date_was_added_now:
                            one_year_ago = datetime.now().date() - relativedelta(years=1)
                            if user.date_employment and user.date_employment >= one_year_ago:
                                logger.info(f"Дата устройства добавлена, создаём пульс-опросы для {user.fio}")
                                await create_pulse(user)
                else:
                    logger.info(f"Без изменений: {user.fio}")
                    unchanged_count += 1

            else:
                # Новый пользователь
                logger.info(f"Новый пользователь: {user.fio} (СНИЛС: {snils})")
                success = await create_pivot(new_data)
                if success:
                    created_count += 1

                    # Пульс-опросы для новых сотрудников с датой меньше года работы
                    if user.date_employment:
                        one_year_ago = datetime.now().date() - relativedelta(years=1)
                        if user.date_employment >= one_year_ago:
                            logger.info(f"Создаём пульс-опросы для нового сотрудника {user.fio}")
                            await create_pulse(user)
                    else:
                        logger.debug(f"У пользователя {user.fio} нет даты устройства, пульс-опросы не создаются")

        except Exception as e:
            logger.error(f"Ошибка обработки пользователя {user.fio} (СНИЛС: {snils}): {e}", exc_info=True)
            error_count += 1

    # Архивирование пользователей, которых больше нет в 1С
    users_to_archive = {
        snils for snils in users_in_pivot.keys()
        if snils not in users_from_1c
           and not users_in_pivot[snils].get('Is_archived', False)
    }

    for snils in users_to_archive:
        try:
            user_data = users_in_pivot[snils]
            fio = user_data.get('FIO', 'нет ФИО')

            success = await archive_pivot(user_data['_id'], user_data)
            if success:
                archived_count += 1
                logger.info(f"Архивирован: {fio} (СНИЛС: {snils})")
        except Exception as e:
            logger.error(f"Ошибка архивации пользователя {snils}: {e}", exc_info=True)
            error_count += 1

    logger.info("Синхронизация сводной таблицы завершена")
    logger.info(f"ИТОГО: создано={created_count}, обновлено={updated_count}, без изменений={unchanged_count}, "
                f"архивировано={archived_count}, ошибок={error_count}")


async def sync_auth(pivot_users):
    """
    Синхронизация таблицы авторизации на основе данных из сводной таблицы.
    Только активные пользователи (не архивные).
    """
    logger.info("Начало синхронизации таблицы авторизации")

    try:
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
                            'Name': snils,
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
                            success = await update_auth(record['_id'], {'FIO': fio, 'Role': role.value})
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
                                'Name': snils,
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
                        success = await delete_auth(record['_id'])
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