import logging

from typing import Dict, List

from app.db.nocodb_client import NocoDBClient
from config import Config


logger = logging.getLogger(__name__)


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
