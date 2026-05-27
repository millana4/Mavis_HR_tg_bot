import logging

from typing import Dict, List

from app.db.nocodb_client import NocoDBClient
from app.services.utils import mask_pii
from config import Config


logger = logging.getLogger(__name__)



# Методы для авторизационной таблицы

async def read_auth() -> Dict[str, List[Dict]]:
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
            snils = user.get('SNILS')
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
                logger.info(f"Создана запись в авторизационной таблице: {mask_pii(auth_record.get('FIO'))}")
                return True
            else:
                logger.error(f"Ошибка создания записи в авторизационной таблице: {mask_pii(auth_record.get('FIO'))}")
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

            logger.debug(f"Обновлена запись в авторизационной таблице: {record_id}")
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
