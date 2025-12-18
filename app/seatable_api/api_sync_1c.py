import logging
import aiohttp
from typing import Dict, Optional, List

from app.seatable_api.api_base import get_base_token, fetch_table
from config import Config

logger = logging.getLogger(__name__)

#____________________________________________________
#  СВОДНАЯ ТАБЛИЦА ПОЛЬЗОВАТЕЛЕЙ ДЛЯ ВСЕГО СЕРВИСА


async def create_pivot(user_data: Dict) -> bool:
    """
    Создает пользователя в сводной таблице
    """
    try:
        # Аналогично create_user_in_table, но для сводной таблицы
        token_data = await get_base_token(app='USER')
        if not token_data:
            logger.error("Не удалось получить токен SeaTable")
            return False

        url = f"{token_data['dtable_server'].rstrip('/')}/api/v1/dtables/{token_data['dtable_uuid']}/rows/"

        headers = {
            "Authorization": f"Bearer {token_data['access_token']}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

        payload = {
            "table_id": Config.SEATABLE_PIVOT_TABLE_ID,
            "row": user_data
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers) as response:
                if response.status in (200, 201):
                    logger.info(f"Пользователь создан в сводной таблице: {user_data.get('FIO')}")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Ошибка создания пользователя в сводной таблице: {response.status} - {error_text}")
                    return False

    except Exception as e:
        logger.error(f"Ошибка при создании пользователя в сводной таблице: {str(e)}")
        return False


async def update_pivot(row_id: str, user_data: Dict) -> bool:
    """
    Обновляет пользователя в сводной таблице
    """
    try:
        token_data = await get_base_token(app='USER')
        if not token_data:
            logger.error("Не удалось получить токен SeaTable")
            return False

        url = f"{token_data['dtable_server'].rstrip('/')}/api/v1/dtables/{token_data['dtable_uuid']}/rows/"

        headers = {
            "Authorization": f"Bearer {token_data['access_token']}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

        payload = {
            "table_id": Config.SEATABLE_PIVOT_TABLE_ID,
            "row_id": row_id,
            "row": user_data
        }

        async with aiohttp.ClientSession() as session:
            async with session.put(url, json=payload, headers=headers) as response:
                if response.status == 200:
                    logger.info(f"Пользователь обновлен в сводной таблице: {user_data.get('FIO')}")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Ошибка обновления пользователя в сводной таблице: {response.status} - {error_text}")
                    return False

    except Exception as e:
        logger.error(f"Ошибка при обновлении пользователя в сводной таблице: {str(e)}")
        return False


async def archive_pivot(row_id: str, user_data: Dict) -> bool:
    """
    Архивирует пользователя в сводной таблице (оставляет только СНИЛС и дату устройства).
    Дата устройства должна оставаться на случай, если сотрудник переоформится в другую компанию,
    чтобы ему потом не создавались пульс-опросы как новому сотруднику от новой даты.
    """
    try:
        token_data = await get_base_token(app='USER')
        if not token_data:
            logger.error("Не удалось получить токен SeaTable")
            return False

        url = f"{token_data['dtable_server'].rstrip('/')}/api/v1/dtables/{token_data['dtable_uuid']}/rows/"

        headers = {
            "Authorization": f"Bearer {token_data['access_token']}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

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

        payload = {
            "table_id": Config.SEATABLE_PIVOT_TABLE_ID,
            "row_id": row_id,
            "row": archived_data
        }

        async with aiohttp.ClientSession() as session:
            async with session.put(url, json=payload, headers=headers) as response:
                if response.status == 200:
                    logger.info(f"Пользователь архивирован в сводной таблице (СНИЛС: {user_data.get('Name')})")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Ошибка архивации пользователя: {response.status} - {error_text}")
                    return False

    except Exception as e:
        logger.error(f"Ошибка при архивации пользователя: {str(e)}")
        return False


#____________________________________________________
#            АВТОРИЗАЦИОННАЯ ТАБЛИЦА

async def get_auth() -> Dict[str, List[Dict]]:
    """
    Получает всех пользователей из таблицы авторизации
    Возвращает словарь {snils: [записи_по_телефонам]}
    """
    try:
        auth_users = await fetch_table(
            table_id=Config.SEATABLE_USERS_TABLE_ID,
            app='USER'
        )

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
    Создает запись пользователя в таблице авторизации
    """
    try:
        token_data = await get_base_token(app='USER')
        if not token_data:
            return False

        url = f"{token_data['dtable_server'].rstrip('/')}/api/v1/dtables/{token_data['dtable_uuid']}/rows/"

        headers = {
            "Authorization": f"Bearer {token_data['access_token']}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

        payload = {
            "table_id": Config.SEATABLE_USERS_TABLE_ID,
            "row": auth_record
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers) as response:
                if response.status in (200, 201):
                    logger.info(f"Создана запись в авторизационной таблице: {auth_record.get('FIO')}")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Ошибка создания записи: {response.status} - {error_text}")
                    return False

    except Exception as e:
        logger.error(f"Ошибка создания записи в авторизационной таблице: {e}")
        return False


async def update_auth(row_id: str, auth_record: Dict) -> bool:
    """
    Обновляет запись пользователя в таблице авторизации
    """
    try:
        token_data = await get_base_token(app='USER')
        if not token_data:
            return False

        url = f"{token_data['dtable_server'].rstrip('/')}/api/v1/dtables/{token_data['dtable_uuid']}/rows/"

        headers = {
            "Authorization": f"Bearer {token_data['access_token']}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

        payload = {
            "table_id": Config.SEATABLE_USERS_TABLE_ID,
            "row_id": row_id,
            "row": auth_record
        }

        async with aiohttp.ClientSession() as session:
            async with session.put(url, json=payload, headers=headers) as response:
                if response.status == 200:
                    logger.info(f"Обновлена запись в авторизационной таблице: {row_id}")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Ошибка обновления записи: {response.status} - {error_text}")
                    return False

    except Exception as e:
        logger.error(f"Ошибка обновления записи в авторизационной таблице: {e}")
        return False


async def delete_auth(row_id: str) -> bool:
    """
    Удаляет запись пользователя из таблицы авторизации
    """
    try:
        token_data = await get_base_token(app='USER')
        if not token_data:
            return False

        url = f"{token_data['dtable_server'].rstrip('/')}/api/v1/dtables/{token_data['dtable_uuid']}/rows/"

        headers = {
            "Authorization": f"Bearer {token_data['access_token']}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

        payload = {
            "table_id": Config.SEATABLE_USERS_TABLE_ID,
            "row_id": row_id
        }

        async with aiohttp.ClientSession() as session:
            async with session.delete(url, json=payload, headers=headers) as response:
                if response.status == 200:
                    logger.info(f"Удалена запись из авторизационной таблицы")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Ошибка удаления записи: {response.status} - {error_text}")
                    return False

    except Exception as e:
        logger.error(f"Ошибка удаления записи из авторизационной таблицы: {e}")
        return False