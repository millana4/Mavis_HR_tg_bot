import asyncio
import pprint
import time
import aiohttp
import logging
from typing import List, Dict, Optional

from config import Config

logger = logging.getLogger(__name__)

# Глобальный кэш токена основного приложения
_token_app_cache: Dict[str, Optional[Dict]] = {
    "token_data": None,
    "timestamp": 0
}

# Глобальный кэш токена базы пользователей
_token_user_cache: Dict[str, Optional[Dict]] = {
    "token_data": None,
    "timestamp": 0
}

# Глобальный кэш токена базы пульс-опросов
_token_pulse_cache: Dict[str, Optional[Dict]] = {
    "token_data": None,
    "timestamp": 0
}

_TOKEN_TTL = 244800  # время жизни токена в секундах — 68 часов


async def get_base_token(app='HR') -> Optional[Dict]:
    """
    Получает временный токен для синхронизации по Апи.

    На вход нужно передать:
    - Если нужен токен для базы пользователей, передать 'USER'.
    - Если нужен токен для основного приложения, то либо передать 'HR', либо ничего не передавать.

    Возвращает словарь:
    {'access_token': 'token_string',
    'app_name': 'mavis-onboarding',
    'dtable_db': 'https://server_name/dtable-db/',
    'dtable_name': 'Mavis_onboarding',
    'dtable_server': 'https://server_name/dtable-server/',
    'dtable_socket': 'https://server_name/',
    'dtable_uuid': '5ce74477-6800-492d-b92e-00d9cd0589a6',
    'workspace_id': 11}
    """

    # Запрашиваем из кеша токен для основного приложения HR или для телефонного справочника —
    # в зависимости от того, что передано на вход
    now = time.time()

    if app == 'USER':
        cached = _token_user_cache["token_data"]
        cached_time = _token_user_cache["timestamp"]
    elif app == 'PULSE':
        cached = _token_pulse_cache["token_data"]
        cached_time = _token_pulse_cache["timestamp"]
    else:
        cached = _token_app_cache["token_data"]
        cached_time = _token_app_cache["timestamp"]

    if cached and (now - cached_time) < _TOKEN_TTL:
        return cached

    # URL одинаковый для всех приложений
    url = f"{Config.SEATABLE_SERVER}/api/v2.1/dtable/app-access-token/"

    # В заголовок передаем ключ API для основного приложения HR или для базы пользователей USER, или базы пульс-опросов
    if app == 'USER':
        headers = {
            "accept": "application/json",
            "authorization": f"Bearer {Config.SEATABLE_API_USER_TOKEN}"
        }
    elif app == 'PULSE':
        headers = {
            "accept": "application/json",
            "authorization": f"Bearer {Config.SEATABLE_API_PULSE_TOKEN}"
        }
    else:
        headers = {
            "accept": "application/json",
            "authorization": f"Bearer {Config.SEATABLE_API_APP_TOKEN}"
        }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()
                token_data = await response.json()
                logger.debug("Base token successfully obtained and cached")

                # Обновляем кэш
                if app == 'USER':
                    _token_user_cache["token_data"] = token_data
                    _token_user_cache["timestamp"] = now
                elif app == 'PULSE':
                    _token_pulse_cache["token_data"] = token_data
                    _token_pulse_cache["timestamp"] = now
                else:
                    _token_app_cache["token_data"] = token_data
                    _token_app_cache["timestamp"] = now

                return token_data

    except aiohttp.ClientError as e:
        logger.error(f"API request failed: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")

    return None


async def fetch_table(table_id: str = '0000', app: str = "HR") -> List[Dict]:
    """
    Получает строки таблицы.
    Аргументом принимает '_id'. В http таблицы указан как tid.
    Если _id при вызове не указан, то выставляет _id главного меню — 0000.
    """
    # Запрашиваем токен для нужного приложения — Мавис-HR или база пользователей
    if app == 'USER':
        token_data = await get_base_token('USER')
    elif app == 'PULSE':
        token_data = await get_base_token('PULSE')
    else:
        token_data = await get_base_token("HR")

    if not token_data:
        logger.error("Не удалось получить токен SeaTable")
        return []

    url = f"{token_data['dtable_server'].rstrip('/')}/api/v1/dtables/{token_data['dtable_uuid']}/rows/"

    headers = {
        "Authorization": f"Bearer {token_data['access_token']}",
        "Accept": "application/json"
    }

    params = {"table_id": table_id}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, params=params) as response:
            if response.status == 200:
                data = await response.json()
                logger.debug(f"Успешный запрос: {url} {params}")
                return data.get("rows", [])

            # Если ошибка 404 - пробуем сбросить токен и запросить новый
            if response.status == 404:
                logger.info(f"Таблица {table_id} не найдена, сбрасываем токен")

                # Сбрасываем кеш токена
                if app == 'USER':
                    _token_user_cache["token_data"] = None
                    _token_user_cache["timestamp"] = 0
                    token_data = await get_base_token('USER')
                elif app == 'PULSE':
                    _token_pulse_cache["token_data"] = None
                    _token_pulse_cache["timestamp"] = 0
                    token_data = await get_base_token('PULSE')
                else:
                    _token_app_cache["token_data"] = None
                    _token_app_cache["timestamp"] = 0
                    token_data = await get_base_token()

                if token_data:
                    # Пробуем запросить с новым токеном
                    url = f"{token_data['dtable_server'].rstrip('/')}/api/v1/dtables/{token_data['dtable_uuid']}/rows/"
                    headers["Authorization"] = f"Bearer {token_data['access_token']}"

                    async with session.get(url, headers=headers, params=params) as retry_response:
                        if retry_response.status == 200:
                            data = await retry_response.json()
                            logger.info(f"Успешный запрос после сброса токена")
                            return data.get("rows", [])

            error_text = await response.text()
            logger.debug(f"Ошибка: {response.status} - {error_text}")

    logger.error(f"Все варианты не сработали для table_id: {table_id}")
    return []


async def get_metadata(app: str = "HR") -> Optional[Dict[str, str]]:
    """Функция возвращает метаданные любой таблицы."""
    # Запрашиваем токен для нужного приложения — Мавис-HR или телефонный справочник
    if app == 'USER':
        token_data = await get_base_token('USER')
    elif app == 'PULSE':
        token_data = await get_base_token('PULSE')
    else:
        token_data = await get_base_token()

    if not token_data:
        logger.error("Не удалось получить токен SeaTable")
        return None

    url = f"{token_data['dtable_server'].rstrip('/')}/api/v1/dtables/{token_data['dtable_uuid']}/metadata/"

    headers = {
        "Authorization": f"Bearer {token_data['access_token']}",
        "Accept": "application/json"
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                metadata = await response.json()
                return metadata

    logger.error("Все варианты endpoints вернули ошибку")
    return None


# Отладочный скрипт для вывода ответов json по API SeaTable
# if __name__ == "__main__":
#     async def main():
#         print("БАЗОВЫЙ ТОКЕН")
#         token_data = await get_base_token("HR")
#         pprint.pprint(token_data)
#
#         print("ТАБЛИЦА")
#         menu_rows = await fetch_table(table_id='PYlV', app='HR')
#         pprint.pprint(menu_rows)
#
#         print("ДРУГАЯ ТАБЛИЦА")
#         menu_rows = await fetch_table(table_id='0000', app='HR')
#         pprint.pprint(menu_rows)
#
#         print("МЕТАДАННЫЕ ТАБЛИЦ")
#         metadata = await get_metadata('PULSE')
#         pprint.pprint(metadata)
#
#     asyncio.run(main())