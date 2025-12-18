import logging
import aiohttp
from datetime import datetime

from config import Config
from app.seatable_api.api_base import get_base_token, fetch_table
from app.services.utils import normalize_phone

logger = logging.getLogger(__name__)


async def check_id_messenger(id_messenger: str) -> tuple[bool, str]:
    """
    Функция для проверки доступа и получения роли пользователя.
    Возвращает (has_access, role)
    """
    try:
        token_data = await get_base_token(app='USER')
        if not token_data:
            logger.error("Не удалось получить токен SeaTable")
            return False, "employee"  # default role

        base_url = f"{token_data['dtable_server']}api/v1/dtables/{token_data['dtable_uuid']}/rows/"
        headers = {
            "Authorization": f"Bearer {token_data['access_token']}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        params = {"table_id": Config.SEATABLE_USERS_TABLE_ID}

        async with aiohttp.ClientSession() as session:
            async with session.get(base_url, headers=headers, params=params) as response:
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(f"Ошибка запроса: {response.status}. Ответ: {error_text}")
                    return False, "employee"

                data = await response.json()
                """
                Пример data:
                    [{'FIO': 'kit_company_account',
                      'ID_messenger': 'ХХХХХХХХХХХ',
                      'Phone': '+7ХХХХХХХХХХ',
                      'Role': 'newcomer',
                      '_ctime': '2025-11-24T08:13:29.157+00:00',
                      '_id': 'VbPzvSVARc6Gff-qO_C8LA',
                      '_mtime': '2025-11-25T07:08:27.303+00:00',
                      'Админы': ['WEkWAuJ5StKe-kaNf4cnMA']},
                """

                # Ищем пользователя с совпадающим id_messenger
                for row in data.get("rows", []):
                    if str(row.get("ID_messenger")) == str(id_messenger):
                        role = row.get('Role', 'employee')  # Получаем роль
                        logger.info(f"Найден пользователь с ID_messenger: {id_messenger}, роль: {role}")
                        return True, role  # возвращаем True, role когда пользователь найден

                logger.info(f"Пользователь с ID_messenger {id_messenger} не найден")
                return False, "employee"  # ← Возвращаем False только если пользователь не найден

    except Exception as e:
        logger.error(f"Ошибка при проверке пользователя: {str(e)}", exc_info=True)
        return False, "employee"


async def register_id_messenger(phone: str, id_messenger: str) -> bool:
    """
    Функция для регистрации и получения доступа.
    Обращается по API к Seatable, ищет там пользователя по телефону и записывает его id_messenger.
    """
    try:
        # Получаем токен
        token_data = await get_base_token(app='USER')
        if not token_data:
            logger.error("Не удалось получить токен SeaTable")
            return False

        base_url = f"{token_data['dtable_server']}api/v1/dtables/{token_data['dtable_uuid']}/rows/"
        headers = {
            "Authorization": f"Bearer {token_data['access_token']}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

        # Используем человекочитаемые названия колонок, а не их внутренние ключи
        phone_column = "Phone"  # Колонка с телефонами
        id_messenger_column = "ID_messenger"  # Колонка для id_messenger
        date_registration = "Date_registration"
        role_column = "Role"

        # Получаем параметры
        params = {
            "table_id": Config.SEATABLE_USERS_TABLE_ID,
            "convert_keys": "false"
        }

        async with aiohttp.ClientSession() as session:
            # Запрашиваем все строки
            async with session.get(base_url, headers=headers, params=params) as resp:
                if resp.status != 200:
                    logger.error(f"Ошибка получения данных: {resp.status}")
                    return False

                data = await resp.json()
                rows = data.get("rows", [])

                # Ищем точное совпадение
                matched_row = None
                for row in rows:
                    if phone_column in row:
                        row_phone_normalized = normalize_phone(str(row[phone_column]))
                        if row_phone_normalized == phone:
                            matched_row = row
                            break

                if not matched_row:
                    logger.error(f"Совпадений не найдено. Проверьте в авторизационной таблице {id_messenger}")
                    return False

                row_id = matched_row.get("_id")
                if not row_id:
                    logger.error("У строки нет ID")
                    return False

                logger.info(f"Найдена строка пользователя для обновления (ID: {row_id})")

                # Проверяем роль: если пусто или None - нужно установить 'employee'
                current_role = matched_row.get(role_column)
                needs_role_update = False

                # Если роль отсутствует или пустая - нужно установить 'employee'
                if not current_role or current_role == '':
                    needs_role_update = True

                # Подготовка обновления (основные поля)
                update_data = {
                    "table_id": Config.SEATABLE_USERS_TABLE_ID,
                    "row_id": row_id,
                    "row": {
                        id_messenger_column: str(id_messenger),
                        date_registration: datetime.now().date().strftime('%Y-%m-%d')
                    }
                }

                if needs_role_update:
                    update_data["row"][role_column] = "employee"

                # Отправка обновления
                async with session.put(base_url, headers=headers, json=update_data) as resp:
                    if resp.status != 200:
                        logger.error(f"Ошибка обновления: {resp.status} - {await resp.text()}")
                        return False

                    if needs_role_update:
                        logger.info(f"ID_messenger и роль 'employee' успешно добавлены для телефона {phone}")
                    else:
                        logger.info(f"ID_messenger успешно добавлен для пользователя с телефоном {phone}")
                    return True

    except Exception as e:
        logger.error(f"Критическая ошибка: {str(e)}", exc_info=True)
        return False

