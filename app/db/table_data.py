import logging
from typing import List, Dict

from config import Config
from app.db.nocodb_client import NocoDBClient

logger = logging.getLogger(__name__)


async def fetch_table(table_id: str = "empty", app: str = "HR", limit: int = None, offset: int = None) -> List[Dict]:
    """
    Получает строки таблицы из NocoDB. Обертка над NocoDBClient.get_all
    Аргументом принимает '_id' таблицы.
    Если _id при вызове не указан, то выставляет _id главного меню базы app.
    База app по умолчанию основная - контентная для HR.
    Параметры пагинации передаютя при необходимости.
    Возвращает:
    - List[Dict] при успехе
    - None при критической ошибке
    - [] если таблица существует, но пуста
    """
    try:
        # Запрашиваем токен для нужного приложения — Мавис-HR или база пользователей
        if table_id == "empty" and app == 'USER':
            table_id = Config.PIVOT_TABLE_ID
        elif table_id == "empty" and app == 'PULSE':
            table_id = Config.PULSE_CONTENT_ID
        elif table_id == "empty" and app == 'HR':
            table_id = Config.MAIN_MENU_EMPLOYEE_ID

        print(f"call table {table_id}")

        async with NocoDBClient() as client:
            return await client.get_all(
                table_id=table_id,
                limit=limit if limit else 100,
                offset=offset if offset else 0
            )
    except Exception as e:
        logger.error(f"Ошибка fetch_table {table_id}: {e}")
        return []