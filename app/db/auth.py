import logging
from datetime import datetime

from app.db.nocodb_client import NocoDBClient
from config import Config

logger = logging.getLogger(__name__)


async def check_id_messenger(id_messenger: str) -> tuple[bool, str]:
    """
    Функция для проверки доступа и получения роли пользователя.
    Возвращает (has_access, role)
    """
    try:
        async with NocoDBClient() as client:
            users = await client.get_all(table_id=Config.AUTH_TABLE_ID)

            for user in users:
                if str(user.get("ID_messenger")) == str(id_messenger):
                    role = user.get('Role', 'employee')
                    logger.info(f"Найден пользователь с ID_messenger: {id_messenger}, роль: {role}")
                    return True, role

            logger.info(f"Пользователь с ID_messenger {id_messenger} не найден")
            return False, "employee"

    except Exception as e:
        logger.error(f"Ошибка при проверке пользователя: {str(e)}", exc_info=True)
        return False, "employee"


async def register_id_messenger(phone: str, id_messenger: str) -> bool:
    """
    Функция для регистрации и получения доступа.
    Ищет пользователя по телефону и записывает его id_messenger.
    """
    try:
        async with NocoDBClient() as client:
            phone_filter = f"(Phone,eq,{phone})"
            users = await client.get_all(table_id=Config.AUTH_TABLE_ID, where=phone_filter, limit=1)

            if not users:
                logger.error(f"Совпадений не найдено. Проверьте в авторизационной таблице {id_messenger}")
                return False

            user = users[0]  # берем первого пользователя из списка, он там один
            user_id = user.get("Id")

            if not user_id:
                logger.error("У строки нет ID")
                return False

            logger.info(f"Найдена строка пользователя для обновления (ID: {user_id})")

            # Проверяем роль: если пусто или None - нужно установить 'employee'
            current_role = user.get("Role")  # получаем роль из записи пользователя
            update_data = {
                "ID_messenger": str(id_messenger),
                "Date_registration": datetime.now().date().strftime('%Y-%m-%d')
            }

            # Если роль отсутствует или пустая - нужно установить 'employee'
            if not current_role or current_role == '':
                update_data["Role"] = "employee"

            # Отправка обновления
            await client.update_record(
                table_id=Config.AUTH_TABLE_ID,
                record_id=user_id,
                data=update_data
            )

            if update_data.get("Role"):
                logger.info(f"ID_messenger и роль 'employee' успешно добавлены для телефона {phone}")
            else:
                logger.info(f"ID_messenger успешно добавлен для пользователя с телефоном {phone}")

            return True

    except Exception as e:
        logger.error(f"Критическая ошибка: {str(e)}", exc_info=True)
        return False