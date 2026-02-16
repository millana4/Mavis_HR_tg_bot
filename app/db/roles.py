import logging
from datetime import datetime, date
from enum import Enum
from typing import List, Optional, Dict
from dateutil.relativedelta import relativedelta

from config import Config
from app.db.table_data import fetch_table
from app.db.nocodb_client import NocoDBClient
from app.db.sync_db_executor import update_auth

logger = logging.getLogger(__name__)


class UserRole(str, Enum):
    EMPLOYEE = "employee"
    NEWCOMER = "newcomer"


class RoleChecker:
    """Проверяет и обновляет роли пользователей"""

    async def get_role(user_id: str) -> Optional[str]:
        """Получает роль пользователя из NocoDB"""
        try:
            async with NocoDBClient() as client:
                # Ищем пользователя по ID_messenger
                where_filter = f"(ID_messenger,eq,{user_id})"
                users = await client.get_all(
                    table_id=Config.AUTH_TABLE_ID,
                    where=where_filter,
                    limit=1
                )

            if not users:
                logger.warning(f"Пользователь {user_id} не найден в таблице")
                return None

            user = users[0]
            role = user.get('Role')
            logger.info(f"Найден пользователь: {user.get('FIO')}, его роль: {role}")
            return role

        except Exception as e:
            logger.error(f"Ошибка получения роли для {user_id}: {str(e)}", exc_info=True)
            return None

    async def change_user_role(user_id: int, new_role: str) -> bool:
        """Изменяет роль пользователя в таблице NocoDB"""
        try:
            async with NocoDBClient() as client:
                # Получаем пользователя по ID_messenger
                where_filter = f"(ID_messenger,eq,{user_id})"
                users = await client.get_all(
                    table_id=Config.AUTH_TABLE_ID,
                    where=where_filter,
                    limit=1
                )

                if not users:
                    logger.error(f"User {user_id} not found")
                    return False

                user_row = users[0]
                record_id = user_row.get('Id')

                if not record_id:
                    logger.error("User row has no ID")
                    return False

                update_data = {"Role": new_role}

                await client.update_record(
                    table_id=Config.AUTH_TABLE_ID,
                    record_id=record_id,
                    data=update_data
                )

                logger.info(f"Role changed to {new_role} for user {user_id}")
                return True

        except Exception as e:
            logger.error(f"Error changing role for {user_id}: {str(e)}", exc_info=True)
            return False


    async def check_and_update_roles(self) -> None:
        """
        Основная функция: проверяет роли новичков и обновляет если нужно
        """
        logger.info("Начало проверки ролей пользователей")

        try:
            # Получаем всех пользователей с ролью newcomer
            newcomer_users = await self._get_newcomer_users()

            if not newcomer_users:
                logger.info("Нет пользователей с ролью newcomer")
                return

            logger.info(f"Найдено {len(newcomer_users)} пользователей с ролью newcomer")

            # Получаем данные из сводной таблицы для проверки дат
            users_pivot = await self._get_users()

            if not users_pivot:
                logger.warning("Нет данных для проверки ролей")
                return

            # Проверяем каждого новичка
            updated_count = 0
            for user in newcomer_users:
                try:
                    need_update = await self._check_user_role(user, users_pivot)
                    if need_update:
                        update_data = {
                            'Role': 'employee'
                        }

                        success = await update_auth(user.get('Id'), update_data)
                        updated_count += 1
                        if success:
                            logger.info(f"Роль пользователя {user.get('FIO')} изменилась: employee ")
                except Exception as e:
                    logger.error(f"Ошибка проверки пользователя {user.get('FIO')}: {e}")

            logger.info(f"Проверка ролей завершена. Обновлено: {updated_count}/{len(newcomer_users)}")

        except Exception as e:
            logger.error(f"Ошибка при проверке ролей: {e}")


    async def _get_newcomer_users(self) -> List[Dict]:
        """
        Получает пользователей из авторизационной таблицы с ролью newcomer
        """
        try:
            users = await fetch_table(table_id=Config.AUTH_TABLE_ID, app='USER')

            if not users:
                return []

            # Фильтруем новичков
            newcomer_users = []
            for user in users:
                if user.get('Role') == 'newcomer':
                    newcomer_users.append(user)

            return newcomer_users

        except Exception as e:
            logger.error(f"Ошибка получения пользователей: {e}")
            return []


    async def _get_users(self) -> List[Dict]:
        """
        Получает данные пользователей из сводной таблицы пользователей
        """
        try:
            users = await fetch_table(table_id=Config.PIVOT_TABLE_ID, app='USER')

            return users if users else []

        except Exception as e:
            logger.error(f"Ошибка получения данных из 1С: {e}")
            return []


    def _parse_date(self, date_str: Optional[str]) -> Optional[date]:
        """Парсит дату из строки"""
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, '%Y-%m-%d').date()
        except (ValueError, TypeError):
            return None


    def _is_still_newcomer(self, employment_date: Optional[date]) -> bool:
        """
        Проверяет, является ли пользователь еще новичком (< 3 месяцев)
        """
        if not employment_date:
            return True  # Если даты нет, оставляем как есть

        three_months_ago = datetime.now().date() - relativedelta(months=3)
        return employment_date > three_months_ago


    async def _check_user_role(self, user: Dict, users_1c: List[Dict]) -> bool:
        """
        Проверяет и обновляет роль одного пользователя
        """
        user_snils = user.get('SNILS')
        if not user_snils:
            logger.warning(f"У пользователя нет СНИЛС: {user.get('FIO')}")
            return False

        # Ищем пользователя в сводной таблице
        user_1c = None
        for u in users_1c:
            if u.get('SNILS') == user_snils:
                user_1c = u
                break

        if not user_1c:
            logger.warning(f"Пользователь не найден в сводной таблице: {user.get('FIO')} ({user_snils})")
            return False

        # Получаем дату устройства из сводной таблицы пользователей
        employment_date_str = user_1c.get('Date_employment')
        employment_date = self._parse_date(employment_date_str)

        # Проверяем, является ли еще новичком
        is_still_newcomer = self._is_still_newcomer(employment_date)

        if not is_still_newcomer:
            return True
        else:
            return False


# Глобальный экземпляр
role_checker = RoleChecker()


async def check_user_roles_daily():
    """
    Основная функция для ежедневной проверки ролей
    """
    logger.info("Запуск ежедневной проверки ролей пользователей")
    await role_checker.check_and_update_roles()