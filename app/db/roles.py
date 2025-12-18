import logging
from datetime import datetime, date
from enum import Enum
from typing import List, Optional, Dict
from dateutil.relativedelta import relativedelta

from config import Config
from app.seatable_api.api_base import fetch_table

logger = logging.getLogger(__name__)


class UserRole(str, Enum):
    EMPLOYEE = "employee"
    NEWCOMER = "newcomer"


class RoleChecker:
    """Проверяет и обновляет роли пользователей"""

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

            # Получаем данные из 1С для проверки дат
            users_1c = await self._get_1c_users()

            if not users_1c:
                logger.warning("Нет данных из 1С для проверки ролей")
                return

            # Проверяем каждого новичка
            updated_count = 0
            for user in newcomer_users:
                try:
                    updated = await self._check_user_role(user, users_1c)
                    if updated:
                        updated_count += 1
                except Exception as e:
                    logger.error(f"Ошибка проверки пользователя {user.get('FIO')}: {e}")

            logger.info(f"Проверка ролей завершена. Обновлено: {updated_count}/{len(newcomer_users)}")

        except Exception as e:
            logger.error(f"Ошибка при проверке ролей: {e}")


    async def _get_newcomer_users(self) -> List[Dict]:
        """
        Получает пользователей с ролью newcomer
        """
        try:
            users = await fetch_table(
                table_id=Config.SEATABLE_USERS_TABLE_ID,
                app='USER'
            )

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


    async def _get_1c_users(self) -> List[Dict]:
        """
        Получает данные пользователей из сводной таблицы пользователей
        """
        try:
            users = await fetch_table(
                table_id=Config.SEATABLE_PIVOT_TABLE_ID,
                app='USER'
            )

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
        user_snils = user.get('Name')  # СНИЛС
        if not user_snils:
            logger.warning(f"У пользователя нет СНИЛС: {user.get('FIO')}")
            return False

        # Ищем пользователя в данных 1С
        user_1c = None
        for u in users_1c:
            if u.get('Name') == user_snils:
                user_1c = u
                break

        if not user_1c:
            logger.warning(f"Пользователь не найден в 1С: {user.get('FIO')} ({user_snils})")
            return False

        # Получаем дату устройства из сводной таблицы пользователей
        employment_date_str = user_1c.get('Data_employment')
        employment_date = self._parse_date(employment_date_str)

        # Проверяем, является ли еще новичком
        is_still_newcomer = self._is_still_newcomer(employment_date)

        if not is_still_newcomer:
            # Меняем роль на employee
            row_id = user.get('_id')
            if not row_id:
                logger.error(f"Нет row_id для пользователя {user.get('FIO')}")
                return False

            update_data = {
                'Role': 'employee'
            }

            logger.info(f"Роль обновлена: {user.get('FIO')} -> employee")
            return True
        else:
            logger.error(f"Ошибка обновления роли: {user.get('FIO')}")
            return False


# Глобальный экземпляр
role_checker = RoleChecker()


async def check_user_roles_daily():
    """
    Основная функция для ежедневной проверки ролей
    """
    logger.info("Запуск ежедневной проверки ролей пользователей")
    await role_checker.check_and_update_roles()