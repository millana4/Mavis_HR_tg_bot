import logging
from cachetools import TTLCache
from typing import Dict, Any, Optional, List

from config import Config


logger = logging.getLogger(__name__)


class AppStates:
    CURRENT_MENU = "current_menu"
    FORM_DATA = "form_data"
    WAITING_FOR_SEARCH_TYPE = "waiting_for_search_type"
    WAITING_FOR_NAME_SEARCH = "waiting_for_name_search"
    WAITING_FOR_DEPARTMENT_SEARCH = "waiting_for_department_search"
    WAITING_FOR_COMPANY_GROUP_SEARCH = "waiting_for_company_group_search"
    WAITING_FOR_SHOP_TITLE_SEARCH = "waiting_for_shop_title_search"
    WAITING_FOR_DRUGSTORE_TITLE_SEARCH = "waiting_for_drugstore_title_search"
    USER_ROLE = "user_role"


class StateManager:
    def __init__(self, maxsize=1000, ttl=3600):
        self._cache = TTLCache(maxsize=maxsize, ttl=ttl)

        # ID главных меню для разных ролей
        self.SEATABLE_MAIN_MENU_EMPLOYEE_ID = Config.SEATABLE_MAIN_MENU_EMPLOYEE_ID
        self.SEATABLE_MAIN_MENU_NEWCOMER_ID = Config.SEATABLE_MAIN_MENU_NEWCOMER_ID


    async def update_data(self, user_id: int, **kwargs):
        """Основной метод для обновления любых данных пользователя"""
        user_data = self._cache.get(user_id, {})
        user_data.update(kwargs)
        self._cache[user_id] = user_data
        logger.info(f"User {user_id} data updated: {list(kwargs.keys())}")


    async def get_data(self, user_id: int) -> Dict[str, Any]:
        """Получить все данные пользователя"""
        return self._cache.get(user_id, {}).copy()


    async def get_user_role(self, user_id: int) -> Optional[str]:
        """Получить роль пользователя"""
        user_data = self._cache.get(user_id, {})
        return user_data.get(AppStates.USER_ROLE)


    async def set_user_role(self, user_id: int, role: str):
        """Установить роль пользователя"""
        await self.update_data(user_id, **{AppStates.USER_ROLE: role})
        logger.info(f"User {user_id} role set to: {role}")


    async def get_current_menu(self, user_id: int) -> Optional[str]:
        """Получить текущее меню пользователя"""
        user_data = self._cache.get(user_id, {})
        return user_data.get(AppStates.CURRENT_MENU)


    async def set_current_menu(self, user_id: int, menu_id: str):
        """Установить текущее меню пользователя"""
        await self.update_data(user_id, **{AppStates.CURRENT_MENU: menu_id})


    async def get_main_menu_id(self, user_id: int) -> str:
        """Получить ID главного меню в зависимости от роли пользователя"""
        user_data = self._cache.get(user_id, {})
        role = user_data.get(AppStates.USER_ROLE)

        if role == "newcomer" and self.SEATABLE_MAIN_MENU_NEWCOMER_ID:
            return self.SEATABLE_MAIN_MENU_NEWCOMER_ID
        else:
            # По умолчанию возвращаем меню для employee
            return self.SEATABLE_MAIN_MENU_EMPLOYEE_ID


    async def clear(self, user_id: int):
        """Очистить данные пользователя"""
        if user_id in self._cache:
            del self._cache[user_id]
            logger.info(f"User {user_id} data cleared")


    async def is_user_employee(self, user_id: int) -> bool:
        """Проверить, является ли пользователь сотрудником"""
        role = await self.get_user_role(user_id)
        return role == "employee"


    async def is_user_newcomer(self, user_id: int) -> bool:
        """Проверить, является ли пользователь новичком"""
        role = await self.get_user_role(user_id)
        return role == "newcomer"


    # Методы для навигации (специализированные методы)
    async def navigate_to_menu(self, user_id: int, menu_id: str):
        """Переход в новое меню - добавляем в историю"""
        user_data = self._cache.get(user_id, {})

        # Инициализируем историю если её нет
        if 'navigation_history' not in user_data:
            user_data['navigation_history'] = []

        # Добавляем текущее меню в историю
        if 'current_menu' in user_data:
            user_data['navigation_history'].append(user_data['current_menu'])

        # Устанавливаем новое меню
        user_data['current_menu'] = menu_id
        self._cache[user_id] = user_data

        logger.info(f"User {user_id} navigated to menu: {menu_id}")


    async def navigate_to_main_menu(self, user_id: int) -> str:
        """Переход в главное меню в зависимости от роли пользователя"""
        main_menu_id = await self.get_main_menu_id(user_id)
        await self.navigate_to_menu(user_id, main_menu_id)
        return main_menu_id


    async def navigate_back(self, user_id: int) -> Optional[str]:
        """Возврат к предыдущему меню"""
        user_data = self._cache.get(user_id, {})

        if not user_data.get('navigation_history'):
            logger.debug(f"No navigation history for user {user_id}, returning to main menu")
            # Если истории нет - возвращаем главное меню по роли
            main_menu_id = await self.get_main_menu_id(user_id)
            user_data['current_menu'] = main_menu_id
            self._cache[user_id] = user_data
            return main_menu_id

        # Получаем предыдущее меню из истории
        previous_menu = user_data['navigation_history'].pop()
        user_data['current_menu'] = previous_menu
        self._cache[user_id] = user_data

        logger.debug(f"User {user_id} navigated back to: {previous_menu}")
        return previous_menu


    async def get_navigation_history(self, user_id: int) -> List[str]:
        """Получить историю навигации пользователя"""
        user_data = self._cache.get(user_id, {})
        return user_data.get('navigation_history', []).copy()


# Глобальный экземпляр
state_manager = StateManager()