import logging
import sqlite3
import os
from typing import Dict, Any, Optional, List
import json
from pathlib import Path

from config import Config

logger = logging.getLogger(__name__)


class AppStates:
    CURRENT_MENU = "current_menu"
    FORM_DATA = "form_data"
    WAITING_FOR_SEARCH_TYPE = "waiting_for_search_type"
    WAITING_FOR_SEGMENT_SEARCH = "waiting_for_segment_search"
    SEGMENT_TO_SEARCH = "segment_to_search"
    WAITING_FOR_NAME_SEARCH = "waiting_for_name_search"
    WAITING_FOR_DEPARTMENT_SEARCH = "waiting_for_department_search"
    WAITING_FOR_COMPANY_GROUP_SEARCH = "waiting_for_company_group_search"
    WAITING_FOR_SHOP_TITLE_SEARCH = "waiting_for_shop_title_search"
    WAITING_FOR_DRUGSTORE_TITLE_SEARCH = "waiting_for_drugstore_title_search"


class StateManager:
    def __init__(self, db_path: str = None):
        self._state: Dict[int, Dict[str, Any]] = {}

        # Устанавливаем путь к базе данных в корне проекта
        if db_path is None:
            project_root = Path(__file__).parent.parent.parent  # app/services -> app -> project_root
            self.db_path = str(project_root / "fsm_state.db")
        else:
            self.db_path = db_path

        logger.info(f"FSM database path: {self.db_path}")

        self._init_db()
        self.load_from_db()

        self.SEATABLE_MAIN_MENU_EMPLOYEE_ID = Config.SEATABLE_MAIN_MENU_EMPLOYEE_ID
        self.SEATABLE_MAIN_MENU_NEWCOMER_ID = Config.SEATABLE_MAIN_MENU_NEWCOMER_ID

    # =========================
    # DB
    # =========================
    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_state (
                user_id INTEGER PRIMARY KEY,
                state_json TEXT NOT NULL
            )
            """
        )
        conn.commit()
        conn.close()

    def save_to_db(self):
        try:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            cur = conn.cursor()

            cur.execute("DELETE FROM user_state")

            for user_id, state in self._state.items():
                # Отладка: что сохраняем
                logger.info(f"Saving user {user_id} state: {state}")

                # Сохраняем в JSON
                state_json = json.dumps(state, ensure_ascii=False)
                logger.info(f"JSON to save: {state_json}")

                cur.execute(
                    "INSERT INTO user_state (user_id, state_json) VALUES (?, ?)",
                    (user_id, state_json),
                )

            conn.commit()
            conn.close()
            logger.info(f"FSM state saved to SQLite: {len(self._state)} users")
        except Exception as e:
            logger.error(f"Error saving FSM state: {e}")

    def load_from_db(self):
        try:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            cur = conn.cursor()

            cur.execute("SELECT user_id, state_json FROM user_state")
            rows = cur.fetchall()

            loaded_count = 0
            for user_id, state_json in rows:
                try:
                    # Отладка: что пытаемся загрузить
                    logger.debug(f"Loading user {user_id}, raw JSON: {state_json[:100]}...")

                    self._state[user_id] = json.loads(state_json)
                    loaded_count += 1
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error for user {user_id}: {e}")
                    logger.error(f"Problematic JSON: {state_json}")

            conn.close()
            logger.info(f"FSM state loaded from SQLite: {loaded_count} users of {len(rows)}")
        except Exception as e:
            logger.error(f"Error loading FSM state from database: {e}")
            self._state = {}

    # =========================
    # BASE
    # =========================
    async def update_data(self, user_id: int, **kwargs):
        user_data = self._state.get(user_id, {})
        user_data.update(kwargs)
        self._state[user_id] = user_data

    async def get_data(self, user_id: int) -> Dict[str, Any]:
        return self._state.get(user_id, {}).copy()

    # =========================
    # MENU
    # =========================
    async def get_current_menu(self, user_id: int) -> Optional[str]:
        return self._state.get(user_id, {}).get(AppStates.CURRENT_MENU)

    async def navigate_to_menu(self, user_id: int, menu_id: str):
        user_data = self._state.get(user_id, {})

        history = user_data.setdefault("navigation_history", [])
        if AppStates.CURRENT_MENU in user_data:
            history.append(user_data[AppStates.CURRENT_MENU])

        user_data[AppStates.CURRENT_MENU] = menu_id
        self._state[user_id] = user_data

    async def navigate_back(self, user_id: int) -> Optional[str]:
        user_data = self._state.get(user_id, {})
        history = user_data.get("navigation_history", [])

        if not history:
            return None

        previous_menu = history.pop()
        user_data[AppStates.CURRENT_MENU] = previous_menu
        self._state[user_id] = user_data

        return previous_menu

    # =========================
    # CLEAR
    # =========================
    async def clear(self, user_id: int):
        if user_id in self._state:
            del self._state[user_id]
            logger.info("FSM cleared for user %s", user_id)


    # =========================
    # DEBUG
    # =========================
    def debug_print_db(self):
        """Вывести содержимое БД для отладки"""
        try:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            cur = conn.cursor()

            cur.execute("SELECT user_id, state_json FROM user_state")
            rows = cur.fetchall()

            print("=== DEBUG: FSM database ===")
            for user_id, state_json in rows:
                print(f"User ID: {user_id}")
                print(f"JSON: {state_json}")
                print("-" * 40)

            conn.close()
        except Exception as e:
            print(f"Error reading DB: {e}")


state_manager = StateManager()