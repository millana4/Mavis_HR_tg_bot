import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    BOT_PROXY = os.getenv("BOT_PROXY")

    NOCOBD_SERVER = os.getenv("NOCOBD_SERVER")
    NOCOBD_API_TOKEN = os.getenv("NOCOBD_API_TOKEN")

    MAIN_MENU_EMPLOYEE_ID = os.getenv("MAIN_MENU_EMPLOYEE_ID")
    MAIN_MENU_NEWCOMER_ID = os.getenv("MAIN_MENU_NEWCOMER_ID")
    BROADCAST_TABLE_ID = os.getenv("BROADCAST_TABLE_ID")

    BUFFER_1C_TABLE_ID = os.getenv("BUFFER_1C_TABLE_ID")
    DATA_1C_TABLE_ID = os.getenv("DATA_1C_TABLE_ID")
    ATS_MAVIS_BOOK_ID = os.getenv("ATS_MAVIS_BOOK_ID")
    ATS_VOTONIA_BOOK_ID = os.getenv("ATS_VOTONIA_BOOK_ID")
    SHOP_TABLE_ID = os.getenv("SHOP_TABLE_ID")
    DRUGSTORE_TABLE_ID = os.getenv("DRUGSTORE_TABLE_ID")
    PIVOT_TABLE_ID = os.getenv("PIVOT_TABLE_ID")
    AUTH_TABLE_ID = os.getenv("AUTH_TABLE_ID")
    ADMIN_TABLE_ID = os.getenv("ADMIN_TABLE_ID")

    PULSE_TASKS_ID = os.getenv("PULSE_TASKS_ID")
    PULSE_CONTENT_ID = os.getenv("PULSE_CONTENT_ID")

    AI_FAQ_TABLE_ID = os.getenv("AI_FAQ_TABLE_ID")
    FEEDBACK_TABLE_ID = os.getenv("FEEDBACK_TABLE_ID")

    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

    AI_AGENT_URL = os.getenv("AI_AGENT_URL")
    AI_AGENT_API_KEY = os.getenv("AI_AGENT_API_KEY")


