import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    BOT_TOKEN = os.getenv("BOT_TOKEN")

    NOCOBD_SERVER = os.getenv("NOCOBD_SERVER")
    NOCOBD_API_TOKEN = os.getenv("NOCOBD_API_TOKEN")

    MAIN_MENU_EMPLOYEE_ID = os.getenv("MAIN_MENU_EMPLOYEE_ID")
    MAIN_MENU_NEWCOMER_ID = os.getenv("MAIN_MENU_NEWCOMER_ID")
    BROADCAST_TABLE_ID = os.getenv("BROADCAST_TABLE_ID")

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


