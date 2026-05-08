import asyncio
from app.db.nocodb_client import NocoDBClient
from config import Config
import os

PIVOT = ""


async def generate_email_list():
    """
    Генерирует список email для рассылки из сводной таблицы.
    Приоритет: Email_mavis > Email_votonia > Email_other
    """
    async with NocoDBClient() as client:
        print("=== ГЕНЕРАЦИЯ СПИСКА EMAIL ДЛЯ РАССЫЛКИ ===")

        # Получаем данные из сводной таблицы
        pivot_data = await client.get_all(table_id=PIVOT)
        print(f"Получено {len(pivot_data)} записей из сводной")

        emails = []

        for row in pivot_data:
            # Выбираем email по приоритету
            email = None
            if row.get("Email_mavis"):
                email = row.get("Email_mavis")
            elif row.get("Email_votonia"):
                email = row.get("Email_votonia")
            elif row.get("Email_other"):
                email = row.get("Email_other")

            if email:
                emails.append(email)

        print(f"Собрано {len(emails)} email адресов")

        # Сохраняем файл в домашнюю папку
        home_dir = os.path.expanduser("~")
        file_path = os.path.join(home_dir, "email_list.txt")

        with open(file_path, "w", encoding="utf-8") as f:
            for email in emails:
                f.write(email + "\n")

        print(f"Список email сохранён в файл: {file_path}")

        return emails


if __name__ == "__main__":
    asyncio.run(generate_email_list())