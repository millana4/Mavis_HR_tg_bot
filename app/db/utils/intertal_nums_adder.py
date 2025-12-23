"""
Вспомогательный скрипт для синхронизации телефонных номеров из ATS в сводную таблицу.
Запускается отдельно от основного сервиса.
"""

import asyncio
import logging
import re
import sys
from typing import List, Dict, Optional, Tuple
from collections import defaultdict

import aiohttp  # Добавляем импорт aiohttp

# Импортируем конфигурацию и функции из вашего проекта
# Убедитесь, что эти модули доступны
try:
    from config import Config
    from app.seatable_api.api_base import get_base_token, fetch_table
except ImportError:
    # Для запуска вне основного проекта
    print("Внимание: running in standalone mode")


    # Здесь нужно определить Config или передать через аргументы
    class Config:
        SEATABLE_SERVER = "https://your-seatable-server.com"
        SEATABLE_ATS_BOOK_ID = "your_ats_table_id"
        SEATABLE_PIVOT_TABLE_ID = "your_pivot_table_id"
        SEATABLE_API_APP_TOKEN = "your_app_token"
        SEATABLE_API_USER_TOKEN = "your_user_token"


    # Замените на ваши реальные значения
    config = Config()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def normalize_name(name: str) -> str:
    """
    Нормализует имя для сравнения.
    Преобразует в нижний регистр и оставляет только фамилию и имя.
    """
    if not name:
        return ""

    # Убираем лишние пробелы
    name = re.sub(r'\s+', ' ', name.strip())

    # Разбиваем на части
    parts = name.split()

    if len(parts) >= 2:
        # Берем фамилию и имя (первые два слова)
        return f"{parts[0]} {parts[1]}".lower()
    else:
        # Если только одно слово - возвращаем как есть
        return name.lower()


def extract_surname_firstname(full_name: str) -> Tuple[str, str]:
    """
    Извлекает фамилию и имя из полного ФИО.
    Возвращает (фамилия_имя, полное_имя).
    """
    if not full_name:
        return "", ""

    # Убираем лишние пробелы
    name = re.sub(r'\s+', ' ', full_name.strip())
    parts = name.split()

    if len(parts) >= 2:
        # Фамилия + Имя
        surname_firstname = f"{parts[0]} {parts[1]}"
    else:
        # Только одно слово
        surname_firstname = parts[0] if parts else ""

    return surname_firstname, name


async def fetch_ats_data() -> List[Dict]:
    """
    Получает данные из таблицы ATS (телефонный справочник).
    """
    logger.info("Получение данных из таблицы ATS...")

    data = await fetch_table(
        table_id=Config.SEATABLE_ATS_BOOK_ID,
        app='USER'
    )

    if data is None:
        logger.error("Не удалось получить данные из таблицы ATS")
        return []

    logger.info(f"Получено {len(data)} записей из ATS")
    return data


async def fetch_pivot_data() -> List[Dict]:
    """
    Получает данные из сводной таблицы.
    """
    logger.info("Получение данных из сводной таблицы...")

    data = await fetch_table(
        table_id=Config.SEATABLE_PIVOT_TABLE_ID,
        app='USER'
    )

    if data is None:
        logger.error("Не удалось получить данные из сводной таблицы")
        return []

    logger.info(f"Получено {len(data)} записей из сводной таблицы")
    return data


async def update_pivot_record(record_id: str, update_data: Dict) -> bool:
    """
    Обновляет запись в сводной таблице.
    """
    try:
        token_data = await get_base_token(app='USER')
        if not token_data:
            logger.error("Не удалось получить токен SeaTable")
            return False

        url = f"{token_data['dtable_server'].rstrip('/')}/api/v1/dtables/{token_data['dtable_uuid']}/rows/"

        headers = {
            "Authorization": f"Bearer {token_data['access_token']}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

        payload = {
            "table_id": Config.SEATABLE_PIVOT_TABLE_ID,
            "row_id": record_id,
            "row": update_data
        }

        async with aiohttp.ClientSession() as session:
            async with session.put(url, headers=headers, json=payload) as response:
                if response.status == 200:
                    logger.debug(f"Запись {record_id} успешно обновлена")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Ошибка обновления записи {record_id}: {response.status} - {error_text}")
                    return False

    except Exception as e:
        logger.error(f"Ошибка при обновлении записи {record_id}: {e}", exc_info=True)
        return False


async def sync_phone_numbers():
    """
    Основная функция синхронизации телефонных номеров.
    """
    logger.info("=== НАЧАЛО СИНХРОНИЗАЦИИ ТЕЛЕФОННЫХ НОМЕРОВ ===")

    try:
        # Получаем данные из обеих таблиц
        ats_data = await fetch_ats_data()
        pivot_data = await fetch_pivot_data()

        if not ats_data or not pivot_data:
            logger.warning("Нет данных для синхронизации")
            return

        # Создаем индекс сводной таблицы по нормализованным именам
        pivot_index = defaultdict(list)  # имя -> список записей

        for pivot_record in pivot_data:
            fio = pivot_record.get('FIO', '')
            if fio:
                normalized = normalize_name(fio)
                if normalized:
                    pivot_index[normalized].append(pivot_record)

        logger.info(f"Создан индекс по {len(pivot_index)} уникальным именам в сводной таблице")

        # Обрабатываем записи из ATS
        processed_count = 0
        updated_count = 0
        ambiguous_count = 0
        not_found_count = 0

        for ats_record in ats_data:
            processed_count += 1

            raw_owner = ats_record.get('Raw_owner', '')
            internal_number = ats_record.get('Internal_number')
            number_direct = ats_record.get('Number_direct')

            if not raw_owner:
                logger.debug(f"Пропускаем запись без Raw_owner: {ats_record.get('_id')}")
                continue

            # Извлекаем фамилию и имя для поиска
            search_name, full_name = extract_surname_firstname(raw_owner)
            normalized_search = normalize_name(search_name)

            if not normalized_search:
                logger.debug(f"Не удалось извлечь имя из: '{raw_owner}'")
                not_found_count += 1
                continue

            # Ищем совпадения в сводной таблице
            matches = pivot_index.get(normalized_search, [])

            if len(matches) == 0:
                logger.info(f"Не найдено совпадений для: '{raw_owner}' (нормализовано: {normalized_search})")
                not_found_count += 1

            elif len(matches) == 1:
                # Одно совпадение - обновляем
                pivot_record = matches[0]
                pivot_id = pivot_record.get('_id')
                pivot_fio = pivot_record.get('FIO', '')

                # Проверяем, нужно ли обновлять
                needs_update = False
                update_data = {}

                # Обновляем поля Internal_number и Number_direct (как в вашей сводной таблице)
                if internal_number and pivot_record.get('Internal_number') != internal_number:
                    update_data['Internal_number'] = internal_number
                    needs_update = True

                if number_direct and pivot_record.get('Number_direct') != number_direct:
                    update_data['Number_direct'] = number_direct
                    needs_update = True

                if needs_update:
                    logger.info(f"Обновление для: {pivot_fio} ← из ATS: '{raw_owner}'")
                    logger.info(f"  Internal_number: {pivot_record.get('Internal_number')} → {internal_number}")
                    logger.info(f"  Number_direct: {pivot_record.get('Number_direct')} → {number_direct}")

                    success = await update_pivot_record(pivot_id, update_data)
                    if success:
                        updated_count += 1
                        logger.info(f"✓ Успешно обновлено: {pivot_fio}")
                    else:
                        logger.error(f"✗ Ошибка обновления: {pivot_fio}")
                else:
                    logger.debug(f"Не требует обновления: {pivot_fio} (данные совпадают)")

            else:
                # Несколько совпадений - не обновляем
                ambiguous_count += 1
                logger.warning(f"Найдено {len(matches)} совпадений для: '{raw_owner}'")
                for i, match in enumerate(matches, 1):
                    logger.warning(f"  {i}. {match.get('FIO', 'нет ФИО')} (ID: {match.get('_id')})")

        # Итоговая статистика
        logger.info("=== СИНХРОНИЗАЦИЯ ЗАВЕРШЕНА ===")
        logger.info(f"ИТОГО:")
        logger.info(f"  Обработано записей из ATS: {processed_count}")
        logger.info(f"  Обновлено записей в сводной: {updated_count}")
        logger.info(f"  Неоднозначных совпадений: {ambiguous_count}")
        logger.info(f"  Не найдено в сводной: {not_found_count}")

    except Exception as e:
        logger.error(f"Критическая ошибка при синхронизации: {e}", exc_info=True)


async def main():
    """
    Главная функция для запуска скрипта.
    """
    logger.info("Запуск скрипта синхронизации телефонных номеров")

    try:
        await sync_phone_numbers()
    except Exception as e:
        logger.error(f"Ошибка выполнения скрипта: {e}", exc_info=True)
        return 1

    return 0


if __name__ == "__main__":
    # Запускаем асинхронную функцию
    sys.exit(asyncio.run(main()))