"""
Скрипт для импорта email из CSV файла в сводную таблицу NocoDB.
Формат CSV: Фамилия,Имя Отчество,email
"""

import asyncio
import logging
import sys
from typing import List, Dict, Optional, Tuple
from collections import defaultdict

from app.db.nocodb_client import NocoDBClient
from config import Config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class EmailCsvImporter:
    def __init__(self):
        self.pivot_data = []
        self.pivot_index_fio = defaultdict(list)

    def read_csv_file(self, filename: str = "Mail_list_votonia.csv") -> List[Dict]:
        """Читает CSV файл с email."""
        records = []

        try:
            with open(filename, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue

                    parts = line.split(',')
                    if len(parts) < 3:
                        logger.warning(f"Строка {line_num} имеет меньше 3 полей: {line}")
                        continue

                    surname = parts[0].strip()
                    name_patronymic = parts[1].strip()
                    email = parts[2].strip().lower()

                    if not surname or not name_patronymic or not email:
                        logger.warning(f"Строка {line_num} содержит пустые поля: {line}")
                        continue

                    # Составляем полное ФИО
                    fio = f"{surname} {name_patronymic}"

                    # Проверяем email
                    if '@' not in email:
                        logger.warning(f"Строка {line_num}: невалидный email: {email}")
                        continue

                    domain = self.get_email_domain(email)

                    records.append({
                        'raw_line': line,
                        'surname': surname,
                        'name_patronymic': name_patronymic,
                        'fio': fio,
                        'email': email,
                        'domain': domain,
                        'matched_snils': None,
                        'matched_fio': None
                    })

            logger.info(f"Прочитано {len(records)} записей из файла {filename}")

        except Exception as e:
            logger.error(f"Ошибка чтения файла {filename}: {e}")
            return []

        return records

    def get_email_domain(self, email: str) -> str:
        """Определяет тип email по домену."""
        if not email or '@' not in email:
            return 'other'

        domain = email.split('@')[1].lower()

        if domain == 'mavis.ru':
            return 'mavis'
        elif domain == 'votonia.ru':
            return 'votonia'
        else:
            return 'other'

    def normalize_name(self, name: str) -> str:
        """Нормализует имя для сравнения."""
        if not name:
            return ""

        # Приводим к нижнему регистру, убираем лишние пробелы
        name = re.sub(r'\s+', ' ', name.strip()).lower()
        # Убираем лишние символы (тире, точки и т.д.)
        name = re.sub(r'[^\w\s]', '', name)
        return name

    async def fetch_pivot_data(self):
        """Получает данные из сводной таблицы и создает индексы."""
        logger.info("Получение данных из сводной таблицы...")

        async with NocoDBClient() as client:
            self.pivot_data = await client.get_all(table_id=Config.PIVOT_TABLE_ID)

        if not self.pivot_data:
            logger.error("Не удалось получить данные из сводной таблицы")
            return False

        logger.info(f"Получено {len(self.pivot_data)} записей из сводной таблицы")

        # Создаем индекс по ФИО
        for record in self.pivot_data:
            fio = record.get('FIO', '')
            snils = record.get('SNILS', '')

            if fio:
                normalized_fio = self.normalize_name(fio)
                self.pivot_index_fio[normalized_fio].append((record, snils))

        logger.info(f"Создан индекс ФИО: {len(self.pivot_index_fio)} записей")

        return True

    def find_pivot_record(self, email_record: Dict) -> Tuple[Optional[Dict], str, Optional[str]]:
        """
        Ищет запись в сводной таблице по ФИО.
        Возвращает (запись, метод_поиска, снилс).
        """
        fio = email_record['fio']
        normalized_fio = self.normalize_name(fio)

        logger.debug(f"Поиск для ФИО: '{fio}' (нормализовано: '{normalized_fio}')")

        # Ищем по полному ФИО
        if normalized_fio in self.pivot_index_fio:
            matches = self.pivot_index_fio[normalized_fio]
            if len(matches) == 1:
                record, snils = matches[0]
                return record, 'full_fio', snils
            elif len(matches) > 1:
                logger.debug(f"Найдено несколько совпадений по ФИО '{normalized_fio}': {len(matches)}")

        return None, 'not_found', None

    def determine_email_field(self, domain: str) -> str:
        """Определяет поле для email на основе домена."""
        if domain == 'mavis':
            return 'Email_mavis'
        elif domain == 'votonia':
            return 'Email_votonia'
        else:
            return 'Email_other'

    def prepare_updates(self, email_records: List[Dict]) -> Dict[str, Dict]:
        """Подготавливает обновления для сводной таблицы."""
        updates = {}

        for email_record in email_records:
            pivot_record, method, snils = self.find_pivot_record(email_record)

            if not pivot_record:
                logger.warning(f"Не найдено однозначное соответствие для: {email_record['fio']}")
                continue

            row_id = pivot_record.get('Id')
            if not row_id:
                logger.error(f"Нет Id для записи: {pivot_record.get('FIO', 'unknown')}")
                continue

            fio = pivot_record.get('FIO', '')
            email_field = self.determine_email_field(email_record['domain'])

            # Проверяем, нужно ли обновлять
            current_email = pivot_record.get(email_field, '')
            if current_email == email_record['email']:
                logger.debug(f"Email уже установлен для {fio}: {email_record['email']}")
                continue

            if row_id not in updates:
                updates[row_id] = {
                    'row_id': row_id,
                    'fio': fio,
                    'snils': snils,
                    'updates': {}
                }

            updates[row_id]['updates'][email_field] = email_record['email']
            email_record['matched_snils'] = snils
            email_record['matched_fio'] = fio

            logger.info(f"✓ Найдено соответствие ({method}):")
            logger.info(f"  Из файла: {email_record['fio']}")
            logger.info(f"  В таблице: {fio} ({snils})")
            logger.info(f"  Email: {email_field} = {email_record['email']}")

        return updates

    async def update_pivot_records(self, updates: Dict[str, Dict]) -> Tuple[int, int]:
        """Обновляет записи в сводной таблице."""
        if not updates:
            return 0, 0

        success_count = 0
        error_count = 0

        async with NocoDBClient() as client:
            for row_id, data in updates.items():
                try:
                    logger.info(f"Обновление записи {row_id}: {data['updates']}")
                    await client.update_record(
                        table_id=Config.PIVOT_TABLE_ID,
                        record_id=int(row_id),
                        data=data['updates']
                    )
                    logger.info(f"✓ Обновлено: {data['fio']} ({data['snils']})")
                    success_count += 1
                except Exception as e:
                    logger.error(f"✗ Ошибка обновления {data['fio']} (row_id={row_id}): {e}")
                    error_count += 1

        return success_count, error_count

    async def process_emails(self, filename: str = "emails.csv"):
        """Основная функция обработки email."""
        logger.info("=== НАЧАЛО ОБРАБОТКИ EMAIL ИЗ CSV ===")

        try:
            # 1. Читаем CSV файл
            email_records = self.read_csv_file(filename)
            if not email_records:
                logger.error("Не найдено email для обработки")
                return

            # 2. Получаем данные из сводной таблицы
            if not await self.fetch_pivot_data():
                return

            # 3. Подготавливаем обновления
            logger.info("Поиск соответствий...")
            updates = self.prepare_updates(email_records)

            # 4. Обновляем записи
            if updates:
                logger.info(f"Найдено {len(updates)} записей для обновления")
                success, errors = await self.update_pivot_records(updates)

                # 5. Статистика
                logger.info("=== ОБРАБОТКА ЗАВЕРШЕНА ===")
                logger.info(f"ИТОГО:")
                logger.info(f"  Обработано email записей: {len(email_records)}")
                logger.info(f"  Найдено соответствий: {len(updates)}")
                logger.info(f"  Успешно обновлено: {success}")
                logger.info(f"  Ошибок обновления: {errors}")

                # 6. Выводим необработанные записи
                unmatched = [r for r in email_records if not r['matched_snils']]
                if unmatched:
                    logger.warning(f"\nНе обработано записей: {len(unmatched)}")
                    for r in unmatched:
                        logger.warning(f"  - {r['fio']} <{r['email']}>")

            else:
                logger.warning("Не найдено записей для обновления")

        except Exception as e:
            logger.error(f"Критическая ошибка при обработке email: {e}", exc_info=True)


async def main():
    """Главная функция для запуска скрипта."""
    importer = EmailCsvImporter()

    import os
    if not os.path.exists("Mail_list_votonia.csv"):
        logger.error("Файл emails.csv не найден в текущей директории")
        return 1

    try:
        await importer.process_emails("Mail_list_votonia.csv")
    except Exception as e:
        logger.error(f"Ошибка выполнения скрипта: {e}", exc_info=True)
        return 1

    return 0


if __name__ == "__main__":
    import re

    sys.exit(asyncio.run(main()))
