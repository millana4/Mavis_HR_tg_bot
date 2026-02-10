"""
Скрипт для импорта email из текстового файла в сводную таблицу SeaTable.
"""

import asyncio
import logging
import re
import sys
from typing import List, Dict, Optional, Tuple, Set
from collections import defaultdict

import aiohttp

try:
    from config import Config
    from app.db.table_data import fetch_table
except ImportError:
    print("Ошибка импорта модулей. Запустите скрипт из корневой папки проекта.")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class EmailImporter:
    def __init__(self):
        self.pivot_data = []
        self.pivot_index_fio = defaultdict(list)
        self.pivot_index_surname_name = defaultdict(list)
        self.pivot_index_name_surname = defaultdict(list)

    def read_emails_file(self, filename: str = "raw_emails.txt") -> str:
        """Читает содержимое файла с email."""
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                content = f.read()
            logger.info(f"Прочитано {len(content)} символов из файла {filename}")
            return content
        except Exception as e:
            logger.error(f"Ошибка чтения файла {filename}: {e}")
            return ""

    def clean_text(self, text: str) -> str:
        """Очищает текст от лишних пробелов и символов."""
        # Заменяем множественные пробелы на один
        text = re.sub(r'\s+', ' ', text)
        # Убираем пробелы вокруг запятых, точек с запятой
        text = re.sub(r'\s*[,;]\s*', ', ', text)
        # Убираем пробелы в начале и конце строк
        text = text.strip()
        return text

    def parse_email_content(self, content: str) -> List[Dict]:
        """Парсит строки с email с улучшенной обработкой разделителей."""
        records = []

        # Очищаем текст
        content = self.clean_text(content)

        # Разные паттерны для разных форматов
        patterns = [
            # Паттерн для: ФИО <email>, ФИО <email>
            r'([^<>,;]+?)<([^>]+)>',
            # Паттерн для: ФИО<email> (без пробела)
            r'([^<>,;]+)<([^>]+)>',
        ]

        all_matches = []
        for pattern in patterns:
            matches = re.findall(pattern, content)
            all_matches.extend(matches)

        # Обрабатываем найденные совпадения
        for name_part, email in all_matches:
            name_part = name_part.strip()
            email = email.strip().lower()

            # Пропускаем мусорные записи
            if not name_part or not email or '@' not in email:
                continue

            # Очищаем name_part от разделителей в начале/конце
            name_part = re.sub(r'^[,\s;]+|[,\s;]+$', '', name_part)

            # Проверяем, что name_part содержит хотя бы одну букву
            if not re.search(r'[а-яА-Яa-zA-Z]', name_part):
                continue

            # Проверяем, что email валидный
            if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email):
                logger.warning(f"Пропускаем невалидный email: {email}")
                continue

            domain = self.get_email_domain(email)

            records.append({
                'raw_text': f"{name_part} <{email}>",
                'name_part': name_part,
                'email': email,
                'domain': domain,
                'matched_snils': None,
                'matched_fio': None
            })

        # Также пытаемся найти email без явных <>
        if not records:
            emails = re.findall(r'\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b', content)
            for email in emails:
                # Ищем имя перед email (до 10 слов назад)
                email_lower = email.lower()
                email_pos = content.lower().find(email_lower)
                if email_pos > 0:
                    # Берем текст перед email
                    before_email = content[max(0, email_pos - 200):email_pos]
                    # Ищем последнее разумное имя (слова с заглавными буквами)
                    name_matches = re.findall(r'([А-ЯЁ][а-яё]+(?:\s+[А-ЯЁ][а-яё]+){1,3})', before_email)
                    if name_matches:
                        name_part = name_matches[-1].strip()
                    else:
                        # Пропускаем email без имени
                        continue

                    domain = self.get_email_domain(email)
                    records.append({
                        'raw_text': f"{name_part} <{email}>",
                        'name_part': name_part,
                        'email': email,
                        'domain': domain,
                        'matched_snils': None,
                        'matched_fio': None
                    })

        logger.info(f"Извлечено {len(records)} валидных записей email")

        # Логируем первые несколько записей для проверки
        for i, record in enumerate(records[:5]):
            logger.debug(f"Пример записи {i + 1}: '{record['name_part']}' <{record['email']}>")

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

    def extract_name_components(self, name_part: str) -> Dict:
        """Извлекает компоненты имени из строки."""
        # Сначала очищаем от должностей и других слов
        # Убираем явные должности (слова полностью в нижнем или верхнем регистре)
        cleaned_name = re.sub(r'\b(?:[А-ЯЁ]{2,}|[а-яё]{2,})\s+[А-ЯЁ]{2,}\s+[А-ЯЁ]{2,}\b', '', name_part)
        cleaned_name = re.sub(
            r'\b(?:отдел|департамент|руководитель|менеджер|директор|специалист|аналитик|программист|бухгалтер|юрист)\b',
            '', cleaned_name, flags=re.IGNORECASE)

        # Извлекаем слова, которые выглядят как части имени
        name_words = []
        words = cleaned_name.split()
        for word in words:
            # Проверяем, что слово похоже на имя/фамилию
            # (начинается с заглавной буквы и содержит только буквы)
            if re.match(r'^[А-ЯЁ][а-яё]*$', word):
                name_words.append(word)
            # Также добавляем составные фамилии через дефис
            elif re.match(r'^[А-ЯЁ][а-яё]*-[А-ЯЁ][а-яё]*$', word):
                name_words.append(word)

        result = {
            'raw': name_part,
            'cleaned': ' '.join(name_words),
            'parts': name_words,
            'surname': '',
            'first_name': '',
            'patronymic': '',
            'surname_firstname': '',
            'firstname_surname': ''
        }

        if len(name_words) >= 2:
            # Пробуем определить порядок
            if len(name_words) >= 3:
                # Фамилия Имя Отчество
                result['surname'] = name_words[0]
                result['first_name'] = name_words[1]
                result['patronymic'] = ' '.join(name_words[2:])
                result['surname_firstname'] = f"{name_words[0]} {name_words[1]}"
                result['firstname_surname'] = f"{name_words[1]} {name_words[0]}"
            else:
                # Два слова - пробуем оба варианта
                result['surname'] = name_words[0]
                result['first_name'] = name_words[1]
                result['surname_firstname'] = f"{name_words[0]} {name_words[1]}"
                result['firstname_surname'] = f"{name_words[1]} {name_words[0]}"

        return result

    async def fetch_pivot_data(self):
        """Получает данные из сводной таблицы и создает индексы."""
        logger.info("Получение данных из сводной таблицы...")

        self.pivot_data = await fetch_table(
            table_id=Config.SEATABLE_PIVOT_TABLE_ID,
            app='USER'
        )

        if not self.pivot_data:
            logger.error("Не удалось получить данные из сводной таблицы")
            return False

        logger.info(f"Получено {len(self.pivot_data)} записей из сводной таблицы")

        # Создаем индексы
        for record in self.pivot_data:
            fio = record.get('FIO', '')
            snils = record.get('Name', '')

            if fio:
                # Нормализуем ФИО
                normalized_fio = self.normalize_name(fio)
                self.pivot_index_fio[normalized_fio].append((record, snils))

                # Извлекаем фамилию и имя
                parts = fio.split()
                if len(parts) >= 2:
                    surname_name = self.normalize_name(f"{parts[0]} {parts[1]}")
                    name_surname = self.normalize_name(f"{parts[1]} {parts[0]}")

                    self.pivot_index_surname_name[surname_name].append((record, snils))
                    self.pivot_index_name_surname[name_surname].append((record, snils))

        logger.info(f"Созданы индексы: ФИО={len(self.pivot_index_fio)}, "
                    f"Фамилия+Имя={len(self.pivot_index_surname_name)}")

        return True

    def find_pivot_record(self, email_record: Dict) -> Tuple[Optional[Dict], str, Optional[str]]:
        """
        Ищет запись в сводной таблице по данным из email.
        Возвращает (запись, метод_поиска, снилс).
        """
        name_part = email_record['name_part']

        # Сначала пробуем найти по очищенному имени
        components = self.extract_name_components(name_part)
        cleaned_name = components['cleaned']

        if cleaned_name:
            # Пробуем разные варианты поиска

            # 1. По очищенному ФИО
            normalized_cleaned = self.normalize_name(cleaned_name)
            if normalized_cleaned in self.pivot_index_fio:
                matches = self.pivot_index_fio[normalized_cleaned]
                if len(matches) == 1:
                    record, snils = matches[0]
                    return record, 'full_fio_cleaned', snils

            # 2. По фамилия+имя из очищенного
            if components['surname_firstname']:
                normalized = self.normalize_name(components['surname_firstname'])
                if normalized in self.pivot_index_surname_name:
                    matches = self.pivot_index_surname_name[normalized]
                    if len(matches) == 1:
                        record, snils = matches[0]
                        return record, 'surname_firstname', snils

            # 3. По имя+фамилия из очищенного
            if components['firstname_surname']:
                normalized = self.normalize_name(components['firstname_surname'])
                if normalized in self.pivot_index_name_surname:
                    matches = self.pivot_index_name_surname[normalized]
                    if len(matches) == 1:
                        record, snils = matches[0]
                        return record, 'firstname_surname', snils

        # Если не нашли по очищенному, пробуем по сырому (но нормализованному)
        normalized_raw = self.normalize_name(name_part)
        if normalized_raw in self.pivot_index_fio:
            matches = self.pivot_index_fio[normalized_raw]
            if len(matches) == 1:
                record, snils = matches[0]
                return record, 'full_fio_raw', snils

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
                logger.warning(f"Не найдено однозначное соответствие для: {email_record['name_part']}")
                continue

            row_id = pivot_record.get('_id')
            if not row_id:
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
            logger.info(f"  Из файла: '{email_record['name_part']}'")
            logger.info(f"  В таблице: {fio} ({snils})")
            logger.info(f"  Email: {email_field} = {email_record['email']}")

        return updates

    async def update_pivot_records(self, updates: Dict[str, Dict]) -> Tuple[int, int]:
        """Обновляет записи в сводной таблице."""
        if not updates:
            return 0, 0

        success_count = 0
        error_count = 0

        for row_id, data in updates.items():
            try:
                token_data = await get_base_token(app='USER')
                if not token_data:
                    logger.error("Не удалось получить токен SeaTable")
                    error_count += 1
                    continue

                url = f"{token_data['dtable_server'].rstrip('/')}/api/v1/dtables/{token_data['dtable_uuid']}/rows/"

                headers = {
                    "Authorization": f"Bearer {token_data['access_token']}",
                    "Accept": "application/json",
                    "Content-Type": "application/json"
                }

                payload = {
                    "table_id": Config.SEATABLE_PIVOT_TABLE_ID,
                    "row_id": row_id,
                    "row": data['updates']
                }

                async with aiohttp.ClientSession() as session:
                    async with session.put(url, headers=headers, json=payload) as response:
                        if response.status == 200:
                            logger.info(f"✓ Обновлено: {data['fio']} ({data['snils']})")
                            success_count += 1
                        else:
                            error_text = await response.text()
                            logger.error(f"✗ Ошибка обновления {data['fio']}: {response.status} - {error_text}")
                            error_count += 1

            except Exception as e:
                logger.error(f"Ошибка при обновлении записи {row_id}: {e}")
                error_count += 1

        return success_count, error_count

    async def process_emails(self, filename: str = "raw_emails.txt"):
        """Основная функция обработки email."""
        logger.info("=== НАЧАЛО ОБРАБОТКИ EMAIL ===")

        try:
            # 1. Читаем файл
            content = self.read_emails_file(filename)
            if not content:
                logger.error("Файл пустой или не найден")
                return

            # 2. Парсим email
            email_records = self.parse_email_content(content)
            if not email_records:
                logger.warning("Не найдено email для обработки")
                return

            # 3. Получаем данные из сводной таблицы
            if not await self.fetch_pivot_data():
                return

            # 4. Подготавливаем обновления
            logger.info("Поиск соответствий...")
            updates = self.prepare_updates(email_records)

            # 5. Обновляем записи
            if updates:
                logger.info(f"Найдено {len(updates)} записей для обновления")
                success, errors = await self.update_pivot_records(updates)

                # 6. Статистика
                logger.info("=== ОБРАБОТКА ЗАВЕРШЕНА ===")
                logger.info(f"ИТОГО:")
                logger.info(f"  Обработано email записей: {len(email_records)}")
                logger.info(f"  Найдено соответствий: {len(updates)}")
                logger.info(f"  Успешно обновлено: {success}")
                logger.info(f"  Ошибок обновления: {errors}")

                # 7. Выводим необработанные записи
                unmatched = [r for r in email_records if not r['matched_snils']]
                if unmatched:
                    logger.warning(f"\nНе обработано записей: {len(unmatched)}")
                    for r in unmatched:
                        logger.warning(f"  - '{r['name_part']}' <{r['email']}>")

            else:
                logger.warning("Не найдено записей для обновления")

        except Exception as e:
            logger.error(f"Критическая ошибка при обработке email: {e}", exc_info=True)


async def main():
    """Главная функция для запуска скрипта."""
    importer = EmailImporter()

    import os
    if not os.path.exists("raw_emails.txt"):
        logger.error("Файл raw_emails.txt не найден в текущей директории")
        return 1

    try:
        await importer.process_emails("raw_emails.txt")
    except Exception as e:
        logger.error(f"Ошибка выполнения скрипта: {e}", exc_info=True)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))