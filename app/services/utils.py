import re
import logging
from typing import Optional, List

logger = logging.getLogger(__name__)


def normalize_phones_string(phones_string: str) -> List[str]:
    """Нормализует строку с несколькими телефонами."""
    if not phones_string:
        return []

    phones_string = phones_string.strip()

    # Шаг 1: Разделяем по явным разделителям (запятая, точка с запятой)
    # Сначала разбиваем по запятым и точкам с запятой
    parts = re.split(r'[,;]', phones_string)

    normalized_phones = []

    for part in parts:
        part = part.strip()
        if not part:
            continue

        # Шаг 2: В каждой части проверяем, не содержится ли несколько телефонов
        # Определяем возможные телефоны в этой части

        # Если часть содержит цифры - обрабатываем
        # Сначала пробуем нормализовать всю часть как один телефон
        single_normalized = normalize_phone(part)

        if single_normalized:
            normalized_phones.append(single_normalized)
        else:
            # Если не получилось как один телефон, пробуем разбить по пробелам
            # Но только если это не городской номер (городской может быть с пробелами)

            # Проверяем, не является ли это городским номером с пробелами
            digits_in_part = re.sub(r'\D', '', part)
            if len(digits_in_part) == 7:
                # Это городской номер с пробелами/дефисами
                normalized_city = normalize_phone(part)
                if normalized_city:
                    normalized_phones.append(normalized_city)
            else:
                # Пробуем разбить по пробелам и проверить каждый кусок
                sub_parts = part.split()
                for sub_part in sub_parts:
                    sub_part = sub_part.strip()
                    if sub_part:
                        normalized = normalize_phone(sub_part)
                        if normalized:
                            normalized_phones.append(normalized)

    return normalized_phones


def normalize_phone(raw: Optional[str]) -> Optional[str]:
    """Приводит телефон к стандартному формату:
    - Мобильные: +7XXXXXXXXXX (11 цифр после +7)
    - Городские: XXX-XX-XX (7 цифр)
    """
    if not raw:
        return None

    # Удаляем все нецифровые символы
    digits = re.sub(r'\D', '', raw)

    # Если цифр нет
    if not digits:
        return None

    # Городской номер (7 цифр)
    if len(digits) == 7:
        return f"{digits[:3]}-{digits[3:5]}-{digits[5:7]}"

    # Мобильный номер
    if len(digits) == 10:
        digits = '7' + digits
    elif len(digits) == 11:
        if digits[0] == '8':
            digits = '7' + digits[1:]
        elif digits[0] != '7':
            return None
    else:
        return None

    if len(digits) == 11 and digits[0] == '7':
        return f"+{digits}"

    return None


def phones_to_set(val) -> set[str]:
    """
    Вспомогательная функция, которая используется при апдейте данных пользователей из 1С
    Приводит Phones из:
    - None
    - строки '+7..., +7...'
    - списка ['+7...', '...']
    к нормализованному множеству телефонов
    """
    if not val:
        return set()

    if isinstance(val, list):
        phones = []
        for item in val:
            phones.extend(normalize_phones_string(item))
        return set(phones)

    if isinstance(val, str):
        return set(normalize_phones_string(val))

    return set()


def values_to_set(val) -> set[str]:
    """
    Приводит значения из сводной таблицы и из 1С к множеству строк для сравнения.

    Поддерживает:
    - None
    - 'A, B, C'
    - 'A,B,C'
    - ['A', 'B']
    """
    if not val:
        return set()

    result = set()

    def split_and_add(s: str):
        for part in re.split(r'\s*,\s*', s):
            part = part.strip()
            if part:
                result.add(part)

    if isinstance(val, list):
        for item in val:
            if isinstance(item, str):
                split_and_add(item)
        return result

    if isinstance(val, str):
        split_and_add(val)
        return result

    return set()


def surname_to_str(val) -> str:
    if not val:
        return ''
    if isinstance(val, list):
        return val[0] if val else ''
    return str(val)


def contains_restricted_emails(text: str) -> bool:
    """
    Проверяет, содержит ли текст email с доменами mavis.ru или votonia.ru
    Возвращает True если находит ограниченные email
    """
    import re

    if not text:
        return False

    # Паттерн для поиска email с доменами mavis.ru или votonia.ru
    # Учитываем разные форматы: example@mavis.ru, example@votonia.ru
    pattern = r'[a-zA-Z0-9._%+-]+@(mavis\.ru|votonia\.ru)\b'

    # Игнорируем регистр
    matches = re.findall(pattern, text, re.IGNORECASE)

    return len(matches) > 0

