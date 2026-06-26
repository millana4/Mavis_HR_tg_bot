import re
import html
import logging
from typing import Optional, List

from app.db.table_data import fetch_table
from config import Config

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


def mask_pii(value, visible: int = 3) -> str:
    """
    Маскирует персональные данные: оставляет первые `visible` символов,
    остальное заменяет звёздочками. Безопасна для None/нестроковых типов.
    Примеры:
        mask_pii("Иванов Иван Иванович") -> "Ива*****************"
        mask_pii("12345678901") -> "123********"
        mask_pii(None) -> "***"
    """
    if value is None:
        return "***"
    s = str(value)
    if len(s) <= visible:
        return "*" * len(s) if s else "***"
    return s[:visible] + "*" * (len(s) - visible)





# [текст](url)
_LINK_RE = re.compile(r"\[([^\]]+)\]\((https?://[^\s)]+)\)")
# **жирный**
_BOLD_RE = re.compile(r"\*\*([^*]+)\*\*")
# голый url (после того как markdown-ссылки уже вырезаны в плейсхолдеры)
_BARE_URL_RE = re.compile(r"(?<!href=\")(?<!\">)(https?://[^\s<]+)")


def markdown_to_html(text: str) -> str:
    """
    Конвертер ответа ИИ-агента (markdown) в Telegram-HTML.

    Агент отдаёт ответ с markdown-разметкой:
      - ссылки в виде [текст](url)
      - жирный в виде **текст**
      - иногда голые ссылки https://...

    Telegram parse_mode=HTML понимает <b>, <i>, <a href>. Этот конвертер
    превращает markdown в HTML и экранирует спецсимволы, чтобы парсер не падал.
    """
    if not text:
        return ""

    # Шаг 1. Вынимаем markdown-ссылки в плейсхолдеры ДО экранирования,
    # чтобы их url/текст не пострадали и не путались с голыми ссылками.
    links: list[tuple[str, str]] = []

    def _stash_link(m: re.Match) -> str:
        link_text = m.group(1)
        url = m.group(2)
        links.append((link_text, url))
        return f"\x00LINK{len(links) - 1}\x00"

    text = _LINK_RE.sub(_stash_link, text)

    # Шаг 2. Экранируем HTML-спецсимволы во всём оставшемся тексте.
    text = html.escape(text, quote=False)

    # Шаг 3. Жирный **...** → <b>...</b>.
    text = _BOLD_RE.sub(r"<b>\1</b>", text)

    # Шаг 4. Голые ссылки → кликабельные <a>.
    text = _BARE_URL_RE.sub(r'<a href="\1">\1</a>', text)

    # Шаг 5. Возвращаем markdown-ссылки уже как HTML <a>.
    def _restore_link(m: re.Match) -> str:
        idx = int(m.group(1))
        link_text, url = links[idx]
        safe_text = html.escape(link_text, quote=False)
        return f'<a href="{html.escape(url, quote=True)}">{safe_text}</a>'

    text = re.sub(r"\x00LINK(\d+)\x00", _restore_link, text)

    return text


async def get_broadcast_admin_ids() -> List[int]:
    """
    Вернуть Telegram-id админов с галкой Content+broadcast_admin=True.

    Используется для рассылки служебных алертов от ИИ-агента
    (например, переключение на запасной провайдер).
    """
    try:
        admins = await fetch_table(table_id=Config.ADMIN_TABLE_ID, app='USER')
    except Exception as e:
        logger.error(f"Не удалось получить список админов для алерта: {e}")
        return []

    ids: List[int] = []
    for admin in admins:
        if not admin.get('Content+broadcast_admin'):
            continue
        messenger_id = admin.get('ID_messenger')
        if not messenger_id:
            continue
        try:
            ids.append(int(messenger_id))
        except (TypeError, ValueError):
            logger.warning(f"Некорректный ID_messenger у админа: {messenger_id!r}")
    return ids
