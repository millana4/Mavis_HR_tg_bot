import re
import logging

logger = logging.getLogger(__name__)


def normalize_phone(raw: str | None) -> str | None:
    """Приводит телефон к формату +7XXXXXXXXXX или возвращает None."""
    if not raw:
        return None

    digits = re.sub(r"\D", "", raw)

    if len(digits) == 11 and digits[0] == "8":
        digits = "7" + digits[1:]
    elif len(digits) == 10:
        digits = "7" + digits
    elif len(digits) == 11 and digits[0] == "7":
        pass
    else:
        return None

    return f"+{digits}"


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