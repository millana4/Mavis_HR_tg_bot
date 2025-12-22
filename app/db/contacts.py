import re
import logging
from typing import List, Dict


logger = logging.getLogger(__name__)


async def give_employee_data(search_type: str, search_query: str, employees: List[Dict]) -> List[Dict]:
    """
    Ищет сотрудников в данных справочника по строке search_query в списке employees.
    На вход нужно передать тип поиска:
    - По ФИО: "FIO"
    - По отделу: "Department"
    Возвращает список с данными найденных сотрудников.
    """
    results = []
    if not employees:
        return results

    # Нормализуем запрос
    query = search_query.strip().lower()
    query_words = re.split(r"\s+", query)

    for emp in employees:
        # Берём ФИО, если оно есть
        name_field = emp.get(search_type, "")
        if not name_field:
            continue

        name_norm = name_field.lower()

        # --- Одинарный запрос (только имя или фамилия)
        if len(query_words) == 1:
            if query_words[0] in name_norm:
                results.append(emp)

        # --- Два слова (имя + фамилия в любом порядке)
        elif len(query_words) >= 2:
            w1, w2 = query_words[0], query_words[1]
            if f"{w1} {w2}" in name_norm or f"{w2} {w1}" in name_norm:
                results.append(emp)

    logger.info(f"По запросу '{search_query}' найдено {len(results)} сотрудник(ов)")
    return results


def format_employee_text(emp: Dict) -> str:
    """
    Форматирует данные одного сотрудника в текст.
    """
    parts = []

    if emp.get("FIO"):
        parts.append(f"<b>{emp['FIO']}</b>")

    if emp.get("Raw_owner"):          # Если выводим из АТС
        parts.append(f"<b>{emp['Raw_owner']}</b>")

    if emp.get("Email_mavis") or emp.get("Email_votonia") or emp.get("Email_other"):
        parts.append(f"Email: {emp.get("Email_mavis")} {emp.get('Email_votonia')} {emp.get('Email_other')}")

    if emp.get("Internal_number"):
        parts.append(f"Внутренний телефон: {emp['Internal_number']}")
        if emp.get("Prefix"):
            parts.append(f"Префикс: {emp['Prefix']}")
        if emp.get("Number_direct"):
            parts.append(f"Городской номер: {emp['Number_direct']}")

    if emp.get("Location"):
        parts.append(f"Рабочее место: {emp['Location']}")

    if emp.get("Positions"):
        parts.append(f"Должность: {emp['Positions']}")

    if emp.get("Departments"):
        parts.append(f"Отдел: {emp['Departments']}")

    return "\n".join(parts)


async def give_unit_data(search_query: str, unit_data: List[Dict]) -> List[Dict]:
    """Ищет данные подразделения - почту и телефон для аптеки или магазина. На вход принимает подстроку с частью адреса"""
    results = []
    if not unit_data:
        return results

    # Нормализуем запрос
    query = search_query.strip().lower()
    query_words = re.split(r"\s+", query)

    for unit in unit_data:
        # Берём название подразделения, если оно есть
        title_field = unit.get("Title", "")
        if not title_field:
            continue

        title_norm = title_field.lower()

        # --- Одинарный запрос (одна подстрока)
        if len(query_words) == 1:
            if query_words[0] in title_norm:
                results.append(unit)

        # --- Две и больше подстроки
        elif len(query_words) >= 2:
            w1, w2 = query_words[0], query_words[1]
            if f"{w1} {w2}" in title_norm or f"{w2} {w1}" in title_norm:
                results.append(unit)

    logger.info(f"По запросу '{search_query}' найдено {len(results)} сотрудник(ов)")
    return results


def format_unit_text(unit: Dict) -> str:
    """
    Форматирует данные магазина/аптеки в текст.
    """
    parts = []

    if unit.get("Title"):
        parts.append(f"<b>{unit["Title"]}</b>")

    if unit.get("Email"):
        parts.append(f"Email: {unit["Email"]}")

    if unit.get("Internal_number"):
        parts.append(f"Внутренний телефон: {unit["Internal_number"]}")


    if not parts:
        return "Нет информации"

    return "\n".join(parts)