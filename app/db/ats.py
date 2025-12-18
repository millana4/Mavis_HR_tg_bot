import re
import logging
from typing import List, Dict


logger = logging.getLogger(__name__)


async def give_employee_data(search_type: str, search_query: str, employees: List[Dict]) -> List[Dict]:
    """
    Ищет сотрудников в данных справочника по строке search_query в списке employees.
    На вход нужно передать тип поиска:
    - По ФИО: "Name/Department"
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
        # Берём ФИО/отдел, если оно есть
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

    # Имя жирным
    if emp.get("Name/Department"):
        parts.append(f"<b>{emp['Name/Department']}</b>")

    if emp.get("Number"):
        parts.append(f"Телефон: {emp['Number']}")

    if emp.get("Email"):
        parts.append(f"Email: {emp['Email']}")

    if emp.get("Location"):
        parts.append(f"Рабочее место: {emp['Location']}")

    if emp.get("Position"):
        parts.append(f"Должность: {emp['Position']}")

    if emp.get("Department"):
        parts.append(f"Отдел: {emp['Department']}")

    if emp.get("Company"):
        company_val = emp["Company"]
        if isinstance(company_val, list):
            company_val = ", ".join(company_val)
        parts.append(f"Компания: {company_val}")

    return "\n".join(parts)