import pprint
import re
import logging
from typing import List, Dict


logger = logging.getLogger(__name__)


async def get_department_list() -> List[str]:
    """
    Получает список отделов из таблицы ATS_BOOK_ID.
    Запрашивает все записи, извлекает уникальные значения поля Department.
    """
    try:
        from app.db.nocodb_client import NocoDBClient
        from config import Config

        departments = set()

        async with NocoDBClient() as client:
            # Получаем все записи из таблицы ATS_BOOK_ID
            records = await client.get_all(table_id=Config.ATS_BOOK_ID)

            # Собираем уникальные значения Department
            for record in records:
                department = record.get("Department")
                if department:  # Проверяем что не None и не пустая строка
                    departments.add(department)

        # Возвращаем отсортированный список
        return sorted(list(departments))

    except Exception as e:
        logger.error(f"Ошибка при получении списка отделов: {e}")
        return []


async def give_employee_data(search_type: str, search_query: str, employees: List[Dict],
                             selected_segment: str = None) -> List[Dict]:
    """
    Ищет сотрудников в данных справочника по строке search_query в списке employees.
    На вход нужно передать тип поиска:
    - По ФИО: "FIO" (также ищет по локации, если по ФИО не найдено)
    - По отделу: "Department"
    - selected_segment: "mavis", "votonia", "both" или None (если не фильтровать)
    Возвращает список с данными найденных сотрудников.
    """
    nickname_map = {
        # Женские имена
        "настя": ["анастасия"], "настюша": ["анастасия"],
        "геля": ["ангелина"],
        "лика": ["анжелика"],
        "аня": ["анна"],
        "тоня": ["антонина", "тоня"],
        "лера": ["валерия"],
        "вика": ["виктория"],
        "галя": ["галина"],
        "даша": ["дарья"],
        "катя": ["екатерина", "катерина"], "катюша": ["екатерина", "катерина"],
        "лена": ["елена"],
        "лиза": ["елизавета"],
        "зина": ["зинаида"],
        "ира": ["ирина"],
        "ксюша": ["ксения"],
        "лида": ["лидия"],
        "лиля": ["лилия"],
        "люба": ["любовь"],
        "люда": ["людмила"],
        "марго": ["маргарита"],
        "мариша": ["марина"],
        "маша": ["мария", "марья"],
        "надя": ["надежда"],
        "наташа": ["наталья", "наталия"],
        "леся": ["олеся"],
        "оля": ["ольга"],
        "поля": ["полина"],
        "рая": ["раиса"],
        "света": ["светлана"],
        "соня": ["софия", "софья"],
        "тая": ["таисия"],
        "тома": ["тамара"],
        "таня": ["татьяна"],
        "уля": ["ульяна"],
        "юля": ["юлия"],

        # Мужские имена
        "лёша": ["алексей"], "алёша": ["алексей"], "леша": ["алексей"],
        "толя": ["анатолий"],
        "аркаша": ["аркадий"],
        "тема": ["артем"],
        "боря": ["борис"],
        "вадик": ["вадим"],
        "вася": ["василий"],
        "витя": ["виктор"],
        "виталик": ["виталий"],
        "володя": ["владимир"], "вова": ["владимир"],
        "влад": ["владислав"],
        "гена": ["геннадий"],
        "гоша": ["георгий"], "жора": ["георгий"],
        "гриша": ["григорий"],
        "даня": ["даниил"],
        "ден": ["денис"],
        "дима": ["дмитрий"], "митя": ["дмитрий"],
        "ваня": ["иван"],
        "ильюша": ["илья"],
        "костя": ["константин"],
        "лёня": ["леонид"], "леня": ["леонид"],
        "макс": ["максим"],
        "миша": ["михаил"],
        "коля": ["николай"],
        "паша": ["павел"],
        "петя": ["петр"],
        "рома": ["роман"],
        "серёжа": ["сергей"],
        "стёпа": ["степан"], "степа": ["степан"],
        "федя": ["федор"],
        "эдик": ["эдуард"],
        "юра": ["юрий"],
        "яша": ["яков"],

        # Универсальные имена (могут быть и мужскими и женскими)
        "женя": ["евгений", "евгения"],
        "саша": ["александр", "александра"],
        "валя": ["валентина", "валентин"],
        "валера": ["валерий", "валерия"],
    }

    results = []
    if not employees:
        return results

    # Нормализуем запрос
    query = search_query.strip().lower()
    query_words = re.split(r"\s+", query)

    # Нормализация: каждое слово из запроса заменяем на все возможные варианты из словаря
    normalized_query_variants = []
    was_normalized = []  # Флаг, было ли слово нормализовано
    for word in query_words:
        if word in nickname_map:
            normalized_query_variants.append(nickname_map[word])
            was_normalized.append(True)
        else:
            normalized_query_variants.append([word])
            was_normalized.append(False)

    for emp in employees:
        # Проверяем сегмент, если указан
        if selected_segment and selected_segment != "both":
            company_segment = emp.get('Company_segment')
            if not company_segment:
                continue

            # Определяем, подходит ли сотрудник под выбранный сегмент
            segment_allowed = False

            if selected_segment == "mavis":
                if company_segment in ["МАВИС", "ОБА"]:
                    segment_allowed = True
            elif selected_segment == "votonia":
                if company_segment in ["ВОТОНЯ", "ОБА"]:
                    segment_allowed = True

            if not segment_allowed:
                continue

        # Проверяем поле для поиска
        if search_type == "FIO":
            # Ищем по ФИО
            name_field = emp.get("FIO", "")
            if name_field:
                # Разбиваем ФИО на части и берем первые два слова (фамилия и имя)
                name_parts = name_field.lower().split()
                if len(name_parts) >= 2:
                    # Берем фамилию и имя (первые два слова)
                    name_to_search = f"{name_parts[0]} {name_parts[1]}"
                    first_name = name_parts[1]  # Имя (второе слово)
                else:
                    # Если вдруг только одно слово, используем его
                    name_to_search = name_parts[0] if name_parts else ""
                    first_name = name_parts[0] if name_parts else ""

                found = False

                # Если одно слово в запросе
                if len(normalized_query_variants) == 1:
                    # Перебираем все варианты нормализованного слова
                    for w1 in normalized_query_variants[0]:
                        if was_normalized[0]:
                            # Если слово было нормализовано, ищем только в имени
                            if w1 == first_name:
                                found = True
                                break
                        else:
                            # Если не нормализовано, ищем подстроку во всей строке фамилия+имя
                            if w1 in name_to_search:
                                found = True
                                break

                # Если два слова в запросе
                elif len(normalized_query_variants) >= 2:
                    # Перебираем все комбинации
                    for w1 in normalized_query_variants[0]:
                        for w2 in normalized_query_variants[1]:
                            # Проверяем совпадение в любом порядке (фамилия имя или имя фамилия)
                            if f"{w1} {w2}" in name_to_search or f"{w2} {w1}" in name_to_search:
                                found = True
                                break
                        if found:
                            break

                if found:
                    results.append(emp)
                    continue  # Если нашли по ФИО, не ищем по локации

            # Если не нашли по ФИО, ищем по локации (без нормализации)
            location = emp.get("Location", "")
            if location:
                location_lower = location.lower()
                if len(query_words) == 1:
                    if query_words[0] in location_lower:
                        results.append(emp)
                elif len(query_words) >= 2:
                    w1, w2 = query_words[0], query_words[1]
                    if f"{w1} {w2}" in location_lower or f"{w2} {w1}" in location_lower:
                        results.append(emp)

        else:  # Поиск по отделу
            dept_field = emp.get("Department", "")
            if not dept_field:
                continue

            dept_lower = dept_field.lower()

            # --- Одинарный запрос
            if len(query_words) == 1:
                if query_words[0] in dept_lower:
                    results.append(emp)

            # --- Два слова
            elif len(query_words) >= 2:
                w1, w2 = query_words[0], query_words[1]
                if f"{w1} {w2}" in dept_lower or f"{w2} {w1}" in dept_lower:
                    results.append(emp)

    logger.info(f"По запросу '{search_query}' (сегмент: {selected_segment}) найдено {len(results)} сотрудник(ов)")
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

    emails = []
    if emp.get("Email_mavis"):
        emails.append(f"{emp['Email_mavis']} ")
    if emp.get("Email_votonia"):
        emails.append(f"{emp['Email_votonia']} ")
    if emp.get("Email_other"):
        emails.append(f"{emp['Email_other']}")

    if emails:
        parts.append(f"Email: {', '.join(emails)}")

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
        parts.append(f"<b>{unit['Title']}</b>")

    if unit.get("Email"):
        parts.append(f"Email: {unit['Email']}")

    if unit.get("Internal_number"):
        parts.append(f"Внутренний телефон: {unit['Internal_number']}")


    if not parts:
        return "Нет информации"

    return "\n".join(parts)