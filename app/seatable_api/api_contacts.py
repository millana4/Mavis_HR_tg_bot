import pprint
import logging
from typing import List, Dict

from config import Config
from app.seatable_api.api_base import fetch_table, get_metadata

logger = logging.getLogger(__name__)

async def get_employees(table_id:str) -> List[Dict]:
    """
    Обращается по АПИ в таблицу со справочником и возвращает json с данными сотрудников.
    Пример ответа:
    [
       {'Companies': 'ООО "МАВИС"',
        'Company_segment': ['МАВИС'],
         'Date_employment': '2020-12-02',
         'Departments': 'Администрация',
         'Email_mavis': 'example@mavis.ru',
         'Email_other': 'example@mail.ru',
         'Email_votonia': 'example@votonia.ru',
         'FIO': 'Никола Тесла',
         'Internal_number': '999',
         'Location': 'каб.1',
         'Name': '111-111-111 11',
         'Number_direct': '999-99-99',
         'Phones': '+79998887766',
         'Photo': ['http://seadoc.r2d.ru/f/5b4420ff4a86468cab51/?dl=1'],
         'Positions': 'Исполнительный директор',
         'Prefix': 9,
         'Previous_surname': 'Tesla',
         '_ctime': '2025-12-18T14:15:00.869+00:00',
         '_id': 'fyvzqpThRz-wJ3CqL25flQ',
         '_mtime': '2025-12-19T12:43:20.254+00:00'},
    ]
    """
    try:
        employees_data = await fetch_table(table_id=table_id, app='USER')
        # pprint.pprint(employees_data)
        return employees_data
    except Exception as e:
        logger.error(f"API error in get_employees: {str(e)}", exc_info=True)
        return None


async def get_department_list() -> List[str]:
    """
    Обращается по АПИ в таблицу со справочником, получает метаданные таблицы.
    Затем из метаданных формирует список отделов (только названия).
    """
    try:
        ats_table_metadata = await get_metadata(app='USER')

        # Достаём список таблиц
        tables_list = ats_table_metadata.get('metadata', {}).get('tables', [])

        for table in tables_list:
            if table.get('_id') == Config.SEATABLE_ATS_BOOK_ID:
                columns = table.get('columns', [])

                # Ищем колонку Department
                for column in columns:
                    if column.get('name') == 'Department':
                        options = column.get('data', {}).get('options', [])
                        # Берём только названия отделов
                        return [opt.get("name") for opt in options if isinstance(opt, dict)]
        return []

    except Exception as e:
        print(f"Ошибка при получении списка отделов: {e}")
        return []
