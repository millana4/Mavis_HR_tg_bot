import logging
from typing import List, Dict, Optional
from datetime import datetime

from config import Config
from app.db.nocodb_client import NocoDBClient

logger = logging.getLogger(__name__)


def is_form(table_data: List[Dict]) -> bool:
    """Проверяет, является ли таблица формой"""
    has_form_fields = False
    for row in table_data:
        # Если есть признаки меню - точно не форма
        if any(field in row for field in ['Submenu_link', 'Button_content', 'External_link']):
            return has_form_fields

        # Проверяем признаки формы
        if ('Free_input' in row) or any(key.startswith('Answer_option_') for key in row.keys()):
            has_form_fields = True

    # Answers_table проверяем только в строке Info
    info_row = next((row for row in table_data if row.get('Name') == 'Info'), {})
    if info_row.get('Answers_table'):
        has_form_fields = True

    return has_form_fields


async def start_form_questions(table_data: List[Dict]) -> Dict:
    """Получает вопросы из формы"""
    questions = [row for row in table_data
                if row.get('Name') not in ['Info', 'Final_message']]

    # Безопасное получение answers_table (может быть None)
    info_row = next((row for row in table_data if row.get('Name') == 'Info'), {})
    answers_table = info_row.get('Answers_table')

    # Безопасное получение final_message (может быть None)
    final_row = next((row for row in table_data if row.get('Name') == 'Final_message'), {})
    final_message = final_row.get('Content')

    return {
        "questions": questions,
        "current_question": 0,
        "answers": [],
        "answers_table": answers_table,  # Может быть None
        "final_message": final_message  # Может быть None
    }


async def prepare_data_to_post_in_seatable(form_data: Dict) -> Optional[Dict]:
    """
    Подготавливает данные из форм обратной связи для сохранения в Seatable.
    Возвращает словарь с данными или None в случае ошибки.
    """
    # Проверяем обязательные поля
    required_fields = ['user_id', 'questions', 'answers', 'answers_table']
    if any(field not in form_data for field in required_fields):
        logger.error(f"Отсутствуют обязательные поля: {[f for f in required_fields if f not in form_data]}")
        return None

    if len(form_data['questions']) != len(form_data['answers']):
        logger.error(f"Количество вопросов ({len(form_data['questions'])}) != ответов ({len(form_data['answers'])})")
        return None

    # Извлекаем table_id из URL
    try:
        from urllib.parse import urlparse, parse_qs
        parsed_url = urlparse(form_data['answers_table'])
        query_params = parse_qs(parsed_url.query)
        table_id = query_params.get('tid')[0]
        logger.info(f"Table ID: {table_id}")
    except Exception as e:
        logger.error(f"Ошибка парсинга URL таблицы: {e}")
        return None

    # Подготавливаем данные
    try:
        from datetime import datetime
        timestamp = form_data.get('timestamp', datetime.now().isoformat())
        formatted_date = datetime.fromisoformat(timestamp).strftime("%d.%m.%Y %H:%M")
    except Exception as e:
        logger.error(f"Ошибка форматирования даты: {e}")
        formatted_date = timestamp

    # Формируем строку для записи
    row_data = {
        'Name': str(form_data['user_id']),
        'Дата и время': formatted_date
    }

    # Добавляем данные пользователя
    if form_data.get('user_fio'):
        row_data['ФИО сотрудника'] = form_data['user_fio']
    if form_data.get('user_phone'):
        row_data['Телефон'] = form_data['user_phone']

    # Добавляем вопросы и ответы
    for question_data, answer in zip(form_data['questions'], form_data['answers']):
        question_text = question_data.get('Name', '')
        if question_text:
            row_data[question_text] = str(answer) if answer is not None else ''

    return {
        'row_data': row_data,
        'table_id': table_id
    }


async def complete_form(form_state: Dict, user_id: int) -> Dict:
    """Формирует финальные данные формы с корректным user_id"""
    return {
        "user_id": user_id,  # Используем переданный user_id (из message.chat.id)
        "questions": form_state["questions"],  # Добавляем вопросы в результат
        "answers": form_state["answers"],
        "answers_table": form_state["answers_table"],
        "final_message": form_state.get("final_message"),
        "timestamp": datetime.now().isoformat()
    }


async def save_form_answers(form_data: Dict) -> bool:
    """Сохраняет ответы формы в таблицу ответов NocoDB"""
    logger.info("Начало сохранения ответов формы")

    try:
        # Получаем данные пользователя
        async with NocoDBClient() as client:
            users = await client.get_all(table_id=Config.AUTH_TABLE_ID)

            # Добавляем данные пользователя в form_data
            for user in users:
                current_id = str(user.get('ID_messenger'))
                if current_id == str(form_data.get('user_id')):
                    form_data['user_fio'] = user.get('FIO')
                    form_data['user_phone'] = user.get('Phone')
                    break

        # Подготавливаем данные для записи
        prepared_data = await prepare_data_to_post_in_seatable(form_data)
        if not prepared_data:
            logger.error("Не удалось подготовить данные для сохранения")
            return False

        row_data = prepared_data['row_data']
        answers_table_id = prepared_data['table_id']

        logger.info(f"Данные для записи: {row_data}")

        # Записываем ответы (NocoDB проигнорирует несуществующие колонки)
        async with NocoDBClient() as client:
            logger.info(f"Отправка данных в таблицу ответов {answers_table_id}")
            result = await client.create_record(table_id=answers_table_id, data=row_data)

            if result:
                logger.info(f"Ответы успешно сохранены. ID новой записи: {result[0].get('Id')}")
                return True
            else:
                logger.error("Не удалось сохранить ответы")
                return False

    except Exception as e:
        logger.error(f"Ошибка при сохранении ответов: {e}", exc_info=True)
        return False