import pprint
import re
import html
import logging
from typing import Dict, List

from aiogram import types
from aiogram.client.session import aiohttp

from config import Config

logger = logging.getLogger(__name__)


async def download_and_send_file(file_url: str, callback_query: types.CallbackQuery):
    """Скачивает файл из хранилища и отправляет его в чат"""
    try:
        # Добавляем параметр для скачивания файла
        if '?' not in file_url:
            download_url = file_url + '?dl=1'
        else:
            download_url = file_url + '&dl=1'

        async with aiohttp.ClientSession() as session:
            # Сначала получаем HTML чтобы узнать название файла
            async with session.get(file_url) as html_response:
                html_content = await html_response.text()

                # Парсим название файла из HTML
                filename = extract_filename_from_html(html_content)
                if not filename:
                    filename = "file"

            # Скачиваем сам файл с параметром dl=1
            async with session.get(download_url) as file_response:
                file_response.raise_for_status()
                file_data = await file_response.read()

                file_to_send = types.BufferedInputFile(file_data, filename=filename)
                await callback_query.message.answer_document(file_to_send)

                logger.info(f"Файл {filename} отправлен в чат {callback_query.message.chat.id}")

    except Exception as e:
        logger.error(f"Ошибка при скачивании или отправке файла: {str(e)}", exc_info=True)
        await callback_query.message.answer("Не удалось отправить файл. Пожалуйста, попробуйте позже.")


def extract_filename_from_html(html_content: str) -> str:
    """Извлекает название файла из HTML"""
    try:
        import re

        # Ищем в og:title
        title_match = re.search(r'<meta property="og:title" content="([^"]+)"', html_content)
        if title_match:
            return title_match.group(1)

        # Ищем в og:description
        desc_match = re.search(r'<meta property="og:description" content="Share link for ([^"]+)"', html_content)
        if desc_match:
            return desc_match.group(1)

        return "file"

    except Exception as e:
        logger.error(f"Ошибка извлечения названия файла: {str(e)}")
        return "file"


async def process_content_part(table_data: List[Dict]) -> Dict:
    """Обрабатывает контентную часть (Info) для NocoDB"""
    logger.info(f"Поиск контентной части (Info) в данных таблицы. Количество строк: {len(table_data)}")

    for row in table_data:
        if row.get('Section') == 'Info':
            # Извлекаем текст и изображение
            content_text = row.get('Content_text', '')
            content_image = row.get('Content_image')

            # Если и текст и картинка пустые - ставим символ пальца
            if not content_text and not content_image:
                content_text = "👉"
                logger.info("Контент пустой, установлен символ пальца")

            logger.info(
                f"Найдена строка с контентом (Info). Content_text: {content_text[:20] if content_text else 'пусто'}...")
            return prepare_telegram_message(content_text, content_image)

    logger.warning("Строка с контентом (Info) не найдена!")
    return {"text": "👉"}


def prepare_telegram_message(text_content: str, image_url: str = None) -> Dict[str, str]:
    """
    Подготавливает контент для отправки в Telegram с HTML разметкой.
    Обрабатывает три случая: только текст, только картинка, текст+картинка.
    """
    result = {
        'text': text_content,
        'image_url': image_url,
        'parse_mode': 'HTML'
    }

    # Случай 1: Только картинка (текста нет)
    if image_url and not text_content:
        result['text'] = "‎"  # Невидимый символ для Telegram
        return result

    # Если есть текст - преобразуем Markdown в HTML
    if text_content:
        text = text_content

        # Обрабатываем переносы строк
        def replace_newlines(match):
            n = len(match.group(0)) // 2
            return '\n' * n

        text = re.sub(r'\n{2,}', replace_newlines, text)

        # Заголовки (#) -> <b>
        text = re.sub(r'^#+\s*(.+?)\s*$', r'<b>\1</b>', text, flags=re.MULTILINE)

        # Жирный текст
        text = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', text)

        # Курсив
        text = re.sub(r'\*(.+?)\*', r'<i>\1</i>', text)

        # Ссылки
        text = re.sub(r'\[([^\]]+)]\(([^)]+)\)', r'<a href="\2">\1</a>', text)

        # Маркированные списки
        text = re.sub(r'^\*\s+(.+)$', r'• \1', text, flags=re.MULTILINE)

        # Экранируем HTML-сущности
        text = html.escape(text)

        # Восстанавливаем теги
        replacements = {
            '&lt;b&gt;': '<b>',
            '&lt;/b&gt;': '</b>',
            '&lt;i&gt;': '<i>',
            '&lt;/i&gt;': '</i>',
            '&lt;a href=&quot;': '<a href="',
            '&quot;&gt;': '">',
            '&lt;/a&gt;': '</a>'
        }

        for old, new in replacements.items():
            text = text.replace(old, new)

        result['text'] = text.strip()

    return result
