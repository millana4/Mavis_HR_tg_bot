import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from aiogram.types import Update

from config import Config


class UserLoggingMiddleware:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    async def __call__(self, handler, event: Update, data: dict):
        user_id = None
        if event.message:
            user_id = event.message.from_user.id
        elif event.callback_query:
            user_id = event.callback_query.from_user.id

        if user_id:
            self.logger.info(f"Update id={event.update_id}", extra={'user_id': user_id})
        else:
            self.logger.info(f"Update id={event.update_id} (no user_id)")

        return await handler(event, data)


class UserIdFilter(logging.Filter):
    """Фильтр для добавления ID пользователя в логи"""
    def filter(self, record):
        if hasattr(record, 'user_id'):
            if record.msg.startswith(f"[user:{record.user_id}]"):
                return True
            record.msg = f"[user:{record.user_id}] {record.msg}"
        return True

def setup_logging():
    """Настройка логирования для всего проекта"""
    # Создаем папку для логов от корня проекта
    project_root = Path(__file__).resolve().parent.parent
    log_dir = project_root / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / "bot.log"

    # Основные настройки — уровень из переменной окружения LOG_LEVEL
    log_level = getattr(logging, Config.LOG_LEVEL.upper(), logging.INFO)
    logger = logging.getLogger()
    logger.setLevel(log_level)

    # Формат логов
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Наш кастомный фильтр
    user_filter = UserIdFilter()

    # Файловый обработчик
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,
        backupCount=3,
        encoding='utf-8',
        delay=False,  # открыть файл сразу, не лениво
    )
    file_handler.setFormatter(formatter)
    file_handler.addFilter(user_filter)

    # Отключаем буферизацию: flush после каждой записи
    _orig_emit = file_handler.emit
    def _emit_and_flush(record):
        _orig_emit(record)
        try:
            file_handler.flush()
        except Exception:
            pass
    file_handler.emit = _emit_and_flush

    # Консольный обработчик
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.addFilter(user_filter)

    # Добавляем обработчики
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    # Настройка логирования aiogram
    aiogram_logger = logging.getLogger('aiogram')
    aiogram_logger.setLevel(logging.INFO)