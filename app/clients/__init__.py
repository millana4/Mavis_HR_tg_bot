"""
HTTP-клиент к ИИ-агенту (отдельный сервис).

Бот отправляет вопрос пользователя на POST /api/ask и получает ответ:
либо текст (response_type=text), либо команду на действие
(response_type=tool_call) с именем tool и аргументами.

Контракт ответа агента:
    {
        "response_type": "text",
        "answer": "...",
        "tool_used": "search_internal" | "answer_general",
        "correlation_id": "..."
    }
  или
    {
        "response_type": "tool_call",
        "tool_calls": [{"name": "search_contacts", "args": {"query": "..."}}],
        "correlation_id": "..."
    }
"""
import logging
import uuid
from typing import Any, Dict, Optional

import aiohttp

from config import Config

logger = logging.getLogger(__name__)

# Таймаут запроса к агенту. LLM может отвечать небыстро (несколько секунд),
# поэтому берём с запасом.
_TIMEOUT_SECONDS = 60


class AIAgentError(Exception):
    """Ошибка обращения к ИИ-агенту."""


async def ask_agent(user_id: int, request_text: str) -> Dict[str, Any]:
    """
    Отправить вопрос пользователя ИИ-агенту и вернуть распарсенный ответ.

    Args:
        user_id: Telegram ID пользователя
        request_text: текст вопроса (как ввёл пользователь)

    Returns:
        Словарь-ответ агента (см. контракт выше).

    Raises:
        AIAgentError при сетевой ошибке, таймауте или плохом статусе.
    """
    if not Config.AI_AGENT_URL or not Config.AI_AGENT_API_KEY:
        raise AIAgentError("AI_AGENT_URL или AI_AGENT_API_KEY не заданы в конфиге")

    url = f"{Config.AI_AGENT_URL.rstrip('/')}/api/ask"
    correlation_id = str(uuid.uuid4())
    headers = {
        "X-API-Key": Config.AI_AGENT_API_KEY,
        "Content-Type": "application/json",
    }
    payload = {
        "user_id": user_id,
        "request": request_text,
        "correlation_id": correlation_id,
    }

    timeout = aiohttp.ClientTimeout(total=_TIMEOUT_SECONDS)

    logger.info(
        f"AI-агент запрос: user_id={user_id}, correlation_id={correlation_id}"
    )

    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, headers=headers, json=payload) as response:
                text_body = await response.text()
                if response.status != 200:
                    logger.error(
                        f"AI-агент вернул HTTP {response.status}: {text_body[:300]}"
                    )
                    raise AIAgentError(f"HTTP {response.status}")
                data = await response.json()
    except aiohttp.ClientError as exc:
        logger.error(f"AI-агент сетевая ошибка: {exc}")
        raise AIAgentError(f"Сетевая ошибка: {exc}") from exc
    except Exception as exc:
        logger.error(f"AI-агент непредвиденная ошибка: {exc}")
        raise AIAgentError(str(exc)) from exc

    logger.info(
        f"AI-агент ответ: response_type={data.get('response_type')}, "
        f"correlation_id={correlation_id}"
    )
    return data


def extract_tool_call(response: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Достать первый tool_call из ответа агента.

    Returns:
        {"name": ..., "args": {...}} или None, если это текстовый ответ.
    """
    if response.get("response_type") != "tool_call":
        return None
    tool_calls = response.get("tool_calls") or []
    if not tool_calls:
        return None
    return tool_calls[0]