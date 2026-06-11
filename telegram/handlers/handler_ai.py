"""
Роутер интеграции с ИИ-агентом.

Поток:
  1. Пользователь нажимает «Спросить ИИ» (callback ai:start) или выбирает
     раздел AI_FAQ в меню → start_ai_conversation.
  2. Ставим состояние AI_CONVERSATION, показываем приветствие.
  3. Любой текст в этом состоянии уходит агенту (handle_ai_question).
     Пользователь остаётся в режиме ИИ, пока не нажмёт «В главное меню».
  4. Агент возвращает либо текст (search_internal/answer_general), либо tool_call.
  5. tool_call диспетчится на функции поиска / HR-форму. После показа
     результата состояние остаётся AI_CONVERSATION — диалог продолжается.

Диспетчинг поиска изолирован: режим ИИ сам вызывает низкоуровневые функции
поиска и показывает результат, не дёргая хендлеры справочника (чтобы их
состояния не перехватывали ввод и не выбивали пользователя из режима ИИ).
"""
import asyncio
import logging
from typing import Dict, List

from aiogram import Router, types, F, Bot
from aiogram.types import Message

from config import Config
from app.services.fsm import state_manager, AppStates
from app.services.utils import mask_pii
from app.db.table_data import fetch_table
from app.db.contacts import (
    give_employee_data,
    format_employee_text,
    give_unit_data,
    format_unit_text,
    format_ats_internal,
)
from app.clients.ai_agent_client import ask_agent, extract_tool_call, AIAgentError

from telegram.handlers.handler_form import process_form
from telegram.handlers.handler_base import start_navigation
from telegram.keyboards import AI_GO_BACK_KEYBOARD, AI_HR_FORM_KEYBOARD
from telegram.utils import check_access

router = Router()
logger = logging.getLogger(__name__)

# Автоудаление сообщений с ПД (как в справочнике контактов) — 2 минуты.
AI_AUTODELETE_TIMER = 120

# Приветствие при входе в режим ИИ.
AI_WELCOME_TEXT = (
    "Вы в режиме ИИ-помощника 🤖\n\n"
    "Можете искать по корпоративным документам, задать вопрос по базе знаний, "
    "а также получить контакты сотрудников или подразделений."
)


# ============================================================================
# Вход в режим ИИ
# ============================================================================

async def start_ai_conversation(message: Message):
    """
    Включить режим диалога с ИИ: проверить доступ, поставить состояние,
    показать приветствие.
    """
    user_id = message.chat.id

    has_access = await check_access(message=message)
    if not has_access:
        return

    await state_manager.update_data(user_id, current_state=AppStates.AI_CONVERSATION)
    logger.info(f"Пользователь {user_id} вошёл в режим ИИ")

    await message.answer(AI_WELCOME_TEXT, reply_markup=AI_GO_BACK_KEYBOARD)


@router.callback_query(F.data == "ai:start")
async def ai_start_callback(callback: types.CallbackQuery):
    """Вход в режим ИИ по кнопке."""
    has_access = await check_access(callback_query=callback)
    if not has_access:
        return
    try:
        await callback.message.delete()
    except Exception:
        pass
    await start_ai_conversation(callback.message)
    await callback.answer()


@router.callback_query(F.data == "ai:exit")
async def ai_exit_callback(callback: types.CallbackQuery):
    """Кнопка «В главное меню» — выход из режима ИИ, сразу рисуем главное меню."""
    user_id = callback.from_user.id

    has_access = await check_access(callback_query=callback)
    if not has_access:
        return

    # Убираем кнопку у предыдущего сообщения и удаляем его.
    try:
        await callback.message.delete()
    except Exception:
        pass

    user_data = await state_manager.get_data(user_id)
    current_role = user_data.get("role")

    # start_navigation сам сбросит состояние и историю навигации и отрисует
    # главное меню (как при /start).
    await start_navigation(message=callback.message, current_role=current_role)
    await callback.answer()


# ============================================================================
# Приём вопроса пользователя в режиме ИИ
# ============================================================================

class _AIConversationFilter:
    """Фильтр: пользователь в состоянии AI_CONVERSATION."""
    async def __call__(self, message: types.Message) -> bool:
        data = await state_manager.get_data(message.from_user.id)
        return data.get("current_state") == AppStates.AI_CONVERSATION


@router.message(F.text, F.content_type == "text", _AIConversationFilter())
async def handle_ai_question(message: Message):
    """
    Главный обработчик: получает вопрос, шлёт агенту, диспетчит ответ.
    Пользователь остаётся в режиме ИИ — следующий текст снова попадёт сюда.
    """
    user_id = message.from_user.id

    # Игнорируем команды меню.
    if message.text.startswith("/"):
        return

    has_access = await check_access(message=message)
    if not has_access:
        return

    request_text = message.text.strip()
    if not request_text:
        await message.answer("Пожалуйста, напишите ваш вопрос.", reply_markup=AI_GO_BACK_KEYBOARD)
        return

    logger.info(f"ИИ-вопрос от {user_id}: {mask_pii(request_text)}")

    # Индикатор «печатает» — запрос к агенту может занять несколько секунд.
    await message.bot.send_chat_action(chat_id=message.chat.id, action="typing")

    try:
        response = await ask_agent(user_id=user_id, request_text=request_text)
    except AIAgentError as exc:
        logger.error(f"Ошибка обращения к ИИ-агенту: {exc}")
        await message.answer(
            "Не удалось получить ответ от ИИ-помощника. "
            "Попробуйте повторить запрос чуть позже.",
            reply_markup=AI_GO_BACK_KEYBOARD,
        )
        return

    await _dispatch_agent_response(message, response)


async def _dispatch_agent_response(message: Message, response: Dict):
    """Разобрать ответ агента и выполнить нужное действие."""
    tool_call = extract_tool_call(response)

    # Текстовый ответ (search_internal / answer_general).
    if tool_call is None:
        answer = response.get("answer") or "Не удалось сформировать ответ."
        await message.answer(answer, reply_markup=AI_GO_BACK_KEYBOARD)
        return

    tool_name = tool_call.get("name")
    args = tool_call.get("args") or {}
    query = (args.get("query") or "").strip()

    logger.info(f"ИИ tool_call: {tool_name}, query={mask_pii(query)}")

    if tool_name == "search_contacts":
        await _ai_search_contacts(message, query)
    elif tool_name == "search_ats_mavis":
        await _ai_search_department(message, query, Config.ATS_MAVIS_BOOK_ID)
    elif tool_name == "search_ats_votonia":
        await _ai_search_department(message, query, Config.ATS_VOTONIA_BOOK_ID)
    elif tool_name == "search_shop":
        await _ai_search_unit(message, query, Config.SHOP_TABLE_ID)
    elif tool_name == "search_drugstore":
        await _ai_search_unit(message, query, Config.DRUGSTORE_TABLE_ID)
    elif tool_name == "suggest_hr_form":
        await _ai_suggest_hr_form(message, response)
    else:
        logger.warning(f"Неизвестный tool от ИИ: {tool_name}")
        await message.answer(
            "Не удалось обработать запрос. Попробуйте переформулировать.",
            reply_markup=AI_GO_BACK_KEYBOARD,
        )


# ============================================================================
# Диспетчинг: поиск контактов по ФИО
# ============================================================================

async def _ai_search_contacts(message: Message, query: str):
    """Поиск сотрудника по ФИО (сегмент both), показ с автоудалением ПД."""
    if not query:
        await message.answer(
            "Уточните, пожалуйста, фамилию или имя сотрудника.",
            reply_markup=AI_GO_BACK_KEYBOARD,
        )
        return

    employees = await fetch_table(table_id=Config.PIVOT_TABLE_ID, app="USER")
    found = await give_employee_data("FIO", query, employees, "both")
    await _show_ai_employees(message, found, group_ats=False)


# ============================================================================
# Диспетчинг: телефоны отделов (Мавис/Вотоня)
# ============================================================================

async def _ai_search_department(message: Message, query: str, table_id: str):
    """Поиск телефонов отдела. Телефоны группируем по ФИО (format_ats_internal)."""
    if not query:
        await message.answer(
            "Уточните, пожалуйста, название отдела.",
            reply_markup=AI_GO_BACK_KEYBOARD,
        )
        return

    employees = await fetch_table(table_id=table_id, app="USER")
    found = await give_employee_data("Department", query, employees)
    await _show_ai_employees(message, found, group_ats=True)


# ============================================================================
# Диспетчинг: магазины / аптеки
# ============================================================================

async def _ai_search_unit(message: Message, query: str, table_id: str):
    """Поиск магазина или аптеки по части адреса."""
    if not query:
        await message.answer(
            "Уточните, пожалуйста, часть адреса.",
            reply_markup=AI_GO_BACK_KEYBOARD,
        )
        return

    unit_data = await fetch_table(table_id=table_id, app="USER")
    found = await give_unit_data(query, unit_data)

    if not found:
        await message.answer(
            "По вашему запросу ничего не нашли. Попробуйте уточнить адрес.",
            reply_markup=AI_GO_BACK_KEYBOARD,
        )
        return

    full_text = "\n\n".join(format_unit_text(u) for u in found)
    await message.answer(full_text, parse_mode="HTML", reply_markup=AI_GO_BACK_KEYBOARD)


# ============================================================================
# Диспетчинг: форма HR (suggest_hr_form)
# ============================================================================

async def _ai_suggest_hr_form(message: Message, response: Dict):
    """
    Агент не нашёл ответ — показываем текст и кнопки (Написать HR / в меню).
    Саму форму запускаем по нажатию «Написать HR» (ai:hr_form).
    """
    text = response.get("answer") or (
        "Не удалось найти ответ на ваш вопрос. "
        "Вы можете обратиться в HR — мы поможем."
    )
    await message.answer(text, reply_markup=AI_HR_FORM_KEYBOARD)


@router.callback_query(F.data == "ai:hr_form")
async def ai_hr_form_callback(callback: types.CallbackQuery):
    """Запуск HR-формы из режима ИИ."""
    has_access = await check_access(callback_query=callback)
    if not has_access:
        return

    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    if not Config.FEEDBACK_TABLE_ID:
        logger.error("FEEDBACK_TABLE_ID не задан — не могу открыть HR-форму")
        await callback.message.answer("Форма обращения временно недоступна.")
        await callback.answer()
        return

    table_data = await fetch_table(table_id=Config.FEEDBACK_TABLE_ID, app="HR")
    if not table_data:
        await callback.message.answer("Не удалось загрузить форму обращения.")
        await callback.answer()
        return

    # process_form сам выставит состояние FORM_DATA и поведёт по форме.
    await process_form(table_data, callback.message)
    await callback.answer()


# ============================================================================
# Показ сотрудников с автоудалением ПД (изолировано от справочника)
# ============================================================================

async def _show_ai_employees(message: Message, found: List[Dict], group_ats: bool):
    """
    Показать найденных сотрудников в режиме ИИ и поставить таймер автоудаления.
    Состояние НЕ меняем — пользователь остаётся в режиме ИИ.

    group_ats=True — группируем телефоны по ФИО (для поиска по отделам).
    """
    if not found:
        await message.answer(
            "По вашему запросу никого не нашли. Попробуйте уточнить запрос.",
            reply_markup=AI_GO_BACK_KEYBOARD,
        )
        return

    employees = await format_ats_internal(found) if group_ats else found

    max_length = 3800
    full_text = ""
    shown = 0
    for emp in employees:
        emp_text = await format_employee_text(emp)
        candidate = (full_text + "\n\n" + emp_text) if full_text else emp_text
        if len(candidate) > max_length:
            full_text += "\n\n❗️Показаны не все результаты — уточните запрос."
            break
        full_text = candidate
        shown += 1
    if shown < len(employees) and "❗️" not in full_text:
        full_text += f"\n\n... и ещё {len(employees) - shown} сотрудник(ов)."

    sent = await message.answer(
        full_text,
        parse_mode="HTML",
        reply_markup=AI_GO_BACK_KEYBOARD,
    )

    # Таймер автоудаления ПД (как в справочнике контактов), но без сброса
    # состояния — пользователь остаётся в режиме ИИ.
    await asyncio.create_task(
        _delete_ai_personal_data(message.bot, message.chat.id, sent.message_id)
    )


async def _delete_ai_personal_data(bot: Bot, chat_id: int, message_id: int):
    """Удалить сообщение с ПД через AI_AUTODELETE_TIMER секунд."""
    try:
        await asyncio.sleep(AI_AUTODELETE_TIMER)
        try:
            await bot.delete_message(chat_id=chat_id, message_id=message_id)
            logger.debug(f"По таймеру удалено ИИ-сообщение с ПД {message_id}")
        except Exception as exc:
            logger.debug(f"ИИ-сообщение {message_id} уже удалено: {exc}")
    except Exception as exc:
        logger.error(f"Ошибка таймера удаления ИИ-сообщения {message_id}: {exc}")