import asyncio
import logging
from typing import List, Dict
from datetime import datetime, timedelta

from aiogram import Router, F, Bot
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile

from app.services.fsm import state_manager
from app.services.broadcast import is_user_admin, get_broadcast_notifications, get_active_users, prepare_notification_content
from telegram.handlers.handler_base import start_navigation

router = Router()
logger = logging.getLogger(__name__)


# Глобальное хранилище запланированных рассылок
scheduled_broadcasts = {}


@router.message(Command("broadcast"))
async def cmd_broadcast(message: Message):
    """Команда запускает выбор уведомления из тех, что есть в таблице Уведомления"""
    try:
        await state_manager.clear(message.from_user.id)

        # Проверяем права администратора
        if not await is_user_admin(message.from_user.id):
            await message.answer("❌ У вас нет прав для этой команды")
            return

        # Получаем список уведомлений из Seatable
        notifications = await get_broadcast_notifications()
        if not notifications:
            await message.answer("Нет уведомлений для рассылки")
            return

        # Создаем клавиатуру с названиями уведомлений
        keyboard = await create_broadcast_keyboard(notifications)

        await message.answer(
            "Выберите уведомление для рассылки",
            reply_markup=keyboard
        )

    except Exception as e:
        logger.error(f"Broadcast command error: {str(e)}")
        await message.answer("Ошибка при загрузке уведомлений")


async def create_broadcast_keyboard(notifications: List[Dict]) -> InlineKeyboardMarkup:
    """Создает клавиатуру с названиями уведомлений"""
    inline_keyboard = []

    for notification in notifications:
        name = notification.get('Section', 'Без названия')
        row_id = notification.get('Id')

        if name and row_id:
            inline_keyboard.append([
                InlineKeyboardButton(
                    text=name,
                    callback_data=f"broadcast_preview:{row_id}"
                )
            ])

    # Добавляем кнопку отмены
    inline_keyboard.append([
        InlineKeyboardButton(text="❌ Отмена", callback_data="broadcast_cancel")
    ])

    return InlineKeyboardMarkup(inline_keyboard=inline_keyboard)


@router.callback_query(F.data.startswith("broadcast_preview:"))
async def handle_broadcast_preview(callback_query: CallbackQuery, bot: Bot):
    """Обрабатывает выбор уведомления для предварительного просмотра"""
    try:
        # Сразу отвечаем на callback, чтобы избежать таймаута
        await callback_query.answer()

        # Извлекаем ID выбранного уведомления
        notification_id = callback_query.data.replace("broadcast_preview:", "")

        # Получаем данные уведомления
        notifications = await get_broadcast_notifications()
        selected_notification = next(
            (n for n in notifications if str(n.get('Id')) == notification_id),  # Приводим к строке
            None
        )

        if not selected_notification:
            await callback_query.answer("Уведомление не найдено", show_alert=True)
            return

        # Убираем клавиатуру
        await callback_query.message.edit_reply_markup(reply_markup=None)

        # Отправляем тестовое уведомление администратору
        await callback_query.message.answer(
            f"ПРЕДВАРИТЕЛЬНЫЙ ПРОСМОТР: {selected_notification.get('Section', 'Без названия')} ⬇️"
        )

        # Отправляем контент уведомления администратору
        await send_test_notification_to_admin(callback_query.from_user.id, selected_notification, bot)

        # Сохраняем выбранное уведомление в FSM
        await state_manager.update_data(
            callback_query.from_user.id,
            selected_notification_id=notification_id,
            selected_notification=selected_notification
        )

        # Создаем клавиатуру для подтверждения (заменяем "Отправить всем" на "ОК")
        confirmation_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Да",
                    callback_data="broadcast_ok"
                ),
                InlineKeyboardButton(
                    text="❌ Отмена",
                    callback_data="broadcast_cancel"
                )
            ]
        ])

        await callback_query.message.answer(
            "Вот ваше уведомление ⬆️ Ознакомьтесь, пожалуйста.\n\n"
            "Контент уведомления вас устраивает?",
            reply_markup=confirmation_keyboard
        )

    except Exception as e:
        logger.error(f"Broadcast preview error: {str(e)}")
        await callback_query.answer("Ошибка при загрузке уведомления", show_alert=True)


@router.callback_query(F.data == "broadcast_ok")
async def handle_broadcast_ok(callback_query: CallbackQuery):
    """Обрабатывает подтверждение уведомления и предлагает даты отправки"""
    try:
        user_id = callback_query.from_user.id

        # Убираем клавиатуру
        await callback_query.message.edit_reply_markup(reply_markup=None)

        # Создаем клавиатуру с датами
        dates_keyboard = await create_dates_keyboard()

        await callback_query.message.answer(
            "Когда отправить уведомление?",
            reply_markup=dates_keyboard
        )

        await callback_query.answer()

    except Exception as e:
        logger.error(f"Broadcast OK error: {str(e)}")
        await callback_query.answer("Ошибка", show_alert=True)


async def create_dates_keyboard() -> InlineKeyboardMarkup:
    """Создает клавиатуру с датами отправки на 7 дней"""
    today = datetime.now()
    dates_keyboard = []

    # Сейчас
    dates_keyboard.append([InlineKeyboardButton(
        text="Сейчас",
        callback_data="broadcast_schedule:now"
    )])

    # Сегодня и завтра
    dates_keyboard.append([InlineKeyboardButton(
        text=f"Сегодня ({today.strftime('%d.%m')})",
        callback_data=f"broadcast_schedule:today"
    )])

    dates_keyboard.append([InlineKeyboardButton(
        text=f"Завтра ({(today + timedelta(days=1)).strftime('%d.%m')})",
        callback_data=f"broadcast_schedule:tomorrow"
    )])

    # Следующие 5 дней (всего 7 дней включая сегодня и завтра)
    for i in range(2, 7):
        date = today + timedelta(days=i)
        dates_keyboard.append([InlineKeyboardButton(
            text=date.strftime('%d.%m'),
            callback_data=f"broadcast_schedule:{date.strftime('%Y-%m-%d')}"
        )])

    # Кнопка отмены
    dates_keyboard.append([InlineKeyboardButton(
        text="❌ Отмена",
        callback_data="broadcast_cancel"
    )])

    return InlineKeyboardMarkup(inline_keyboard=dates_keyboard)


@router.callback_query(F.data.startswith("broadcast_schedule:"))
async def handle_schedule_choice(callback_query: CallbackQuery):
    """Обрабатывает выбор даты отправки"""
    try:
        user_id = callback_query.from_user.id
        schedule_type = callback_query.data.replace("broadcast_schedule:", "")

        # Убираем клавиатуру
        await callback_query.message.edit_reply_markup(reply_markup=None)

        if schedule_type == "now":
            # Для отправки сейчас - подтверждение
            confirmation_keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="✅ Отправить всем",
                        callback_data="broadcast_confirm_immediate"
                    ),
                    InlineKeyboardButton(
                        text="❌ Отмена",
                        callback_data="broadcast_cancel"
                    )
                ]
            ])

            await callback_query.message.answer(
                "Вы хотите сейчас отправить уведомление?",
                reply_markup=confirmation_keyboard
            )

        else:
            # Для отложенной отправки - запрос времени
            await state_manager.update_data(
                user_id,
                selected_schedule_date=schedule_type
            )

            cancel_keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="❌ Отмена", callback_data="broadcast_cancel")
            ]])

            await callback_query.message.answer(
                "Укажите, пожалуйста, время отправки через двоеточие, например: 14:00\n"
                "Время должно быть московское.",
                reply_markup=cancel_keyboard
            )

        await callback_query.answer()

    except Exception as e:
        logger.error(f"Schedule choice error: {str(e)}")
        await callback_query.answer("Ошибка", show_alert=True)


@router.message(F.text.regexp(r'^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$'))
async def handle_time_input(message: Message):
    """Обрабатывает ввод времени"""
    try:
        user_id = message.from_user.id
        time_str = message.text.strip()

        user_data = await state_manager.get_data(user_id)
        schedule_date = user_data.get('selected_schedule_date')

        if not schedule_date:
            return

        # Форматируем дату для отображения и рассчитываем datetime
        now = datetime.now()

        if schedule_date == "today":
            display_date = f"сегодня ({now.strftime('%d.%m')})"
            schedule_datetime = now.replace(
                hour=int(time_str.split(':')[0]),
                minute=int(time_str.split(':')[1]),
                second=0,
                microsecond=0
            )

            # Если время уже прошло сегодня, предлагаем отправить сейчас
            if schedule_datetime <= now:
                # Сохраняем данные для немедленной отправки
                await state_manager.update_data(
                    user_id,
                    selected_schedule_datetime=now.isoformat(),  # Отправляем сейчас
                    display_schedule="сейчас",
                    is_immediate_send=True  # Флаг для немедленной отправки
                )

                # Предлагаем отправить сейчас
                confirmation_keyboard = InlineKeyboardMarkup(inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="✅ Да, отправить",
                            callback_data="broadcast_confirm_immediate"
                        ),
                        InlineKeyboardButton(
                            text="❌ Отмена",
                            callback_data="broadcast_cancel"
                        )
                    ]
                ])

                await message.answer(
                    f"Указанное время уже прошло.\n"
                    f"Отправить рассылку сейчас?",
                    reply_markup=confirmation_keyboard
                )
                return

        elif schedule_date == "tomorrow":
            tomorrow = now + timedelta(days=1)
            display_date = f"завтра ({tomorrow.strftime('%d.%m')})"
            schedule_datetime = tomorrow.replace(
                hour=int(time_str.split(':')[0]),
                minute=int(time_str.split(':')[1]),
                second=0,
                microsecond=0
            )

        else:
            # Для конкретной даты в будущем
            schedule_date_obj = datetime.strptime(schedule_date, '%Y-%m-%d')
            display_date = schedule_date_obj.strftime('%d.%m.%Y')
            schedule_datetime = schedule_date_obj.replace(
                hour=int(time_str.split(':')[0]),
                minute=int(time_str.split(':')[1]),
                second=0,
                microsecond=0
            )

        # Сохраняем полную дату и время для отложенной отправки
        await state_manager.update_data(
            user_id,
            selected_schedule_datetime=schedule_datetime.isoformat(),
            display_schedule=f"{display_date} в {time_str}",
            is_immediate_send=False  # Флаг для отложенной отправки
        )

        # Подтверждение отложенной отправки
        confirmation_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Да, запланировать",
                    callback_data="broadcast_confirm_scheduled"
                ),
                InlineKeyboardButton(
                    text="❌ Отмена",
                    callback_data="broadcast_cancel"
                )
            ]
        ])

        await message.answer(
            f"Вы хотите запланировать отправку уведомления на {display_date} в {time_str}?",
            reply_markup=confirmation_keyboard
        )

    except Exception as e:
        logger.error(f"Time input error: {str(e)}")
        await message.answer("Ошибка при обработке времени")


@router.callback_query(F.data == "broadcast_confirm_immediate")
async def handle_immediate_broadcast(callback_query: CallbackQuery, bot: Bot):
    """Обрабатывает немедленную рассылку"""
    try:
        user_id = callback_query.from_user.id

        # Убираем клавиатуру
        await callback_query.message.edit_reply_markup(reply_markup=None)

        user_data = await state_manager.get_data(user_id)
        notification = user_data.get('selected_notification')

        if not notification:
            await callback_query.answer("Уведомление не найдено", show_alert=True)
            return

        await callback_query.message.answer(
            f"Запускаю рассылку: {notification.get('Section', 'Без названия')}"
        )

        success = await send_broadcast_to_all_users(notification, bot)

        if success:
            await callback_query.message.answer("Рассылка завершена!")
        else:
            await callback_query.message.answer("Ошибка при рассылке")

        # Кнопка возврата в меню
        menu_keyboard = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="⬅️ В главное меню", callback_data="broadcast_back_to_menu")
        ]])

        await callback_query.message.answer("Что делаем дальше?", reply_markup=menu_keyboard)
        await callback_query.answer()

    except Exception as e:
        logger.error(f"Immediate broadcast error: {str(e)}")
        await callback_query.answer("Ошибка при запуске рассылки", show_alert=True)


@router.callback_query(F.data == "broadcast_confirm_scheduled")
async def handle_scheduled_broadcast(callback_query: CallbackQuery, bot: Bot):
    """Обрабатывает подтверждение отложенной рассылки"""
    try:
        user_id = callback_query.from_user.id

        # Убираем клавиатуру
        await callback_query.message.edit_reply_markup(reply_markup=None)

        user_data = await state_manager.get_data(user_id)
        display_schedule = user_data.get('display_schedule')
        schedule_datetime_str = user_data.get('selected_schedule_datetime')
        notification = user_data.get('selected_notification')

        if not all([display_schedule, schedule_datetime_str, notification]):
            await callback_query.answer("Данные рассылки не найдены", show_alert=True)
            return

        # Парсим дату и время
        schedule_datetime = datetime.fromisoformat(schedule_datetime_str)

        # Планируем рассылку
        broadcast_id = await schedule_broadcast(bot, notification, schedule_datetime, user_id)

        if broadcast_id:
            menu_keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="⬅️ В главное меню", callback_data="broadcast_back_to_menu")
            ]])

            await callback_query.message.answer(
                f"Ваше уведомление будет отправлено {display_schedule}.",
                reply_markup=menu_keyboard
            )
        else:
            await callback_query.message.answer(
                "   Не удалось запланировать рассылку. Попробуйте еще раз.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="⬅️ В главное меню", callback_data="broadcast_back_to_menu")
                ]])
            )

        await callback_query.answer()

    except Exception as e:
        logger.error(f"Scheduled broadcast error: {str(e)}")
        await callback_query.answer("Ошибка при планировании рассылки", show_alert=True)


@router.callback_query(F.data == "broadcast_cancel")
async def handle_broadcast_cancel(callback_query: CallbackQuery):
    """Обрабатывает отмену рассылки"""
    await callback_query.message.edit_reply_markup(reply_markup=None)

    # Создаем клавиатуру с кнопкой возврата в меню
    keyboard = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="⬅️ В главное меню",
            callback_data="broadcast_back_to_menu"
        )
    ]])

    await callback_query.message.answer(
        "Рассылка отменена",
        reply_markup=keyboard
    )
    await callback_query.answer()


async def send_test_notification_to_admin(admin_id: int, notification: Dict, bot: Bot):
    """Отправляет тестовое уведомление администратору для проверки"""
    try:
        # Подготавливаем контент
        content, file_data, filename = await prepare_notification_content(notification)

        # Отправляем файл (если есть)
        if file_data:
            await send_telegram_file(admin_id, file_data, filename, bot)

        # Отправляем контент
        await send_telegram_content(admin_id, content, bot)

        logger.info(f"Тестовое уведомление отправлено администратору {admin_id}")

    except Exception as e:
        logger.error(f"Ошибка отправки тестового уведомления администратору {admin_id}: {str(e)}")
        await bot.send_message(admin_id, "Ошибка при загрузке контента уведомления")


async def send_broadcast_to_all_users(notification: Dict, bot: Bot) -> bool:
    """Отправляет уведомление всем активным пользователям"""
    try:
        # Получаем активных пользователей
        active_users = await get_active_users()
        logger.info(f"Начинаю рассылку '{notification.get('Section')}' для {len(active_users)} пользователей")

        # Подготавливаем контент один раз для всех пользователей
        content, file_data, filename = await prepare_notification_content(notification)

        # Создаем клавиатуру с кнопкой (один раз для всех пользователей)
        keyboard = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="Ок 👍 вернуться в меню",
                callback_data="broadcast_back_to_menu"
            )
        ]])

        # Отправляем каждому пользователю
        success_count = 0
        for user in active_users:
            try:
                user_id = int(user['ID_messenger'])

                # Отправляем файл (если есть)
                if file_data:
                    await send_telegram_file(user_id, file_data, filename, bot)

                # Отправляем контент
                await send_telegram_content(user_id, content, bot, keyboard)

                logger.debug(f"Отправлено пользователю {user_id}")
                success_count += 1

            except Exception as e:
                logger.error(f"Ошибка отправки пользователю {user['ID_messenger']}: {str(e)}")

        logger.info(f"Рассылка завершена. Успешно: {success_count}/{len(active_users)}")
        return True

    except Exception as e:
        logger.error(f"Broadcast error: {str(e)}")
        return False


async def send_telegram_content(user_id: int, content: Dict, bot: Bot, keyboard: InlineKeyboardMarkup = None):
    """Отправляет контент пользователю в Telegram"""
    if content.get('image_url'):  # prepare_telegram_message создает image_url из content_image
        await bot.send_photo(
            chat_id=user_id,
            photo=content['image_url'],
            caption=content.get('text', ''),
            parse_mode="HTML",
            reply_markup=keyboard
        )
    elif content.get('text'):
        await bot.send_message(
            chat_id=user_id,
            text=content.get('text', ''),
            parse_mode="HTML",
            reply_markup=keyboard
        )


async def send_telegram_file(user_id: int, file_data: bytes, filename: str, bot: Bot):
    """Отправляет файл пользователю в Telegram"""
    file_to_send = BufferedInputFile(file_data, filename=filename)
    await bot.send_document(chat_id=user_id, document=file_to_send)


@router.callback_query(F.data == "broadcast_back_to_menu")
async def handle_broadcast_back_to_menu(callback_query: CallbackQuery):
    """Обрабатывает кнопку возврата в меню из рассылки"""
    try:
        # Запускаем навигацию (аналогично команде /start)
        await start_navigation(message=callback_query.message)

        await callback_query.answer()

    except Exception as e:
        logger.error(f"Ошибка обработки кнопки возврата в меню: {str(e)}")
        await callback_query.answer("Ошибка возврата в меню", show_alert=True)


async def schedule_broadcast(bot: Bot, notification: Dict, schedule_datetime: datetime, admin_id: int):
    """Планирует отложенную рассылку через asyncio таймер"""
    try:
        now = datetime.now()
        delay_seconds = (schedule_datetime - now).total_seconds()

        if delay_seconds <= 0:
            # Если время уже прошло, отправляем сразу
            await send_broadcast_to_all_users(notification, bot)
            await bot.send_message(admin_id, "Рассылка отправлена!")
            return

        # Создаем уникальный ID для рассылки
        broadcast_id = f"{admin_id}_{datetime.now().timestamp()}"

        logger.info(f"Планируем рассылку {broadcast_id} через {delay_seconds} секунд")

        # Запускаем таймер
        task = asyncio.create_task(
            delayed_broadcast(bot, notification, broadcast_id, admin_id, delay_seconds)
        )

        # Сохраняем информацию о запланированной рассылке
        scheduled_broadcasts[broadcast_id] = {
            'task': task,
            'notification_name': notification.get('Section', 'Без названия'),
            'scheduled_time': schedule_datetime,
            'admin_id': admin_id
        }

        return broadcast_id

    except Exception as e:
        logger.error(f"Ошибка планирования рассылки: {str(e)}")
        await bot.send_message(admin_id, "Ошибка при планировании рассылки")


async def delayed_broadcast(bot: Bot, notification: Dict, broadcast_id: str, admin_id: int, delay_seconds: int):
    """Выполняет отложенную рассылку"""
    try:
        logger.info(f"Таймер рассылки {broadcast_id} запущен, ждем {delay_seconds} секунд")

        # Ждем указанное время
        await asyncio.sleep(delay_seconds)

        # Отправляем рассылку
        success = await send_broadcast_to_all_users(notification, bot)

        # Уведомляем администратора
        if success:
            await bot.send_message(
                admin_id,
                f"Запланированная рассылка «{notification.get('Name', 'Без названия')}» отправлена!"
            )
        else:
            await bot.send_message(
                admin_id,
                f"Ошибка при отправке запланированной рассылки «{notification.get('Name', 'Без названия')}»"
            )

        # Удаляем из хранилища
        if broadcast_id in scheduled_broadcasts:
            del scheduled_broadcasts[broadcast_id]

        logger.info(f"Рассылка {broadcast_id} завершена")

    except Exception as e:
        logger.error(f"Ошибка в отложенной рассылке {broadcast_id}: {str(e)}")
        if admin_id:
            await bot.send_message(admin_id, f"Ошибка при выполнении запланированной рассылки: {str(e)}")


async def cancel_scheduled_broadcast(broadcast_id: str) -> bool:
    """Отменяет запланированную рассылку"""
    try:
        if broadcast_id in scheduled_broadcasts:
            # Отменяем задачу
            scheduled_broadcasts[broadcast_id]['task'].cancel()
            # Удаляем из хранилища
            del scheduled_broadcasts[broadcast_id]
            logger.info(f"Рассылка {broadcast_id} отменена")
            return True
        return False
    except Exception as e:
        logger.error(f"Ошибка отмены рассылки {broadcast_id}: {str(e)}")
        return False