import logging
import time
from typing import Dict

from app.services.utils import get_broadcast_admin_ids

# Антифлуд алертов: не чаще одного раза в это окно на каждый тип алерта.
ALERT_THROTTLE_SECONDS = 15 * 60  # 15 минут

# Время последней рассылки по типу алерта (in-memory, на процесс).
_last_alert_sent: Dict[str, float] = {}

logger = logging.getLogger(__name__)

async def send_alert_to_admins(bot, alert_type: str, message_text: str) -> None:
    """
    Разослать служебный алерт broadcast-админам с антифлудом.

    Антифлуд по типу алерта: если такой же тип рассылался менее
    ALERT_THROTTLE_SECONDS назад — пропускаем (чтобы при затяжном сбое
    провайдера не заваливать админов на каждый запрос).
    """
    now = time.monotonic()
    last = _last_alert_sent.get(alert_type)
    if last is not None and (now - last) < ALERT_THROTTLE_SECONDS:
        logger.info(
            f"Алерт '{alert_type}' подавлен антифлудом "
            f"(прошло {int(now - last)}с < {ALERT_THROTTLE_SECONDS}с)"
        )
        return

    admin_ids = await get_broadcast_admin_ids()
    if not admin_ids:
        logger.warning("Нет broadcast-админов для отправки алерта")
        return

    # Отмечаем время ДО рассылки — чтобы параллельные запросы не дублировали.
    _last_alert_sent[alert_type] = now

    text = f"⚠️ Уведомление от ИИ-помощника\n\n{message_text}"
    sent = 0
    for admin_id in admin_ids:
        try:
            await bot.send_message(chat_id=admin_id, text=text)
            sent += 1
        except Exception as exc:
            err = str(exc).lower()
            if "forbidden" in err or "chat not found" in err:
                logger.warning(f"Админ {admin_id} недоступен для алерта: {exc}")
            else:
                logger.error(f"Ошибка отправки алерта админу {admin_id}: {exc}")

    logger.info(f"Алерт '{alert_type}' разослан {sent}/{len(admin_ids)} админам")