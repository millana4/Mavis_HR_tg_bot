import asyncio
import logging
import pprint
from datetime import datetime, time, timedelta, timezone, date
from typing import Dict, List, Optional, Tuple

from app.db.nocodb_client import NocoDBClient
from config import Config

logger = logging.getLogger(__name__)

_last_update_date: Optional[date] = None

sync_buffer_time = [time(7, 30), time(8, 00)]

BATCH_SIZE = 100


# ----------------------------
# TIMESTAMP FIX
# ----------------------------
def convert_timestamp(timestamp: int) -> datetime:
    seconds = timestamp / 1000
    dt = datetime.fromtimestamp(seconds, tz=timezone.utc)

    new_year = dt.year - 1969
    dt_corrected = dt.replace(year=new_year)

    return dt_corrected.replace(microsecond=0)


# ----------------------------
# KEY BUILDER
# ----------------------------
def build_key(record: Dict) -> Tuple:
    return (
        record.get('SNILS'),
        record.get('INN_org'),
        (record.get('Department') or "").strip(),
        (record.get('Position') or "").strip(),
        (record.get('Phone_private') or "").strip()
    )


# ----------------------------
# MAIN SYNC
# ----------------------------
async def sync_buffer_to_1c():
    logger.info("Начало синхронизации")

    global _last_update_date
    _last_update_date = None

    try:
        total_processed = 0

        async with NocoDBClient() as client:

            existed_records = await client.get_all(table_id=Config.DATA_1C_TABLE_ID)
            logger.info(f"Загружено {len(existed_records)} записей")

            # 🔥 INDEX для быстрого поиска
            if isinstance(existed_records, dict):
                existed_records = existed_records.get("list", [])

            index = {build_key(r): r for r in existed_records if isinstance(r, dict)}

            prev_ids = set()

            while True:
                buffer_records = await client.get_all(
                    table_id=Config.BUFFER_1C_TABLE_ID,
                    limit=BATCH_SIZE
                )

                if not buffer_records:
                    break

                current_ids = {r.get("Id") for r in buffer_records}

                # защита от бесконечного цикла
                if current_ids == prev_ids:
                    logger.error("Зацикливание: одни и те же записи в буфере")
                    break

                prev_ids = current_ids

                logger.info(f"Обработка {len(buffer_records)} записей")

                for buffer_record in buffer_records:
                    try:
                        existed_records, index = await _process_buffer_record(
                            client,
                            buffer_record,
                            existed_records,
                            index
                        )
                        total_processed += 1

                    except Exception as e:
                        logger.error(f"Ошибка {buffer_record.get('SNILS')}: {e}")

            await asyncio.sleep(10)
            await _cleanup_old_records(client)

        logger.info(f"Готово. Обработано {total_processed}")

    except Exception as e:
        logger.error(f"Фатальная ошибка: {e}")


# ----------------------------
# PROCESS RECORD
# ----------------------------
async def _process_buffer_record(
    client,
    buffer_record: Dict,
    existed_records: List[Dict],
    index: Dict
):

    global _last_update_date

    snils = buffer_record.get('SNILS')
    inn = buffer_record.get('INN_org')

    if not snils or not inn:
        return existed_records, index

    key = build_key(buffer_record)

    timestamp = buffer_record.get('Timestamp')
    if timestamp:
        dt = convert_timestamp(timestamp)
    else:
        dt = datetime.now(timezone.utc)

    _update_global_date(dt.date())

    record_data = {
        'SNILS': snils,
        'FIO': buffer_record.get('FIO'),
        'Previous_surname': buffer_record.get('Previous_surname'),
        'INN_org': inn,
        'Company': buffer_record.get('Company'),
        'Department': buffer_record.get('Department'),
        'Position': buffer_record.get('Position'),
        'Is_main': buffer_record.get('Is_main'),
        'Phone_private': buffer_record.get('Phone_private'),
        'Email_private': buffer_record.get('Email_private'),
        'Date_employment': buffer_record.get('Date_employment'),
        'Birthday': buffer_record.get('Birthday'),
        'Last_update': dt.isoformat()
    }

    existing = index.get(key)

    if existing:
        record_id = existing.get('Id')

        await client.update_record(
            table_id=Config.DATA_1C_TABLE_ID,
            record_id=record_id,
            data=record_data
        )

        logger.info(f"Обновление / создание записи для {snils}")

        # обновляем локально
        existing.update(record_data)

    else:
        created = await client.create_record(
            table_id=Config.DATA_1C_TABLE_ID,
            data=record_data
        )

        logger.info(f"Создание записи для {snils}")

        existed_records.append(created)
        if isinstance(created, list):
            created = created[0]
        index[key] = created

    await _delete_buffer_record(client, buffer_record.get('Id'))

    return existed_records, index


# ----------------------------
# DELETE BUFFER
# ----------------------------
async def _delete_buffer_record(client, record_id: int):
    try:
        await client.delete_record(
            table_id=Config.BUFFER_1C_TABLE_ID,
            record_id=record_id
        )
    except Exception as e:
        logger.error(f"Delete buffer error {record_id}: {e}")


# ----------------------------
# GLOBAL DATE TRACKER
# ----------------------------
def _update_global_date(new_date: date):
    global _last_update_date

    if _last_update_date is None or new_date > _last_update_date:
        _last_update_date = new_date


# ----------------------------
# CLEANUP
# ----------------------------
async def _cleanup_old_records(client):
    global _last_update_date

    if not _last_update_date:
        return

    logger.info(f"Удаление устаревших записей до {_last_update_date}")

    # Получаем все записи
    all_records = await client.get_all(
        table_id=Config.DATA_1C_TABLE_ID,
        fields=["Id", "Last_update"]
    )

    if not all_records:
        logger.info("Нет записей")
        return

    date_str = _last_update_date.strftime('%Y-%m-%d')

    deleted = 0
    for record in all_records:
        rid = record.get('Id')
        lu = record.get('Last_update')
        if not rid or not lu:
            continue

        try:
            # Извлекаем дату из строки (формат: "2026-03-25 04:00:01+00:00")
            record_date = lu.split(' ')[0] if ' ' in lu else lu.split('T')[0]

            if record_date < date_str:
                await client.delete_record(
                    table_id=Config.DATA_1C_TABLE_ID,
                    record_id=rid
                )
                deleted += 1
        except Exception as e:
            logger.error(f"Delete error {rid}: {e}")

    logger.info(f"Deleted: {deleted}")


# ----------------------------
# RUNNER
# ----------------------------
async def run_daily_buffer_sync():
    times = sync_buffer_time  # список времени

    logger.info(
        "Синхронизация выгрузки с буфером 1С будет запускаться в "
        + ", ".join(t.strftime("%H:%M") for t in times)
    )

    while True:
        now_utc = datetime.utcnow()
        now_msk = now_utc + timedelta(hours=3)

        # ищем ближайший запуск
        next_run = min(
            (
                datetime.combine(now_msk.date(), t)
                + (
                    timedelta(days=1)
                    if datetime.combine(now_msk.date(), t) <= now_msk
                    else timedelta()
                )
            )
            for t in times
        )

        sleep_seconds = (next_run - now_msk).total_seconds()

        logger.info(f"Следующий запуск синхронизации с буфером {next_run}")

        await asyncio.sleep(sleep_seconds)

        try:
            logger.info("Запуск sync_buffer_to_1c")
            await sync_buffer_to_1c()
        except Exception:
            logger.exception("Ошибка синхронизации с буфером 1С")

        # защита от двойного мгновенного запуска
        await asyncio.sleep(5)