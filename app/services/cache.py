import logging
from cachetools import TTLCache

from app.db.auth import check_id_messenger

logger = logging.getLogger(__name__)

# user_id -> role
auth_cache = TTLCache(maxsize=2000, ttl=3600)


async def get_user_access_and_role(user_id: int) -> tuple[bool, str | None]:
    """
    Возвращает (has_access, role)
    role == None если доступа нет
    """

    # 1. cache hit
    if user_id in auth_cache:
        role = auth_cache[user_id]
        logger.info("Auth cache hit: %s -> %s", user_id, role)
        return True, role

    # 2. cache miss → Seatable
    has_access, role = await check_id_messenger(str(user_id))

    if not has_access:
        logger.info("User %s has no access", user_id)
        return False, None

    # 3. cache save
    auth_cache[user_id] = role
    logger.info("Auth cached: %s -> %s", user_id, role)

    return True, role


def clear_user_auth(user_id: int):
    auth_cache.pop(user_id, None)