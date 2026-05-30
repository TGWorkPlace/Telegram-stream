# database.py
import motor.motor_asyncio
from config import MONGO_URI

_client = None
_targets_col = None
_admins_col = None


def _get_targets_col():
    global _client, _targets_col
    if _targets_col is None:
        _client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
        _db = _client["streamer_bot"]
        _targets_col = _db["stream_targets"]
    return _targets_col


def _get_admins_col():
    global _client, _admins_col
    if _admins_col is None:
        # Re-use the same client if already created by _get_targets_col()
        if _client is None:
            _client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
        _db = _client["streamer_bot"]
        _admins_col = _db["admins"]
    return _admins_col


# ═══════════════════════════════════════════════════════════════
# Stream targets
# ═══════════════════════════════════════════════════════════════

async def save_target(name: str, link: str, key: str) -> tuple[bool, str]:
    """Insert or replace a stream target by name. Returns (success, error_msg)."""
    try:
        col = _get_targets_col()
        await col.update_one(
            {"name": name},
            {"$set": {"name": name, "link": link, "key": key}},
            upsert=True,
        )
        return True, ""
    except Exception as e:
        return False, str(e)


async def get_all_targets() -> list[dict]:
    try:
        col = _get_targets_col()
        return await col.find({}, {"_id": 0}).to_list(length=None)
    except Exception as e:
        print(f"[DB] get_all_targets error: {e}")
        return []


async def get_target(name: str) -> dict | None:
    try:
        col = _get_targets_col()
        return await col.find_one({"name": name}, {"_id": 0})
    except Exception as e:
        print(f"[DB] get_target error: {e}")
        return None


async def delete_target(name: str) -> bool:
    try:
        col = _get_targets_col()
        result = await col.delete_one({"name": name})
        return result.deleted_count > 0
    except Exception as e:
        print(f"[DB] delete_target error: {e}")
        return False


# ═══════════════════════════════════════════════════════════════
# Admins
# ═══════════════════════════════════════════════════════════════

async def add_admin(user_id: int) -> tuple[bool, str]:
    """
    Persist a new admin user_id.
    Returns (True, "") on success, (False, error) on failure.
    Silently succeeds (upsert) if the user is already an admin.
    """
    try:
        col = _get_admins_col()
        await col.update_one(
            {"user_id": user_id},
            {"$set": {"user_id": user_id}},
            upsert=True,
        )
        return True, ""
    except Exception as e:
        return False, str(e)


async def remove_admin(user_id: int) -> bool:
    """
    Delete an admin by user_id.
    Returns True if a document was deleted, False otherwise.
    """
    try:
        col = _get_admins_col()
        result = await col.delete_one({"user_id": user_id})
        return result.deleted_count > 0
    except Exception as e:
        print(f"[DB] remove_admin error: {e}")
        return False


async def get_all_admins() -> list[int]:
    """Return a list of all persisted admin user_ids."""
    try:
        col = _get_admins_col()
        docs = await col.find({}, {"_id": 0, "user_id": 1}).to_list(length=None)
        return [d["user_id"] for d in docs]
    except Exception as e:
        print(f"[DB] get_all_admins error: {e}")
        return []


async def is_admin_in_db(user_id: int) -> bool:
    """Return True if user_id exists in the admins collection."""
    try:
        col = _get_admins_col()
        doc = await col.find_one({"user_id": user_id}, {"_id": 1})
        return doc is not None
    except Exception as e:
        print(f"[DB] is_admin_in_db error: {e}")
        return False


# ═══════════════════════════════════════════════════════════════
# Health check
# ═══════════════════════════════════════════════════════════════

async def ping_db() -> tuple[bool, str]:
    try:
        _get_targets_col()          # ensures _client is initialised
        await _client.admin.command("ping")
        return True, "MongoDB connection OK"
    except Exception as e:
        return False, str(e)
