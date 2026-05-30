# database.py
import motor.motor_asyncio
from config import MONGO_URI

# ─────────────────────────────────────────────────────────────
# All Motor objects are created lazily inside async functions so
# they are always bound to the *running* event loop (the one
# Pyrogram starts).  Never create them at import time or inside
# a temporary loop — Motor will reject operations on a closed loop.
# ─────────────────────────────────────────────────────────────

_client: motor.motor_asyncio.AsyncIOMotorClient | None = None
_db = None


async def _get_db():
    """Return the Motor database, creating the client if needed."""
    global _client, _db
    if _client is None:
        _client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
        _db = _client["streamer_bot"]
    return _db


async def _targets():
    db = await _get_db()
    return db["stream_targets"]


async def _admins():
    db = await _get_db()
    return db["admins"]


# ═══════════════════════════════════════════════════════════════
# Stream targets
# ═══════════════════════════════════════════════════════════════

async def save_target(name: str, link: str, key: str) -> tuple[bool, str]:
    """Insert or replace a stream target by name. Returns (success, error_msg)."""
    try:
        col = await _targets()
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
        col = await _targets()
        return await col.find({}, {"_id": 0}).to_list(length=None)
    except Exception as e:
        print(f"[DB] get_all_targets error: {e}")
        return []


async def get_target(name: str) -> dict | None:
    try:
        col = await _targets()
        return await col.find_one({"name": name}, {"_id": 0})
    except Exception as e:
        print(f"[DB] get_target error: {e}")
        return None


async def delete_target(name: str) -> bool:
    try:
        col = await _targets()
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
    Upserts so calling twice is safe.
    """
    try:
        col = await _admins()
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
        col = await _admins()
        result = await col.delete_one({"user_id": user_id})
        return result.deleted_count > 0
    except Exception as e:
        print(f"[DB] remove_admin error: {e}")
        return False


async def get_all_admins() -> list[int]:
    """Return a list of all persisted admin user_ids."""
    try:
        col = await _admins()
        docs = await col.find({}, {"_id": 0, "user_id": 1}).to_list(length=None)
        return [d["user_id"] for d in docs]
    except Exception as e:
        print(f"[DB] get_all_admins error: {e}")
        return []


async def is_admin_in_db(user_id: int) -> bool:
    """Return True if user_id exists in the admins collection."""
    try:
        col = await _admins()
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
        await _get_db()          # ensures _client is initialised
        await _client.admin.command("ping")
        return True, "MongoDB connection OK"
    except Exception as e:
        return False, str(e)
