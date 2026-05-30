import motor.motor_asyncio
from config import MONGO_URI

# ─────────────────────────────────────────────────────────────
# MongoDB client
# ─────────────────────────────────────────────────────────────
_client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
_db = _client["streamer_bot"]
_col = _db["stream_targets"]


# ─────────────────────────────────────────────────────────────
# CRUD helpers
# ─────────────────────────────────────────────────────────────
async def save_target(name: str, link: str, key: str) -> bool:
    """Insert or replace a stream target by name. Returns True on success."""
    try:
        await _col.update_one(
            {"name": name},
            {"$set": {"name": name, "link": link, "key": key}},
            upsert=True,
        )
        return True
    except Exception as e:
        print(f"[DB] save_target error: {e}")
        return False


async def get_all_targets() -> list[dict]:
    """Return all saved targets as a list of dicts."""
    try:
        return await _col.find({}, {"_id": 0}).to_list(length=None)
    except Exception as e:
        print(f"[DB] get_all_targets error: {e}")
        return []


async def get_target(name: str) -> dict | None:
    """Return a single target by name, or None."""
    try:
        return await _col.find_one({"name": name}, {"_id": 0})
    except Exception as e:
        print(f"[DB] get_target error: {e}")
        return None


async def delete_target(name: str) -> bool:
    """Delete a target by name. Returns True if something was deleted."""
    try:
        result = await _col.delete_one({"name": name})
        return result.deleted_count > 0
    except Exception as e:
        print(f"[DB] delete_target error: {e}")
        return False
