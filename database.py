# database.py
import motor.motor_asyncio
from config import MONGO_URI

_client = None
_col = None


def _get_col():
    global _client, _col
    if _col is None:
        _client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
        _db = _client["streamer_bot"]
        _col = _db["stream_targets"]
    return _col


async def save_target(name: str, link: str, key: str) -> tuple[bool, str]:
    """Insert or replace a stream target by name. Returns (success, error_msg)."""
    try:
        col = _get_col()
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
        col = _get_col()
        return await col.find({}, {"_id": 0}).to_list(length=None)
    except Exception as e:
        print(f"[DB] get_all_targets error: {e}")
        return []


async def get_target(name: str) -> dict | None:
    try:
        col = _get_col()
        return await col.find_one({"name": name}, {"_id": 0})
    except Exception as e:
        print(f"[DB] get_target error: {e}")
        return None


async def delete_target(name: str) -> bool:
    try:
        col = _get_col()
        result = await col.delete_one({"name": name})
        return result.deleted_count > 0
    except Exception as e:
        print(f"[DB] delete_target error: {e}")
        return False


async def ping_db() -> tuple[bool, str]:
    try:
        col = _get_col()
        await _client.admin.command("ping")
        return True, "MongoDB connection OK"
    except Exception as e:
        return False, str(e)
