import os

API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
MONGO_URI = os.environ.get("MONGO_URI", "")

RTMP_URL = os.environ.get("RTMP_URL", "")
STREAM_KEY = os.environ.get("STREAM_KEY", "")

OWNER_ID = int(os.environ.get("OWNER_ID", 0))

PORT = int(os.environ.get("PORT", 8080))
