import asyncio
import os
import subprocess
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from pyrogram import Client, filters
from pyrogram.types import Message
from config import API_ID, API_HASH, BOT_TOKEN, RTMP_URL, STREAM_KEY, OWNER_ID, PORT

## ── Health check server for Koyeb ──────────────────────────────────────────────
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, format, *args):
        pass  # Suppress access logs

def run_health_server():
    server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
    server.serve_forever()

# ── Bot setup ──────────────────────────────────────────────────────────────────
app = Client(
    "streamer_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

current_stream = None  # Track active ffmpeg process

# ── /start ─────────────────────────────────────────────────────────────────────
@app.on_message(filters.command("start"))
async def start(client: Client, message: Message):
    await message.reply(
        "👋 **Telegram Stream Bot**\n\n"
        "Send me a video file and I'll stream it live to your Telegram channel.\n\n"
        "**Commands:**\n"
        "• /start — Show this message\n"
        "• /status — Check stream status\n"
        "• /stop — Stop current stream\n\n"
        "📤 Just send or forward a video to begin!"
    )

# ── /status ────────────────────────────────────────────────────────────────────
@app.on_message(filters.command("status"))
async def status_cmd(client: Client, message: Message):
    global current_stream
    if current_stream and current_stream.poll() is None:
        await message.reply("🔴 **Stream is active.**")
    else:
        await message.reply("⚪ **No active stream.**")

# ── /stop ──────────────────────────────────────────────────────────────────────
@app.on_message(filters.command("stop"))
async def stop_stream(client: Client, message: Message):
    global current_stream
    if current_stream and current_stream.poll() is None:
        current_stream.terminate()
        await message.reply("⏹️ Stream stopped.")
    else:
        await message.reply("⚪ No active stream to stop.")

# ── Video/Document handler ─────────────────────────────────────────────────────
@app.on_message(filters.private & (filters.video | filters.document))
async def handle_video(client: Client, message: Message):
    global current_stream

    # Owner-only guard
    if OWNER_ID and message.from_user.id != OWNER_ID:
        await message.reply("🚫 You are not authorized to use this bot.")
        return

    # Block if already streaming
    if current_stream and current_stream.poll() is None:
        await message.reply(
            "⚠️ A stream is already running.\n"
            "Use /stop to end it first."
        )
        return

    status = await message.reply("⬇️ Downloading video...")

    try:
        file_path = await message.download(file_name="stream_input.mp4")
    except Exception as e:
        await status.edit(f"❌ Download failed:\n`{e}`")
        return

    await status.edit("📡 Preparing stream...")

    ffmpeg_cmd = [
        "ffmpeg", "-re",
        "-i", file_path,
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-b:v", "1500k",
        "-maxrate", "1500k",
        "-bufsize", "3000k",
        "-vf", "scale=1280:720",
        "-g", "50",
        "-c:a", "aac",
        "-b:a", "128k",
        "-f", "flv",
        f"{RTMP_URL}{STREAM_KEY}"
    ]

    try:
        current_stream = subprocess.Popen(
            ffmpeg_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        await status.edit(
            "🔴 **Streaming live!**\n\n"
            "Use /stop to end the stream."
        )

        # Watch ffmpeg in background and notify when done
        asyncio.get_event_loop().run_in_executor(
            None, _watch_stream, current_stream, message, file_path
        )

    except Exception as e:
        await status.edit(f"❌ Stream failed:\n`{e}`")

# ── Background watcher: cleans up file when stream ends ───────────────────────
def _watch_stream(process, message, file_path):
    process.wait()
    if os.path.exists(file_path):
        os.remove(file_path)

# ── Main entry ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Start health check server in background thread
    thread = threading.Thread(target=run_health_server, daemon=True)
    thread.start()
    print(f"✅ Health server running on port {PORT}")

    print("✅ Bot starting...")
    app.run()
