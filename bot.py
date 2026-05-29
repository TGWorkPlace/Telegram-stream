import asyncio
import os
import subprocess
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from pyrogram import Client, filters
from pyrogram.types import Message
from config import API_ID, API_HASH, BOT_TOKEN, RTMP_URL, STREAM_KEY, OWNER_ID, PORT

# ── Health check server for Koyeb ──────────────────────────────────────────────
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

current_stream: subprocess.Popen | None = None
stream_task: asyncio.Task | None = None

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
    global current_stream, stream_task
    if current_stream and current_stream.poll() is None:
        current_stream.terminate()
        if stream_task and not stream_task.done():
            stream_task.cancel()
        await message.reply("⏹️ Stream stopped.")
    else:
        await message.reply("⚪ No active stream to stop.")

# ── Background async watcher ───────────────────────────────────────────────────
async def _watch_stream(process: subprocess.Popen, message: Message, file_path: str):
    """Wait for ffmpeg to finish, then clean up the file."""
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, process.wait)
    if os.path.exists(file_path):
        os.remove(file_path)

# ── Video/Document handler ─────────────────────────────────────────────────────
@app.on_message(filters.private & (filters.video | filters.document))
async def handle_video(client: Client, message: Message):
    global current_stream, stream_task

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

    # Stream copy: no re-encoding — zero CPU lag.
    # Requires source to be H.264 video + AAC audio (standard for most MP4s).
    # If the stream fails immediately, the source codec may be incompatible;
    # swap -c:v copy / -c:a copy for libx264 / aac as a fallback.
    ffmpeg_cmd = [
        "ffmpeg",
        "-re",                    # Read at native frame rate (crucial for live RTMP)
        "-i", file_path,
        "-c:v", "copy",           # No video re-encode — eliminates CPU lag
        "-c:a", "copy",           # No audio re-encode
        "-f", "flv",
        f"{RTMP_URL}{STREAM_KEY}"
    ]

    try:
        current_stream = subprocess.Popen(
            ffmpeg_cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE   # Keep stderr so errors are readable if needed
        )
        await status.edit(
            "🔴 **Streaming live!**\n\n"
            "Use /stop to end the stream."
        )

        # Schedule async watcher — properly tied to the running event loop
        stream_task = asyncio.get_event_loop().create_task(
            _watch_stream(current_stream, message, file_path)
        )

    except Exception as e:
        await status.edit(f"❌ Stream failed:\n`{e}`")
        if os.path.exists(file_path):
            os.remove(file_path)

# ── Main entry ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    thread = threading.Thread(target=run_health_server, daemon=True)
    thread.start()
    print(f"✅ Health server running on port {PORT}")

    print("✅ Bot starting...")
    app.run()
