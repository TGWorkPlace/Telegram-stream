import asyncio
import os
import signal
import threading
import uuid
from http.server import HTTPServer, BaseHTTPRequestHandler

from pyrogram import Client, filters
from pyrogram.types import Message

from config import (
    API_ID,
    API_HASH,
    BOT_TOKEN,
    RTMP_URL,
    STREAM_KEY,
    OWNER_ID,
    PORT,
)

# ─────────────────────────────────────────────────────────────
# Optional uvloop (better asyncio performance on Linux/Koyeb)
# ─────────────────────────────────────────────────────────────
try:
    import uvloop

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except Exception:
    pass


# ─────────────────────────────────────────────────────────────
# Health server for Koyeb
# ─────────────────────────────────────────────────────────────
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, format, *args):
        pass


def run_health_server():
    server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
    server.serve_forever()


# ─────────────────────────────────────────────────────────────
# Bot setup
# ─────────────────────────────────────────────────────────────
app = Client(
    "streamer_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
)

current_stream: asyncio.subprocess.Process | None = None
stream_task: asyncio.Task | None = None
current_file: str | None = None


# ─────────────────────────────────────────────────────────────
# Kill helper
# ─────────────────────────────────────────────────────────────
async def kill_process(process: asyncio.subprocess.Process):
    if process.returncode is not None:
        return

    try:
        process.send_signal(signal.SIGTERM)

        try:
            await asyncio.wait_for(process.wait(), timeout=2)
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()

    except Exception:
        pass


# ─────────────────────────────────────────────────────────────
# Stream watcher
# ─────────────────────────────────────────────────────────────
async def watch_stream(
    process: asyncio.subprocess.Process,
    file_path: str,
):
    global current_stream, stream_task, current_file

    try:
        return_code = await process.wait()

        if return_code != 0:
            print(f"⚠️ FFmpeg exited with code {return_code}")

    except asyncio.CancelledError:
        await kill_process(process)
        raise

    finally:
        current_stream = None
        stream_task = None
        current_file = None

        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"🗑 Deleted: {file_path}")
        except Exception as e:
            print(f"Cleanup error: {e}")


# ─────────────────────────────────────────────────────────────
# /start
# ─────────────────────────────────────────────────────────────
@app.on_message(filters.command("start"))
async def start_cmd(client: Client, message: Message):
    await message.reply(
        "👋 **Telegram Stream Bot**\n\n"
        "Send a video and I will stream it live.\n\n"
        "**Commands:**\n"
        "• /start — Show help\n"
        "• /status — Stream status\n"
        "• /stop — Stop stream"
    )


# ─────────────────────────────────────────────────────────────
# /status
# ─────────────────────────────────────────────────────────────
@app.on_message(filters.command("status"))
async def status_cmd(client: Client, message: Message):
    global current_stream

    if current_stream and current_stream.returncode is None:
        await message.reply("🔴 **Stream is active.**")
    else:
        await message.reply("⚪ **No active stream.**")


# ─────────────────────────────────────────────────────────────
# /stop
# ─────────────────────────────────────────────────────────────
@app.on_message(filters.command("stop"))
async def stop_stream(client: Client, message: Message):
    global current_stream, stream_task

    if not current_stream:
        await message.reply("⚪ No active stream.")
        return

    try:
        if stream_task and not stream_task.done():
            stream_task.cancel()

            try:
                await stream_task
            except asyncio.CancelledError:
                pass

        await kill_process(current_stream)

        current_stream = None
        stream_task = None

        await message.reply("⏹️ **Stream stopped.**")

    except Exception as e:
        await message.reply(f"❌ Stop failed:\n`{e}`")


# ─────────────────────────────────────────────────────────────
# Video handler
# ─────────────────────────────────────────────────────────────
@app.on_message(filters.private & (filters.video | filters.document))
async def handle_video(client: Client, message: Message):
    global current_stream, stream_task, current_file

    # Owner restriction
    if OWNER_ID and message.from_user.id != OWNER_ID:
        await message.reply("🚫 Unauthorized.")
        return

    # Validate file type
    is_video = bool(message.video)
    is_video_doc = (
        message.document
        and message.document.mime_type
        and message.document.mime_type.startswith("video/")
    )

    if not (is_video or is_video_doc):
        await message.reply("❌ Please send a video file.")
        return

    # Prevent multiple streams
    if current_stream and current_stream.returncode is None:
        await message.reply(
            "⚠️ Stream already running.\n"
            "Use /stop first."
        )
        return

    status = await message.reply("⬇️ Downloading video...")

    # Unique filename
    filename = f"{uuid.uuid4()}.mp4"

    try:
        file_path = await message.download(
            file_name=filename
        )

        current_file = file_path

    except Exception as e:
        await status.edit_text(
            f"❌ Download failed:\n`{e}`"
        )
        return

    await status.edit_text("📡 Starting stream...")

    # Optimized FFmpeg for low latency + low CPU
    ffmpeg_cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",

        "-re",

        "-fflags",
        "nobuffer",

        "-flags",
        "low_delay",

        "-i",
        file_path,

        "-c:v",
        "copy",

        "-c:a",
        "copy",

        "-muxdelay",
        "0",

        "-muxpreload",
        "0",

        "-flush_packets",
        "1",

        "-f",
        "flv",

        f"{RTMP_URL}{STREAM_KEY}",
    ]

    try:
        current_stream = await asyncio.create_subprocess_exec(
            *ffmpeg_cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )

        stream_task = asyncio.create_task(
            watch_stream(current_stream, file_path)
        )

        await status.edit_text(
            "🔴 **Streaming live!**\n\n"
            "Use /stop to end stream."
        )

    except Exception as e:
        await status.edit_text(
            f"❌ Stream failed:\n`{e}`"
        )

        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    thread = threading.Thread(
        target=run_health_server,
        daemon=True,
    )
    thread.start()

    print(f"✅ Health server running on port {PORT}")
    print("✅ Bot starting...")

    app.run()
