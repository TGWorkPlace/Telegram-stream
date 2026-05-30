import asyncio
import os
import signal
import threading
import time
import uuid
from http.server import HTTPServer, BaseHTTPRequestHandler

from pyrogram import Client, filters
from pyrogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
)

from config import (
    API_ID,
    API_HASH,
    BOT_TOKEN,
    OWNER_ID,
    PORT,
)
from database import save_target, get_all_targets, get_target, delete_target

# ─────────────────────────────────────────────────────────────
# Optional uvloop
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

# Tracks users currently in /save flow
_save_pending: set[int] = set()

# Tracks a pending video waiting for channel selection {user_id: message}
_pending_video: dict[int, Message] = {}


# ─────────────────────────────────────────────────────────────
# Helpers: progress bar + formatting
# ─────────────────────────────────────────────────────────────
def make_bar(percent: float, width: int = 18) -> str:
    filled = int(width * percent / 100)
    return "[" + "●" * filled + "○" * (width - filled) + "]"


def fmt_size(b: float) -> str:
    if b >= 1024 ** 3:
        return f"{b / 1024 ** 3:.2f} GB"
    if b >= 1024 ** 2:
        return f"{b / 1024 ** 2:.2f} MB"
    if b >= 1024:
        return f"{b / 1024:.2f} KB"
    return f"{b:.2f} B"


def fmt_speed(bps: float) -> str:
    if bps >= 1024 ** 2:
        return f"{bps / 1024 ** 2:.2f} MB/s"
    if bps >= 1024:
        return f"{bps / 1024:.2f} KB/s"
    return f"{bps:.2f} B/s"


def fmt_time(s: float) -> str:
    s = max(0, int(s))
    m, s = divmod(s, 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"


# ─────────────────────────────────────────────────────────────
# Download progress callback factory
# ─────────────────────────────────────────────────────────────
def make_progress_callback(status_msg: Message, total_size: int):
    state = {"last_t": 0.0}
    start = time.time()

    async def progress(current: int, total: int):
        now = time.time()
        if now - state["last_t"] < 7 and current != total:
            return

        elapsed = now - start
        speed = current / elapsed if elapsed > 0 else 0
        pct = (current / total * 100) if total else 0
        eta = (total - current) / speed if speed > 0 else 0

        text = (
            f"📥 **Downloading**\n"
            f"{make_bar(pct)}\n"
            f"**Progress:** {pct:.1f}%\n"
            f"**Speed:** {fmt_speed(speed)}\n"
            f"**Processed:** {fmt_size(current)} / {fmt_size(total)}\n"
            f"**ETA:** {fmt_time(eta)}"
        )
        try:
            await status_msg.edit_text(text)
        except Exception:
            pass

        state["last_t"] = now

    return progress


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
# Get video duration via ffprobe
# ─────────────────────────────────────────────────────────────
async def get_duration(file_path: str) -> float:
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe",
            "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            file_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await proc.communicate()
        return float(stdout.decode().strip())
    except Exception:
        return 0.0


# ─────────────────────────────────────────────────────────────
# Stream watcher with live status updates
# ─────────────────────────────────────────────────────────────
async def watch_stream(
    process: asyncio.subprocess.Process,
    file_path: str,
    status_msg: Message,
    total_duration: float,
):
    global current_stream, stream_task, current_file

    stream_start = time.time()

    async def update_status():
        while True:
            await asyncio.sleep(7)
            if process.returncode is not None:
                break

            elapsed = time.time() - stream_start
            pct = min((elapsed / total_duration * 100) if total_duration > 0 else 0, 100.0)
            total_fmt = fmt_time(total_duration) if total_duration > 0 else "--:--"

            text = (
                f"📡 **Broadcasting....**\n"
                f"{make_bar(pct)}\n"
                f"⏱ {fmt_time(elapsed)} / {total_fmt} ({pct:.0f}%)"
            )
            try:
                await status_msg.edit_text(text)
            except Exception:
                pass

    updater = asyncio.create_task(update_status())

    try:
        rc = await process.wait()
        if rc != 0:
            print(f"⚠️ FFmpeg exited with code {rc}")
    except asyncio.CancelledError:
        updater.cancel()
        await kill_process(process)
        raise
    finally:
        updater.cancel()
        current_stream = None
        stream_task = None
        current_file = None
        try:
            await status_msg.edit_text("✅ **Stream ended.**")
        except Exception:
            pass
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"🗑 Deleted: {file_path}")
        except Exception as e:
            print(f"Cleanup error: {e}")


# ─────────────────────────────────────────────────────────────
# Internal: start streaming to a target
# ─────────────────────────────────────────────────────────────
async def start_stream(file_path: str, rtmp_url: str, stream_key: str, status_msg: Message):
    global current_stream, stream_task

    total_duration = await get_duration(file_path)

    ffmpeg_cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "error",
        "-re",
        "-fflags", "nobuffer",
        "-flags", "low_delay",
        "-i", file_path,
        "-c:v", "copy",
        "-c:a", "copy",
        "-muxdelay", "0",
        "-muxpreload", "0",
        "-flush_packets", "1",
        "-f", "flv",
        f"{rtmp_url}{stream_key}",
    ]

    current_stream = await asyncio.create_subprocess_exec(
        *ffmpeg_cmd,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )

    stream_task = asyncio.create_task(
        watch_stream(current_stream, file_path, status_msg, total_duration)
    )

    await status_msg.edit_text(
        "🔴 **Streaming live!**\n\n"
        "Use /stop to end stream."
    )


# ─────────────────────────────────────────────────────────────
# /start
# ─────────────────────────────────────────────────────────────
@app.on_message(filters.command("start") & filters.private)
async def start_cmd(client: Client, message: Message):
    await message.reply(
        "👋 **Telegram Stream Bot**\n\n"
        "Send a video and I will stream it live.\n\n"
        "**Commands:**\n"
        "• /start — Show help\n"
        "• /save — Save a stream target\n"
        "• /delete — Delete a stream target\n"
        "• /status — Stream status\n"
        "• /stop — Stop stream"
    )


# ─────────────────────────────────────────────────────────────
# /status
# ─────────────────────────────────────────────────────────────
@app.on_message(filters.command("status") & filters.private)
async def status_cmd(client: Client, message: Message):
    if current_stream and current_stream.returncode is None:
        await message.reply("🔴 **Stream is active.**")
    else:
        await message.reply("⚪ **No active stream.**")


# ─────────────────────────────────────────────────────────────
# /stop
# ─────────────────────────────────────────────────────────────
@app.on_message(filters.command("stop") & filters.private)
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
# /save  — ask user to provide link, key, name
# ─────────────────────────────────────────────────────────────
@app.on_message(filters.command("save") & filters.private)
async def save_cmd(client: Client, message: Message):
    if OWNER_ID and message.from_user.id != OWNER_ID:
        await message.reply("🚫 Unauthorized.")
        return

    _save_pending.add(message.from_user.id)
    await message.reply(
        "📋 **Send stream target in this format:**\n\n"
        "`{link}`\n"
        "`{key}`\n"
        "`{name}`\n\n"
        "Example:\n"
        "`rtmp://live.twitch.tv/app/`\n"
        "`live_abc123`\n"
        "`Twitch Main`"
    )


# ─────────────────────────────────────────────────────────────
# /delete — show inline buttons for each saved target
# ─────────────────────────────────────────────────────────────
@app.on_message(filters.command("delete") & filters.private)
async def delete_cmd(client: Client, message: Message):
    if OWNER_ID and message.from_user.id != OWNER_ID:
        await message.reply("🚫 Unauthorized.")
        return

    targets = await get_all_targets()
    if not targets:
        await message.reply("📭 No saved stream targets.")
        return

    buttons = [
        [InlineKeyboardButton(f"🗑 {t['name']}", callback_data=f"del:{t['name']}")]
        for t in targets
    ]
    buttons.append([InlineKeyboardButton("❌ Cancel", callback_data="del:__cancel__")])

    await message.reply(
        "🗑 **Select a target to delete:**",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


# ─────────────────────────────────────────────────────────────
# Callback: delete button pressed
# ─────────────────────────────────────────────────────────────
@app.on_callback_query(filters.regex(r"^del:"))
async def on_delete_cb(client: Client, cb: CallbackQuery):
    if OWNER_ID and cb.from_user.id != OWNER_ID:
        await cb.answer("🚫 Unauthorized.", show_alert=True)
        return

    name = cb.data[4:]  # strip "del:"

    if name == "__cancel__":
        await cb.message.edit_text("❌ Cancelled.")
        return

    deleted = await delete_target(name)
    if deleted:
        await cb.message.edit_text(f"✅ **Deleted:** `{name}`")
    else:
        await cb.message.edit_text(f"⚠️ Target `{name}` not found.")

    await cb.answer()


# ─────────────────────────────────────────────────────────────
# Callback: channel selection button pressed
# ─────────────────────────────────────────────────────────────
@app.on_callback_query(filters.regex(r"^stream:"))
async def on_stream_select_cb(client: Client, cb: CallbackQuery):
    if OWNER_ID and cb.from_user.id != OWNER_ID:
        await cb.answer("🚫 Unauthorized.", show_alert=True)
        return

    uid = cb.from_user.id
    name = cb.data[7:]  # strip "stream:"

    if name == "__cancel__":
        _pending_video.pop(uid, None)
        await cb.message.edit_text("❌ Cancelled.")
        await cb.answer()
        return

    video_msg = _pending_video.pop(uid, None)
    if not video_msg:
        await cb.message.edit_text("⚠️ Session expired. Please resend the video.")
        await cb.answer()
        return

    target = await get_target(name)
    if not target:
        await cb.message.edit_text(f"⚠️ Target `{name}` not found in DB.")
        await cb.answer()
        return

    # Prevent multiple streams
    if current_stream and current_stream.returncode is None:
        await cb.message.edit_text(
            "⚠️ Stream already running.\nUse /stop first."
        )
        await cb.answer()
        return

    # Get total file size
    if video_msg.video:
        total_size = video_msg.video.file_size or 0
    else:
        total_size = video_msg.document.file_size or 0

    await cb.message.edit_text(f"⬇️ Downloading for **{name}**...")
    status = cb.message

    filename = f"{uuid.uuid4()}.mp4"

    try:
        progress_cb = make_progress_callback(status, total_size)
        file_path = await video_msg.download(
            file_name=filename,
            progress=progress_cb,
        )
    except Exception as e:
        await status.edit_text(f"❌ Download failed:\n`{e}`")
        await cb.answer()
        return

    await status.edit_text(f"📡 Starting stream to **{name}**...")

    try:
        await start_stream(file_path, target["link"], target["key"], status)
    except Exception as e:
        await status.edit_text(f"❌ Stream failed:\n`{e}`")
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except Exception:
            pass

    await cb.answer()


# ─────────────────────────────────────────────────────────────
# Text handler — catches /save replies AND any plain text
# ─────────────────────────────────────────────────────────────
@app.on_message(filters.private & filters.text & ~filters.command(["start", "save", "delete", "status", "stop"]))
async def on_text(client: Client, message: Message):
    uid = message.from_user.id

    if uid not in _save_pending:
        return

    # Parse the 3-line format
    lines = [l.strip() for l in message.text.strip().splitlines() if l.strip()]
    if len(lines) != 3:
        await message.reply(
            "⚠️ Invalid format. Send exactly 3 lines:\n"
            "`{link}`\n`{key}`\n`{name}`"
        )
        return

    link, key, name = lines
    _save_pending.discard(uid)

    ok = await save_target(name=name, link=link, key=key)
    if ok:
        await message.reply(
            f"✅ **Saved!**\n\n"
            f"📛 Name: `{name}`\n"
            f"🔗 Link: `{link}`\n"
            f"🔑 Key: `{key}`"
        )
    else:
        await message.reply("❌ Failed to save. Check logs.")


# ─────────────────────────────────────────────────────────────
# Video handler — shows channel selection buttons
# ─────────────────────────────────────────────────────────────
@app.on_message(filters.private & (filters.video | filters.document))
async def handle_video(client: Client, message: Message):
    uid = message.from_user.id

    if OWNER_ID and uid != OWNER_ID:
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

    # Load targets from DB
    targets = await get_all_targets()
    if not targets:
        await message.reply(
            "📭 No stream targets saved.\n"
            "Use /save to add one first."
        )
        return

    # Stash the video message for after selection
    _pending_video[uid] = message

    buttons = [
        [InlineKeyboardButton(f"📺 {t['name']}", callback_data=f"stream:{t['name']}")]
        for t in targets
    ]
    buttons.append([InlineKeyboardButton("❌ Cancel", callback_data="stream:__cancel__")])

    await message.reply(
        "📡 **Select a stream target:**",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    thread = threading.Thread(target=run_health_server, daemon=True)
    thread.start()

    print(f"✅ Health server running on port {PORT}")
    print("✅ Bot starting...")

    app.run()
