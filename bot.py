# bot.py
import asyncio
import os
import re
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
    ADMINS,  # list[int] — admin user IDs (owner always has access too)
    PORT,
)
from database import save_target, get_all_targets, get_target, delete_target, ping_db

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except Exception:
    pass


# ─────────────────────────────────────────────────────────────
# Auth helper
# ─────────────────────────────────────────────────────────────
def is_authorized(user_id: int) -> bool:
    """Return True if the user is the owner or in the ADMINS list."""
    if OWNER_ID and user_id == OWNER_ID:
        return True
    return user_id in ADMINS


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
current_file: str | None = None   # None when streaming from URL

_save_pending: set[int] = set()
_pending_video: dict[int, Message] = {}
_pending_url: dict[int, str] = {}   # uid → direct stream URL


# ─────────────────────────────────────────────────────────────
# Helpers
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


def is_url(text: str) -> bool:
    return bool(re.match(r"^https?://", text.strip()))


# ─────────────────────────────────────────────────────────────
# Download progress callback
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
# Get video duration
# ─────────────────────────────────────────────────────────────
async def get_duration(source: str) -> float:
    """Works for both local file paths and HTTP/RTMP/HLS URLs."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe",
            "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            source,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await proc.communicate()
        return float(stdout.decode().strip())
    except Exception:
        return 0.0


# ─────────────────────────────────────────────────────────────
# Stream watcher
# ─────────────────────────────────────────────────────────────
async def watch_stream(
    process: asyncio.subprocess.Process,
    source: str,              # file path OR URL (used only for cleanup label)
    status_msg: Message,
    total_duration: float,
    is_file: bool = True,     # True → delete source after stream
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
        if is_file:
            try:
                if os.path.exists(source):
                    os.remove(source)
                    print(f"🗑 Deleted: {source}")
            except Exception as e:
                print(f"Cleanup error: {e}")


# ─────────────────────────────────────────────────────────────
# Build FFmpeg command
# ─────────────────────────────────────────────────────────────
def _build_ffmpeg_cmd(source: str, rtmp_url: str, stream_key: str, from_url: bool) -> list[str]:
    """
    For URL sources we pass the input directly — FFmpeg reads over the
    network so the server never stores or transcodes the file.
    For both modes we copy streams (no re-encoding) to keep CPU near zero.
    """
    base = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "error",
    ]

    if from_url:
        # Let FFmpeg handle reconnect for live/HLS streams
        base += [
            "-reconnect", "1",
            "-reconnect_streamed", "1",
            "-reconnect_delay_max", "5",
        ]

    base += [
        "-re",
        "-fflags", "nobuffer",
        "-flags", "low_delay",
        "-i", source,
        "-c:v", "copy",
        "-c:a", "copy",
        "-muxdelay", "0",
        "-muxpreload", "0",
        "-flush_packets", "1",
        "-f", "flv",
        f"{rtmp_url}{stream_key}",
    ]
    return base


# ─────────────────────────────────────────────────────────────
# Start stream
# ─────────────────────────────────────────────────────────────
async def start_stream(
    source: str,
    rtmp_url: str,
    stream_key: str,
    status_msg: Message,
    from_url: bool = False,
):
    global current_stream, stream_task, current_file

    total_duration = await get_duration(source)

    ffmpeg_cmd = _build_ffmpeg_cmd(source, rtmp_url, stream_key, from_url)

    current_stream = await asyncio.create_subprocess_exec(
        *ffmpeg_cmd,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )

    current_file = None if from_url else source

    stream_task = asyncio.create_task(
        watch_stream(
            current_stream,
            source,
            status_msg,
            total_duration,
            is_file=not from_url,
        )
    )

    label = "🌐 URL" if from_url else "📁 File"
    await status_msg.edit_text(
        f"🔴 **Streaming live!** ({label})\n\n"
        "Use /stop to end stream."
    )


# ─────────────────────────────────────────────────────────────
# /start
# ─────────────────────────────────────────────────────────────
@app.on_message(filters.command("start") & filters.private)
async def start_cmd(client: Client, message: Message):
    await message.reply(
        "👋 **Telegram Stream Bot**\n\n"
        "Send a **video file** or use **/stream\\_url** to stream directly from a URL "
        "(no download — saves server CPU).\n\n"
        "**Commands:**\n"
        "• /start — Show help\n"
        "• /stream_url — Stream from a direct URL\n"
        "• /save — Save a stream target\n"
        "• /delete — Delete a stream target\n"
        "• /status — Stream status\n"
        "• /stop — Stop stream\n"
        "• /ping_db — Test DB connection"
    )


# ─────────────────────────────────────────────────────────────
# /ping_db
# ─────────────────────────────────────────────────────────────
@app.on_message(filters.command("ping_db") & filters.private)
async def ping_db_cmd(client: Client, message: Message):
    if not is_authorized(message.from_user.id):
        await message.reply("🚫 Unauthorized.")
        return

    msg = await message.reply("🔄 Pinging MongoDB...")
    ok, info = await ping_db()
    if ok:
        await msg.edit_text(f"✅ **DB OK**\n`{info}`")
    else:
        await msg.edit_text(f"❌ **DB Error:**\n`{info}`")


# ─────────────────────────────────────────────────────────────
# /status
# ─────────────────────────────────────────────────────────────
@app.on_message(filters.command("status") & filters.private)
async def status_cmd(client: Client, message: Message):
    if current_stream and current_stream.returncode is None:
        source_label = "🌐 URL stream" if current_file is None else f"📁 `{os.path.basename(current_file)}`"
        await message.reply(f"🔴 **Stream is active.**\nSource: {source_label}")
    else:
        await message.reply("⚪ **No active stream.**")


# ─────────────────────────────────────────────────────────────
# /stop
# ─────────────────────────────────────────────────────────────
@app.on_message(filters.command("stop") & filters.private)
async def stop_stream(client: Client, message: Message):
    global current_stream, stream_task

    proc = current_stream
    task = stream_task

    if not proc:
        await message.reply("⚪ No active stream.")
        return

    try:
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        await kill_process(proc)

        current_stream = None
        stream_task = None

        await message.reply("⏹️ **Stream stopped.**")

    except Exception as e:
        await message.reply(f"❌ Stop failed:\n`{e}`")


# ─────────────────────────────────────────────────────────────
# /stream_url — stream directly from a URL
# ─────────────────────────────────────────────────────────────
@app.on_message(filters.command("stream_url") & filters.private)
async def stream_url_cmd(client: Client, message: Message):
    if not is_authorized(message.from_user.id):
        await message.reply("🚫 Unauthorized.")
        return

    uid = message.from_user.id

    # Allow inline usage: /stream_url https://example.com/live.m3u8
    parts = message.text.split(maxsplit=1)
    if len(parts) == 2 and is_url(parts[1]):
        _pending_url[uid] = parts[1].strip()
    else:
        _pending_url[uid] = ""   # mark as expecting URL next message
        await message.reply(
            "🌐 **Direct URL Streaming**\n\n"
            "Send the **direct media URL** to stream.\n"
            "Supports HTTP/HTTPS (MP4, MKV, TS), HLS (`.m3u8`), DASH, RTMP, etc.\n\n"
            "No file is downloaded — FFmpeg reads it over the network, "
            "keeping server CPU and disk usage near zero."
        )
        return

    await _show_target_picker_for_url(message, uid)


async def _show_target_picker_for_url(message: Message, uid: int):
    targets = await get_all_targets()
    if not targets:
        _pending_url.pop(uid, None)
        await message.reply(
            "📭 No stream targets saved.\nUse /save to add one first."
        )
        return

    buttons = [
        [InlineKeyboardButton(f"📺 {t['name']}", callback_data=f"urlstream:{t['name']}")]
        for t in targets
    ]
    buttons.append([InlineKeyboardButton("❌ Cancel", callback_data="urlstream:__cancel__")])

    await message.reply(
        f"🌐 **URL queued.** Select stream target:",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


# ─────────────────────────────────────────────────────────────
# /save
# ─────────────────────────────────────────────────────────────
@app.on_message(filters.command("save") & filters.private)
async def save_cmd(client: Client, message: Message):
    if not is_authorized(message.from_user.id):
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
# /delete
# ─────────────────────────────────────────────────────────────
@app.on_message(filters.command("delete") & filters.private)
async def delete_cmd(client: Client, message: Message):
    if not is_authorized(message.from_user.id):
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
# Callback: delete
# ─────────────────────────────────────────────────────────────
@app.on_callback_query(filters.regex(r"^del:"))
async def on_delete_cb(client: Client, cb: CallbackQuery):
    await cb.answer()

    if not is_authorized(cb.from_user.id):
        await cb.message.edit_text("🚫 Unauthorized.")
        return

    name = cb.data[4:]

    if name == "__cancel__":
        await cb.message.edit_text("❌ Cancelled.")
        return

    deleted = await delete_target(name)
    if deleted:
        await cb.message.edit_text(f"✅ **Deleted:** `{name}`")
    else:
        await cb.message.edit_text(f"⚠️ Target `{name}` not found.")


# ─────────────────────────────────────────────────────────────
# Callback: URL stream target selection
# ─────────────────────────────────────────────────────────────
@app.on_callback_query(filters.regex(r"^urlstream:"))
async def on_urlstream_select_cb(client: Client, cb: CallbackQuery):
    await cb.answer()

    if not is_authorized(cb.from_user.id):
        await cb.message.edit_text("🚫 Unauthorized.")
        return

    uid = cb.from_user.id
    name = cb.data[len("urlstream:"):]

    if name == "__cancel__":
        _pending_url.pop(uid, None)
        await cb.message.edit_text("❌ Cancelled.")
        return

    url = _pending_url.pop(uid, None)
    if not url:
        await cb.message.edit_text("⚠️ Session expired. Please use /stream_url again.")
        return

    target = await get_target(name)
    if not target:
        await cb.message.edit_text(f"⚠️ Target `{name}` not found in DB.")
        return

    if current_stream and current_stream.returncode is None:
        await cb.message.edit_text("⚠️ Stream already running.\nUse /stop first.")
        return

    await cb.message.edit_text(
        f"🌐 **Starting URL stream to {name}...**\n\n"
        f"`{url}`"
    )

    try:
        await start_stream(url, target["link"], target["key"], cb.message, from_url=True)
    except Exception as e:
        await cb.message.edit_text(f"❌ Stream failed:\n`{e}`")


# ─────────────────────────────────────────────────────────────
# Callback: channel selection (file stream)
# ─────────────────────────────────────────────────────────────
@app.on_callback_query(filters.regex(r"^stream:"))
async def on_stream_select_cb(client: Client, cb: CallbackQuery):
    await cb.answer()

    if not is_authorized(cb.from_user.id):
        await cb.message.edit_text("🚫 Unauthorized.")
        return

    uid = cb.from_user.id
    name = cb.data[7:]

    if name == "__cancel__":
        _pending_video.pop(uid, None)
        await cb.message.edit_text("❌ Cancelled.")
        return

    video_msg = _pending_video.pop(uid, None)
    if not video_msg:
        await cb.message.edit_text("⚠️ Session expired. Please resend the video.")
        return

    target = await get_target(name)
    if not target:
        await cb.message.edit_text(f"⚠️ Target `{name}` not found in DB.")
        return

    if current_stream and current_stream.returncode is None:
        await cb.message.edit_text(
            "⚠️ Stream already running.\nUse /stop first."
        )
        return

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
        return

    await status.edit_text(f"📡 Starting stream to **{name}**...")

    try:
        await start_stream(file_path, target["link"], target["key"], status, from_url=False)
    except Exception as e:
        await status.edit_text(f"❌ Stream failed:\n`{e}`")
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────
# Text handler — /save replies + /stream_url URL collection
# ─────────────────────────────────────────────────────────────
@app.on_message(
    filters.private
    & filters.text
    & ~filters.command(["start", "save", "delete", "status", "stop", "ping_db", "stream_url"])
)
async def on_text(client: Client, message: Message):
    uid = message.from_user.id
    text = message.text.strip()

    # ── Collect URL for /stream_url flow ──────────────────────
    if uid in _pending_url and _pending_url[uid] == "":
        if not is_url(text):
            await message.reply(
                "⚠️ That doesn't look like a valid URL.\n"
                "Please send a URL starting with `http://` or `https://`."
            )
            return
        _pending_url[uid] = text
        await _show_target_picker_for_url(message, uid)
        return

    # ── Collect target info for /save flow ───────────────────
    if uid not in _save_pending:
        return

    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if len(lines) != 3:
        await message.reply(
            "⚠️ Invalid format. Send exactly 3 lines:\n"
            "`{link}`\n`{key}`\n`{name}`"
        )
        return

    link, key, name = lines
    _save_pending.discard(uid)

    ok, err = await save_target(name=name, link=link, key=key)
    if ok:
        await message.reply(
            f"✅ **Saved!**\n\n"
            f"📛 Name: `{name}`\n"
            f"🔗 Link: `{link}`\n"
            f"🔑 Key: `{key}`"
        )
    else:
        await message.reply(
            f"❌ **Failed to save.**\n\n"
            f"**Error:** `{err}`\n\n"
            f"Run /ping\\_db to test your MongoDB connection."
        )


# ─────────────────────────────────────────────────────────────
# Video handler
# ─────────────────────────────────────────────────────────────
@app.on_message(filters.private & (filters.video | filters.document))
async def handle_video(client: Client, message: Message):
    uid = message.from_user.id

    if not is_authorized(uid):
        await message.reply("🚫 Unauthorized.")
        return

    is_video = bool(message.video)
    is_video_doc = (
        message.document
        and message.document.mime_type
        and message.document.mime_type.startswith("video/")
    )
    if not (is_video or is_video_doc):
        await message.reply("❌ Please send a video file.")
        return

    targets = await get_all_targets()
    if not targets:
        await message.reply(
            "📭 No stream targets saved.\n"
            "Use /save to add one first."
        )
        return

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
