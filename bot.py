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
    PORT,
)
from database import (
    save_target, get_all_targets, get_target, delete_target, ping_db,
    add_admin, remove_admin, get_all_admins, is_admin_in_db,
)

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except Exception:
    pass


# ─────────────────────────────────────────────────────────────
# ADMINS — in-memory cache, seeded from DB on startup.
# Writes go to both the cache and DB so they survive restarts.
# OWNER_ID always has full access regardless of this set.
# ─────────────────────────────────────────────────────────────
ADMINS: set[int] = set()


async def load_admins_from_db() -> None:
    """Populate the in-memory ADMINS cache from MongoDB."""
    ids = await get_all_admins()
    ADMINS.update(ids)
    print(f"✅ Loaded {len(ids)} admin(s) from DB: {ids}")


def is_authorized(user_id: int) -> bool:
    """Return True if user is the owner or a persisted admin."""
    if OWNER_ID and user_id == OWNER_ID:
        return True
    return user_id in ADMINS


# ─────────────────────────────────────────────────────────────
# URL detector
# ─────────────────────────────────────────────────────────────
URL_RE = re.compile(
    r"^(https?|rtmp|rtmps|rtsp|mms|srt)://\S+",
    re.IGNORECASE,
)


def is_url(text: str) -> bool:
    return bool(URL_RE.match(text.strip()))


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
current_file: str | None = None          # None when streaming from URL

_save_pending: set[int] = set()
_pending_video: dict[int, Message] = {}
_pending_url: dict[int, str] = {}        # uid -> direct URL to stream


# ─────────────────────────────────────────────────────────────
# Startup hook — load admins before the bot starts handling msgs
# ─────────────────────────────────────────────────────────────
@app.on_message(filters.command("start") & filters.private, group=-999)
async def _noop(_c, _m):
    pass  # dummy; real startup is done via asyncio task below


async def _startup():
    await load_admins_from_db()


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
# Get video duration  (works for both local files and URLs)
# ─────────────────────────────────────────────────────────────
async def get_duration(source: str) -> float:
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
    file_path: str | None,          # None when source is a URL
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
        # Only delete if it was a local file (not URL streaming)
        if file_path:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    print(f"🗑 Deleted: {file_path}")
            except Exception as e:
                print(f"Cleanup error: {e}")


# ─────────────────────────────────────────────────────────────
# Build FFmpeg command
# ─────────────────────────────────────────────────────────────
def build_ffmpeg_cmd(
    source: str,
    rtmp_url: str,
    stream_key: str,
    is_url_source: bool,
) -> list[str]:
    cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error"]

    if is_url_source:
        cmd += [
            "-reconnect", "1",
            "-reconnect_streamed", "1",
            "-reconnect_delay_max", "5",
        ]

    cmd += [
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
    return cmd


# ─────────────────────────────────────────────────────────────
# Start stream  (unified for file and URL)
# ─────────────────────────────────────────────────────────────
async def start_stream(
    source: str,
    rtmp_url: str,
    stream_key: str,
    status_msg: Message,
    is_url_source: bool = False,
):
    global current_stream, stream_task, current_file

    total_duration = await get_duration(source)
    ffmpeg_cmd = build_ffmpeg_cmd(source, rtmp_url, stream_key, is_url_source)

    current_stream = await asyncio.create_subprocess_exec(
        *ffmpeg_cmd,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )

    file_to_clean = None if is_url_source else source
    current_file = file_to_clean

    stream_task = asyncio.create_task(
        watch_stream(current_stream, file_to_clean, status_msg, total_duration)
    )

    source_label = "🌐 URL" if is_url_source else "📁 File"
    await status_msg.edit_text(
        f"🔴 **Streaming live!** ({source_label})\n\n"
        "Use /stop to end stream."
    )


# ─────────────────────────────────────────────────────────────
# /start
# ─────────────────────────────────────────────────────────────
@app.on_message(filters.command("start") & filters.private)
async def start_cmd(client: Client, message: Message):
    await message.reply(
        "👋 **Telegram Stream Bot**\n\n"
        "Send a **video file** or a **direct URL** and I will stream it live.\n\n"
        "**Commands:**\n"
        "• /start — Show help\n"
        "• /save — Save a stream target\n"
        "• /delete — Delete a stream target\n"
        "• /status — Stream status\n"
        "• /stop — Stop stream\n"
        "• /addadmin `<user_id>` — Grant admin access _(owner only)_\n"
        "• /removeadmin `<user_id>` — Revoke admin access _(owner only)_\n"
        "• /admins — List all admins _(owner only)_\n"
        "• /ping\\_db — Test DB connection\n\n"
        "ℹ️ URL streaming uses **zero transcoding CPU** — forwarded directly."
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
        source_type = "🌐 URL" if current_file is None else "📁 File"
        await message.reply(f"🔴 **Stream is active.** ({source_type})")
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
# /addadmin  — owner only
# Saves to DB and updates the in-memory cache.
# ─────────────────────────────────────────────────────────────
@app.on_message(filters.command("addadmin") & filters.private)
async def addadmin_cmd(client: Client, message: Message):
    if not (OWNER_ID and message.from_user.id == OWNER_ID):
        await message.reply("🚫 Only the owner can manage admins.")
        return

    parts = message.text.split()
    if len(parts) != 2 or not parts[1].lstrip("-").isdigit():
        await message.reply("⚠️ Usage: `/addadmin <user_id>`")
        return

    uid = int(parts[1])

    if OWNER_ID and uid == OWNER_ID:
        await message.reply("ℹ️ Owner already has full access.")
        return

    if uid in ADMINS:
        await message.reply(f"ℹ️ `{uid}` is already an admin.")
        return

    ok, err = await add_admin(uid)
    if ok:
        ADMINS.add(uid)
        await message.reply(
            f"✅ **Admin added:** `{uid}`\n\n"
            "They now have full bot permissions and will be remembered after restarts."
        )
    else:
        await message.reply(f"❌ **Failed to save admin to DB:**\n`{err}`")


# ─────────────────────────────────────────────────────────────
# /removeadmin  — owner only
# Removes from DB and updates the in-memory cache.
# ─────────────────────────────────────────────────────────────
@app.on_message(filters.command("removeadmin") & filters.private)
async def removeadmin_cmd(client: Client, message: Message):
    if not (OWNER_ID and message.from_user.id == OWNER_ID):
        await message.reply("🚫 Only the owner can manage admins.")
        return

    parts = message.text.split()
    if len(parts) != 2 or not parts[1].lstrip("-").isdigit():
        await message.reply("⚠️ Usage: `/removeadmin <user_id>`")
        return

    uid = int(parts[1])

    if uid not in ADMINS:
        await message.reply(f"⚠️ `{uid}` is not an admin.")
        return

    deleted = await remove_admin(uid)
    if deleted:
        ADMINS.discard(uid)
        await message.reply(f"✅ **Admin removed:** `{uid}`")
    else:
        # Shouldn't normally happen — in-memory had it but DB didn't.
        # Still remove from cache for consistency.
        ADMINS.discard(uid)
        await message.reply(
            f"⚠️ `{uid}` removed from memory but was not found in DB.\n"
            "The cache has been cleaned up."
        )


# ─────────────────────────────────────────────────────────────
# /admins  — owner only
# Always reads fresh from DB to reflect actual persisted state.
# ─────────────────────────────────────────────────────────────
@app.on_message(filters.command("admins") & filters.private)
async def admins_cmd(client: Client, message: Message):
    if not (OWNER_ID and message.from_user.id == OWNER_ID):
        await message.reply("🚫 Only the owner can view the admin list.")
        return

    ids = await get_all_admins()

    # Keep in-memory cache in sync with whatever DB actually has
    ADMINS.clear()
    ADMINS.update(ids)

    if not ids:
        await message.reply(
            "📭 No admins saved in DB yet.\n"
            "Use `/addadmin <user_id>` to add one."
        )
        return

    lines = "\n".join(f"• `{uid}`" for uid in sorted(ids))
    await message.reply(f"👥 **Admins (from DB):**\n\n{lines}")


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
# Callback: delete target
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
# Shared helper: show target selection keyboard
# ─────────────────────────────────────────────────────────────
async def show_target_keyboard(message: Message, prefix: str, intro: str):
    targets = await get_all_targets()
    if not targets:
        await message.reply(
            "📭 No stream targets saved.\n"
            "Use /save to add one first."
        )
        return False

    buttons = [
        [InlineKeyboardButton(f"📺 {t['name']}", callback_data=f"{prefix}:{t['name']}")]
        for t in targets
    ]
    buttons.append([InlineKeyboardButton("❌ Cancel", callback_data=f"{prefix}:__cancel__")])

    await message.reply(intro, reply_markup=InlineKeyboardMarkup(buttons))
    return True


# ─────────────────────────────────────────────────────────────
# Callback: channel selection for FILE streaming
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
        await cb.message.edit_text("⚠️ Stream already running.\nUse /stop first.")
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
        await start_stream(file_path, target["link"], target["key"], status, is_url_source=False)
    except Exception as e:
        await status.edit_text(f"❌ Stream failed:\n`{e}`")
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────
# Callback: channel selection for URL streaming
# ─────────────────────────────────────────────────────────────
@app.on_callback_query(filters.regex(r"^urlstream:"))
async def on_url_stream_select_cb(client: Client, cb: CallbackQuery):
    await cb.answer()

    if not is_authorized(cb.from_user.id):
        await cb.message.edit_text("🚫 Unauthorized.")
        return

    uid = cb.from_user.id
    name = cb.data[10:]  # strip "urlstream:"

    if name == "__cancel__":
        _pending_url.pop(uid, None)
        await cb.message.edit_text("❌ Cancelled.")
        return

    source_url = _pending_url.pop(uid, None)
    if not source_url:
        await cb.message.edit_text("⚠️ Session expired. Please send the URL again.")
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
        f"`{source_url[:80]}{'...' if len(source_url) > 80 else ''}`"
    )

    try:
        await start_stream(source_url, target["link"], target["key"], cb.message, is_url_source=True)
    except Exception as e:
        await cb.message.edit_text(f"❌ Stream failed:\n`{e}`")


# ─────────────────────────────────────────────────────────────
# Text handler — /save replies AND direct URL input
# ─────────────────────────────────────────────────────────────
@app.on_message(
    filters.private
    & filters.text
    & ~filters.command([
        "start", "save", "delete", "status", "stop", "ping_db",
        "addadmin", "removeadmin", "admins",
    ])
)
async def on_text(client: Client, message: Message):
    uid = message.from_user.id
    text = message.text.strip()

    # ── /save reply flow ──────────────────────────────────────
    if uid in _save_pending:
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
        return

    # ── Direct URL stream flow ────────────────────────────────
    if is_url(text):
        if not is_authorized(uid):
            await message.reply("🚫 Unauthorized.")
            return

        _pending_url[uid] = text
        shown = text[:80] + ("..." if len(text) > 80 else "")
        intro = (
            f"🌐 **Direct URL detected:**\n`{shown}`\n\n"
            "📡 **Select a stream target:**\n\n"
            "ℹ️ _No download — FFmpeg reads from the URL directly. Zero extra CPU._"
        )
        await show_target_keyboard(message, "urlstream", intro)
        return

    # Unknown text — ignore silently


# ─────────────────────────────────────────────────────────────
# Video / document handler
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

    _pending_video[uid] = message

    await show_target_keyboard(message, "stream", "📡 **Select a stream target:**")


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()
    print(f"✅ Health server running on port {PORT}")

    # Load admins from DB before the bot starts accepting messages.
    # app.run() starts its own event loop, so we run the coroutine
    # synchronously right here using a temporary loop.
    loop = asyncio.new_event_loop()
    loop.run_until_complete(load_admins_from_db())
    loop.close()

    print("✅ Bot starting...")
    app.run()
