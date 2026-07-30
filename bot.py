"""
Telegram OTP Sender Bot — single file version.

Setup:
  pip install pyrogram tgcrypto python-telegram-bot

Env vars:
  TELEGRAM_BOT_TOKEN   — BotFather token
  TELEGRAM_API_ID      — my.telegram.org API ID
  TELEGRAM_API_HASH    — my.telegram.org API hash
  TELEGRAM_ADMIN_ID    — your Telegram user ID (only this user can control the bot)
"""

import asyncio
import logging
import os
import re
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional

from pyrogram import Client
from pyrogram.errors import FloodWait
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ─────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)

_pylog = logging.getLogger("otp_bot")


class AppLogger:
    def __init__(self, maxlen: int = 300):
        self._all: deque = deque(maxlen=maxlen)
        self._success: deque = deque(maxlen=maxlen)
        self._failed: deque = deque(maxlen=maxlen)

    @staticmethod
    def _ts() -> str:
        return datetime.now().strftime("%H:%M:%S")

    def _append(self, line: str) -> None:
        self._all.appendleft(line)

    def info(self, msg: str) -> None:
        _pylog.info(msg)
        self._append(f"[{self._ts()}] INFO  {msg}")

    def warning(self, msg: str) -> None:
        _pylog.warning(msg)
        self._append(f"[{self._ts()}] WARN  {msg}")

    def error(self, msg: str) -> None:
        _pylog.error(msg)
        self._append(f"[{self._ts()}] ERROR {msg}")

    def log_success(self, phone: str, msg: str) -> None:
        line = f"[{self._ts()}] ✅ {phone} — {msg}"
        self._success.appendleft(line)
        self._append(line)

    def log_failed(self, phone: str, msg: str) -> None:
        line = f"[{self._ts()}] ❌ {phone} — {msg}"
        self._failed.appendleft(line)
        self._append(line)

    def get_all_logs(self, n: int = 30) -> str:
        return "\n".join(list(self._all)[:n])

    def get_success_logs(self, n: int = 20) -> str:
        return "\n".join(list(self._success)[:n])

    def get_failed_logs(self, n: int = 20) -> str:
        return "\n".join(list(self._failed)[:n])


# ─────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────

def parse_phone_list(text: str) -> list:
    phones, seen = [], set()
    for raw in text.strip().splitlines():
        cleaned = re.sub(r"[\s\-\(\)\.]+", "", raw.strip())
        if re.match(r"^\+\d{7,15}$", cleaned) and cleaned not in seen:
            phones.append(cleaned)
            seen.add(cleaned)
    return phones


def format_duration(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds}s"
    m, s = divmod(seconds, 60)
    if m < 60:
        return f"{m}m {s}s"
    h, m = divmod(m, 60)
    return f"{h}h {m}m {s}s"


# ─────────────────────────────────────────────────────────────────────
# OTP Sender (Pyrogram)
# ─────────────────────────────────────────────────────────────────────

class ProcessStatus(Enum):
    IDLE     = "idle"
    RUNNING  = "running"
    STOPPING = "stopping"
    DONE     = "done"


@dataclass
class ProcessStats:
    total:   int = 0
    success: int = 0
    failed:  int = 0
    _start: float = field(default_factory=time.time)

    @property
    def remaining(self) -> int:
        return max(0, self.total - self.success - self.failed)

    @property
    def progress_pct(self) -> float:
        if self.total == 0:
            return 0.0
        return round((self.success + self.failed) / self.total * 100, 1)

    @property
    def elapsed(self) -> int:
        return int(time.time() - self._start)


API_ID   = 32249278
API_HASH = "6db59964aa54223b2f6f9b2ef8d700a6"
RECONNECT_EVERY = 20   # reconnect after every N numbers


class TelegramOTPSender:
    def __init__(self, log: AppLogger):
        self._log = log
        self._stats  = ProcessStats()
        self._status = ProcessStatus.IDLE
        self._should_stop = False
        self._client: Optional[Client] = None

    @property
    def stats(self) -> ProcessStats:
        return self._stats

    @property
    def status(self) -> ProcessStatus:
        return self._status

    def is_running(self) -> bool:
        return self._status == ProcessStatus.RUNNING

    def stop(self) -> None:
        self._should_stop = True
        self._status = ProcessStatus.STOPPING
        self._log.info("🛑 Stop requested.")

    def start(self, phones_text: str) -> bool:
        phones = parse_phone_list(phones_text)
        if not phones or self.is_running():
            return False
        self._stats = ProcessStats()
        self._stats.total = len(phones)
        self._should_stop = False
        self._status = ProcessStatus.RUNNING
        asyncio.create_task(self._run(phones))
        return True

    def _make_client(self) -> Client:
        return Client(
            name=":memory:",
            api_id=API_ID,
            api_hash=API_HASH,
            no_updates=True,
        )

    async def _reconnect(self) -> None:
        self._log.info("🔄 Reconnecting (off → on)…")
        if self._client is not None:
            try:
                await self._client.disconnect()
            except Exception:
                pass
            self._client = None
        await asyncio.sleep(0.1)
        self._client = self._make_client()
        await self._client.connect()
        self._log.info("✅ Connected.")

    async def _send_one(self, phone: str) -> None:
        try:
            await self._client.send_code(phone)
            self._stats.success += 1
            self._log.log_success(phone, "OTP sent ✓")
        except FloodWait as e:
            self._log.warning(f"FloodWait {e.value}s for {phone}, waiting…")
            await asyncio.sleep(e.value + 1)
            try:
                await self._client.send_code(phone)
                self._stats.success += 1
                self._log.log_success(phone, "OTP sent ✓ (after flood wait)")
            except Exception as e2:
                self._stats.failed += 1
                self._log.log_failed(phone, str(e2))
        except Exception as e:
            self._stats.failed += 1
            self._log.log_failed(phone, str(e))

    async def _run(self, phones: list) -> None:
        try:
            await self._reconnect()

            for i, phone in enumerate(phones):
                if self._should_stop:
                    self._log.info("🛑 Stopped by user.")
                    break

                if i > 0 and i % RECONNECT_EVERY == 0:
                    self._log.info(f"🔄 {i} done — reconnecting…")
                    await self._reconnect()

                await self._send_one(phone)
                await asyncio.sleep(0.01)

        except Exception as e:
            self._log.error(f"Batch error: {e}")
        finally:
            if self._client is not None:
                try:
                    await self._client.disconnect()
                except Exception:
                    pass
                self._client = None
            self._status = ProcessStatus.DONE
            s = self._stats
            self._log.info(
                f"✅ Done — sent:{s.success} failed:{s.failed} "
                f"time:{format_duration(s.elapsed)}"
            )


# ─────────────────────────────────────────────────────────────────────
# Telegram Bot (python-telegram-bot)
# ─────────────────────────────────────────────────────────────────────

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
ADMIN_ID  = 8523774444

KEYBOARD = ReplyKeyboardMarkup(
    [
        [KeyboardButton("📊 Stats"),       KeyboardButton("✅ Sent")],
        [KeyboardButton("❌ Failed"),      KeyboardButton("📋 Logs")],
        [KeyboardButton("🛑 Stop"),        KeyboardButton("🗑 Clear")],
        [KeyboardButton("💻 Developer")],
    ],
    resize_keyboard=True,
    is_persistent=True,
)

log     = AppLogger()
tester  = TelegramOTPSender(log)


async def _reply(update: Update, text: str) -> None:
    await update.effective_message.reply_text(
        text, parse_mode=ParseMode.MARKDOWN, reply_markup=KEYBOARD
    )


async def cmd_start(update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
    await _reply(
        update,
        "📲 *OTP Sender*\n\n"
        "ফোন নম্বর পেস্ট করুন (একটা করে প্রতি লাইনে):\n"
        "`+8801711000001`\n"
        "`+8801711000002`\n\n"
        "🔄 প্রতি ব্যাচের আগে এবং প্রতি *৫টা* নম্বরের পরে *off→on*।",
    )


async def cmd_stats(update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
    s = tester.stats
    await _reply(
        update,
        f"📊 *Statistics*\n\n"
        f"▶️ Status:    `{tester.status.value}`\n"
        f"📋 Total:     `{s.total}`\n"
        f"✅ Success:   `{s.success}`\n"
        f"❌ Failed:    `{s.failed}`\n"
        f"⏳ Remaining: `{s.remaining}`\n"
        f"📈 Progress:  `{s.progress_pct:.1f}%`\n"
        f"⏱ Elapsed:   `{format_duration(s.elapsed)}`",
    )


async def cmd_success(update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
    text = log.get_success_logs(20)
    if len(text) > 3800:
        text = "…\n" + text[-3600:]
    await _reply(update, f"✅ *Successful OTPs:*\n```\n{text or '(none yet)'}\n```")


async def cmd_failed(update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
    text = log.get_failed_logs(20)
    if len(text) > 3800:
        text = "…\n" + text[-3600:]
    await _reply(update, f"❌ *Failed numbers:*\n```\n{text or '(none yet)'}\n```")


async def cmd_logs(update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
    text = log.get_all_logs(30)
    if len(text) > 3800:
        text = "…(truncated)\n" + text[-3600:]
    await _reply(update, f"📋 *Logs:*\n```\n{text or '(empty)'}\n```")


async def cmd_developer(update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
    await update.effective_message.reply_text(
        "👨‍💻 *Developer*\n\n📲 [@Rabbi122q](https://t.me/Rabbi122q)",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=KEYBOARD,
    )


async def cmd_stop(update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if tester.is_running():
        tester.stop()
        await _reply(update, "🛑 Stop request sent…")
    else:
        await _reply(update, "ℹ️ Nothing is running right now.")


async def cmd_clear(update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if tester.is_running():
        await _reply(update, "⚠️ Still running — press 🛑 Stop first.")
        return
    tester._stats = ProcessStats()
    log._all.clear()
    log._success.clear()
    log._failed.clear()
    await _reply(update, "✅ Stats & logs cleared.")


async def handle_phones(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    text   = (update.message.text or "").strip()
    phones = parse_phone_list(text)

    if not phones:
        await _reply(
            update,
            "⚠️ কোনো valid নম্বর পাওয়া যায়নি।\n\nFormat:\n`+8801711000001`",
        )
        return

    if tester.is_running():
        await _reply(update, "⚠️ এখনো চলছে। 🛑 Stop করুন, তারপর নতুন নম্বর দিন।")
        return

    total = len(phones)
    sent_msg = await update.effective_message.reply_text(
        f"📋 *{total} টা নম্বর queued*\n\n"
        f"🔄 শুরুর আগে reconnect হচ্ছে…\n"
        f"⚡ প্রতি *{RECONNECT_EVERY}* টার পরে auto-reconnect\n\n"
        f"📨 Sent: *0/{total}*",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=KEYBOARD,
    )

    ok = tester.start(text)
    if not ok:
        await _reply(update, "❌ Start করতে পারেনি। Logs চেক করুন।")
        return

    asyncio.create_task(_live_counter(ctx, update.effective_chat.id, sent_msg.message_id, total))
    asyncio.create_task(_notify_when_done(update.effective_chat.id, total, ctx))


async def _live_counter(
    ctx: ContextTypes.DEFAULT_TYPE, chat_id: int, msg_id: int, total: int
) -> None:
    """Edit the queued message every 0.5s showing live sent count."""
    last_text = ""
    while tester.status == ProcessStatus.RUNNING:
        s = tester.stats
        sent = s.success + s.failed
        new_text = (
            f"📋 *{total} টা নম্বর queued*\n\n"
            f"🔄 প্রতি *{RECONNECT_EVERY}* টার পরে auto-reconnect\n\n"
            f"📨 Sent: *{sent}/{total}*  ✅{s.success}  ❌{s.failed}"
        )
        if new_text != last_text:
            last_text = new_text
            try:
                await ctx.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=msg_id,
                    text=new_text,
                    parse_mode=ParseMode.MARKDOWN,
                )
            except Exception:
                pass
        await asyncio.sleep(0.5)

    # Final update after done/stopped
    await asyncio.sleep(0.5)   # let stats settle
    s = tester.stats
    sent = s.success + s.failed
    try:
        await ctx.bot.edit_message_text(
            chat_id=chat_id,
            message_id=msg_id,
            text=(
                f"✅ *সম্পন্ন!*\n\n"
                f"📨 Sent:   *{sent}/{total}*\n"
                f"✅ Success: `{s.success}`\n"
                f"❌ Failed:  `{s.failed}`\n"
                f"⏱ Time:    `{format_duration(s.elapsed)}`"
            ),
            parse_mode=ParseMode.MARKDOWN,
        )
    except Exception:
        pass


async def _notify_when_done(chat_id: int, total: int, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    while tester.status not in (ProcessStatus.DONE, ProcessStatus.IDLE):
        await asyncio.sleep(3)

    s = tester.stats
    emoji = "✅" if s.failed == 0 else ("⚠️" if s.success > 0 else "❌")
    try:
        await ctx.bot.send_message(
            chat_id,
            f"{emoji} *সম্পন্ন!*\n\n"
            f"✅ Sent:   `{s.success}`\n"
            f"❌ Failed: `{s.failed}`\n"
            f"📋 Total:  `{total}`\n"
            f"⏱ Time:   `{format_duration(s.elapsed)}`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=KEYBOARD,
        )
    except Exception as e:
        _pylog.debug("Notify error: %s", e)


# ─────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────

def main() -> None:
    missing = []
    if not BOT_TOKEN:  missing.append("TELEGRAM_BOT_TOKEN")
    if not ADMIN_ID:   missing.append("TELEGRAM_ADMIN_ID")
    if not API_ID:     missing.append("TELEGRAM_API_ID")
    if not API_HASH:   missing.append("TELEGRAM_API_HASH")
    if missing:
        print(f"❌ Missing env vars: {', '.join(missing)}")
        sys.exit(1)

    app   = Application.builder().token(BOT_TOKEN).build()
    txt   = filters.TEXT & ~filters.COMMAND

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("stop",  cmd_stop))
    app.add_handler(CommandHandler("clear", cmd_clear))
    app.add_handler(CommandHandler("logs",  cmd_logs))

    app.add_handler(MessageHandler(txt & filters.Regex(r"^📊 Stats$"),        cmd_stats))
    app.add_handler(MessageHandler(txt & filters.Regex(r"^✅ Sent$"),         cmd_success))
    app.add_handler(MessageHandler(txt & filters.Regex(r"^❌ Failed$"),       cmd_failed))
    app.add_handler(MessageHandler(txt & filters.Regex(r"^📋 Logs$"),         cmd_logs))
    app.add_handler(MessageHandler(txt & filters.Regex(r"^🛑 Stop$"),         cmd_stop))
    app.add_handler(MessageHandler(txt & filters.Regex(r"^🗑 Clear$"),        cmd_clear))
    app.add_handler(MessageHandler(txt & filters.Regex(r"^💻 Developer$"), cmd_developer))
    app.add_handler(MessageHandler(txt, handle_phones))

    _pylog.info(f"Starting | admin={ADMIN_ID} | reconnect_every={RECONNECT_EVERY}")
    app.run_polling(drop_pending_updates=True, allowed_updates=["message"])


if __name__ == "__main__":
    main()
