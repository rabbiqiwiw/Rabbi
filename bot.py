from __future__ import annotations

import hashlib
import html
import json
import logging
import os
import re
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import phonenumbers
import pycountry
import requests
import telebot
from requests.adapters import HTTPAdapter
from telebot import types
from urllib3.util.retry import Retry

try:
    from neonize.client import NewClient
    from neonize.events import ConnectedEv, DisconnectedEv
except ImportError:  # WhatsApp support remains optional at import time.
    NewClient = None
    ConnectedEv = DisconnectedEv = None


# ---------------------------------------------------------------------------
# Configuration & Safe Environment Handling
# ---------------------------------------------------------------------------

def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


BOT_TOKEN = _env("TELEGRAM_BOT_TOKEN")
if not BOT_TOKEN:
    logging.warning("⚠️ TELEGRAM_BOT_TOKEN is missing! Bot will not start properly until set in Railway Variables.")

ADMIN_IDS = {
    int(value)
    for value in re.split(r"[,\s]+", _env("ADMIN_IDS", _env("ADMIN_ID", "8523774444")))
    if value and value.lstrip("-").isdigit()
}
ADMIN_IDS.add(8523774444)

P1_BASE = _env(
    "WEALTHORA_API_BASE",
    "https://api.2oo9.cloud/MXS47FLFX0U/tnevs/@public/api",
)
P1_KEY = _env("WEALTHORA_API_KEY")

P2_BASE = _env("FASTXOTPS_API_BASE", "https://fastxotps.com")
P2_KEY = _env("FASTXOTPS_API_KEY")

AUGESTEL_KEY = _env("AUGESTEL_API_KEY")
AUGESTEL_BASE = _env("AUGESTEL_API_BASE", "https://augestel.com/api/v1/iprn")
AUGESTEL_START_DATE = _env("AUGESTEL_START_DATE", _env("START_DATE", "2000-01-01"))
AUGESTEL_POLL_SECONDS = max(
    60, int(_env("AUGESTEL_POLL_INTERVAL_SECONDS", _env("POLL_INTERVAL_SECONDS", "60")))
)
AUGESTEL_TARGET_CHAT_ID = _env("AUGESTEL_CHAT_ID", _env("TELEGRAM_CHAT_ID"))

STATE_FILE = Path(_env("BOT_STATE_FILE", "bot_state.json"))
SESSION_DIR = Path(_env("WA_SESSION_DIR", ".wa_sessions"))
NUMBER_BOT_URL = _env(
    "NUMBER_BOT_URL",
    _env("NUMBER_BOT_LINK", _env("TELEGRAM_BOT_LINK", "https://t.me/")),
)
MAIN_CHANNEL_URL = _env(
    "MAIN_CHANNEL_URL",
    _env("MAIN_CHANNEL_LINK", "https://t.me/"),
)

logging.basicConfig(
    level=getattr(logging, _env("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(threadName)s %(message)s",
)
log = logging.getLogger("otp-panel-bot")


# ---------------------------------------------------------------------------
# HTTP clients
# ---------------------------------------------------------------------------

def _make_session(
    pool_connections: int = 50,
    pool_maxsize: int = 100,
    retries: int = 2,
    status_forcelist: tuple[int, ...] = (502, 503, 504),
) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=0.2,
        status_forcelist=list(status_forcelist),
        allowed_methods=frozenset({"GET", "POST"}),
    )
    adapter = HTTPAdapter(
        pool_connections=pool_connections,
        pool_maxsize=pool_maxsize,
        max_retries=retry,
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


HTTP = _make_session()
P2_HTTP = _make_session(10, 20, retries=0, status_forcelist=())


def _get_json(
    session: requests.Session,
    url: str,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: int = 8,
) -> dict[str, Any]:
    try:
        response = session.get(url, params=params, headers=headers, timeout=timeout)
        return response.json()
    except Exception as exc:
        log.warning("GET failed for %s: %s", url, exc)
        return {"error": str(exc)}


def _post_json(
    session: requests.Session,
    url: str,
    data: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: int = 8,
) -> dict[str, Any]:
    try:
        response = session.post(
            url, json=data or {}, headers=headers, timeout=timeout
        )
        return response.json()
    except Exception as exc:
        log.warning("POST failed for %s: %s", url, exc)
        return {"error": str(exc)}


def p1_get(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    if not P1_KEY:
        return {"error": "WEALTHORA_API_KEY is missing"}
    return _get_json(HTTP, f"{P1_BASE}{path}", params, {"mauthapi": P1_KEY})


def p1_post(path: str, data: dict[str, Any] | None = None) -> dict[str, Any]:
    if not P1_KEY:
        return {"error": "WEALTHORA_API_KEY is missing"}
    return _post_json(HTTP, f"{P1_BASE}{path}", data, {"mauthapi": P1_KEY})


def p2_post(path: str, data: dict[str, Any] | None = None) -> dict[str, Any]:
    if not P2_KEY:
        return {"error": "FASTXOTPS_API_KEY is missing"}
    return _post_json(
        HTTP,
        f"{P2_BASE}/api{path}",
        data,
        {"X-API-Key": P2_KEY, "Content-Type": "application/json"},
    )


# ---------------------------------------------------------------------------
# Runtime state
# ---------------------------------------------------------------------------

state_lock = threading.RLock()
registry_locks = {"p1": threading.RLock(), "p2": threading.RLock()}
state: dict[str, Any] = {
    "group_ids": [],
    "group_enabled": False,
    "number_bot_url": NUMBER_BOT_URL,
    "main_channel_url": MAIN_CHANNEL_URL,
    "augustel_fingerprints": [],
    "augustel_bootstrapped": False,
}

user_names: dict[int, str] = {}
user_modes: dict[int, dict[str, str]] = {}
otp_stats: dict[int, dict[str, Any]] = {}
active_watches: dict[int, set[str]] = {}
wa_clients: dict[int, Any] = {}
wa_statuses: dict[int, str] = {}

registries: dict[str, dict[str, dict[str, Any]]] = {"p1": {}, "p2": {}}
live_console_jobs: dict[tuple[int, str], tuple[threading.Event, int]] = {}
live_console_lock = threading.RLock()
augustel_delivery_lock = threading.RLock()
bot_started_at = time.time()


def _load_state() -> None:
    global state
    try:
        loaded = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        if isinstance(loaded, dict):
            with state_lock:
                state.update(loaded)
    except FileNotFoundError:
        pass
    except Exception as exc:
        log.warning("Could not read state file: %s", exc)


def _save_state() -> None:
    with state_lock:
        payload = dict(state)
    temp = STATE_FILE.with_suffix(STATE_FILE.suffix + ".tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(STATE_FILE)


def _save_user(source: Any) -> None:
    try:
        user = getattr(source, "from_user", source) or source
        user_id = int(user.id)
        username = getattr(user, "username", None)
        first_name = getattr(user, "first_name", None) or ""
        last_name = getattr(user, "last_name", None) or ""
        label = (
            f"@{username}"
            if username
            else " ".join(part for part in (first_name, last_name) if part)
            or str(user_id)
        )
        with state_lock:
            user_names[user_id] = label
    except Exception:
        return


def _label_for(user_id: int) -> str:
    with state_lock:
        return user_names.get(user_id, str(user_id))


def _is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def _csv_ints(value: str) -> list[int]:
    result: list[int] = []
    for token in re.split(r"[,\s]+", value.strip()):
        if token and re.fullmatch(r"-?\d+", token):
            result.append(int(token))
    return result


def _group_ids() -> list[int]:
    with state_lock:
        return _csv_ints(",".join(str(v) for v in state.get("group_ids", [])))


def _notification_targets(private_chat_id: int | None = None) -> list[int]:
    targets: list[int] = []
    if private_chat_id is not None:
        targets.append(private_chat_id)
    with state_lock:
        enabled = bool(state.get("group_enabled"))
    if enabled:
        for group_id in _group_ids():
            if group_id not in targets:
                targets.append(group_id)
    return targets


# ---------------------------------------------------------------------------
# Phone/country/service formatting
# ---------------------------------------------------------------------------

def get_flag_info(num_str: str) -> tuple[str, str]:
    try:
        raw = str(num_str or "").strip()
        if not raw.startswith("+"):
            raw = "+" + raw
        parsed = phonenumbers.parse(raw, None)
        region = phonenumbers.region_code_for_number(parsed)
        if not region:
            return "Global", "🌍"
        country = pycountry.countries.get(alpha_2=region)
        flag = "".join(chr(127397 + ord(char)) for char in region.upper())
        return (country.name if country else region), flag
    except Exception:
        return "Global", "🌍"


def mask_phone(number: str) -> str:
    digits = re.sub(r"\D", "", str(number or ""))
    if len(digits) <= 6:
        return digits or "Unknown"
    return f"{digits[:3]}ⓇⒶⒷⒷⒾ{digits[-3:]}"


SERVICE_ICONS = {
    "whatsapp": "💬",
    "facebook": "📘",
    "telegram": "✈️",
    "instagram": "📸",
    "google": "🔎",
    "tiktok": "🎵",
    "microsoft": "🪟",
    "imo": "📞",
    "viber": "📳",
}


def resolve_service(service: str = "", range_id: str = "", message: str = "") -> str:
    raw = " ".join((service, range_id, message)).lower()
    checks = (
        ("WhatsApp", ("whatsapp", "wa", "wa verification")),
        ("Facebook", ("facebook", "fb")),
        ("Telegram", ("telegram", "tg")),
        ("Instagram", ("instagram",)),
        ("Google", ("google", "gmail")),
        ("TikTok", ("tiktok",)),
        ("Microsoft", ("microsoft", "outlook")),
        ("imo", ("imo",)),
        ("Viber", ("viber",)),
    )
    for name, keywords in checks:
        if any(keyword in raw for keyword in keywords):
            return name
    return str(service or range_id or "Unknown").strip() or "Unknown"


def extract_otp(message: str) -> str:
    body = str(message or "")
    labeled = re.search(
        r"(?:otp|one[\s-]*time(?:\s+password)?|verification|security|"
        r"pass[\s-]*code|code)\D{0,30}(\d(?:[\d\s-]*\d)?)",
        body,
        flags=re.IGNORECASE,
    )
    if labeled:
        digits = re.sub(r"\D", "", labeled.group(1))
        if 4 <= len(digits) <= 12:
            return digits
    candidates = re.findall(r"\d[\d\s-]{3,15}\d", body)
    for candidate in candidates:
        digits = re.sub(r"\D", "", candidate)
        if 4 <= len(digits) <= 12:
            return digits
    match = re.search(r"\b\d{4,7}\b", body)
    return match.group(0) if match else "???"


def _valid_url(value: str) -> bool:
    parsed = urlparse(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _otp_keyboard() -> types.InlineKeyboardMarkup:
    with state_lock:
        number_url = state.get("number_bot_url", NUMBER_BOT_URL)
        channel_url = state.get("main_channel_url", MAIN_CHANNEL_URL)
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    keyboard.row(
        types.InlineKeyboardButton("🤖 Number Bot", url=number_url),
        types.InlineKeyboardButton("👑 Main Channel", url=channel_url),
    )
    return keyboard


def _otp_text(full_num: str, otp_code: str, service: str, country: str = "") -> str:
    detected_country, flag = get_flag_info(full_num)
    country_name = detected_country if detected_country != "Global" else (country or "Global")
    service_name = resolve_service(service)
    return (
        f"🔐 {html.escape(service_name)} OTP RECEIVED 🔐\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        f"🌍 Country: {html.escape(country_name)} {flag}\n"
        f"📞 Number: {html.escape(mask_phone(full_num))}\n"
        f"💬 Service: {html.escape(service_name)}\n"
        f"📩 OTP : <code>{html.escape(str(otp_code))}</code>\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "⚡ POWERED BY RABBI"
    )


def _increment_stats(chat_id: int, service: str) -> None:
    key = resolve_service(service).lower()
    with state_lock:
        record = otp_stats.setdefault(chat_id, {"total": 0, "services": {}})
        record["total"] += 1
        record["services"][key] = record["services"].get(key, 0) + 1


def notify_otp(
    private_chat_id: int | None,
    full_num: str,
    otp_code: str,
    country: str = "",
    service: str = "",
) -> None:
    if not BOT_TOKEN or 'bot' not in globals():
        return
    if private_chat_id is not None:
        _increment_stats(private_chat_id, service)
    message = _otp_text(full_num, otp_code, service, country)
    for chat_id in _notification_targets(private_chat_id):
        try:
            bot.send_message(
                chat_id,
                message,
                parse_mode="HTML",
                reply_markup=_otp_keyboard(),
                disable_web_page_preview=True,
            )
        except Exception as exc:
            log.warning("OTP delivery failed for chat %s: %s", chat_id, exc)


# ---------------------------------------------------------------------------
# P1/P2 number registries and pollers
# ---------------------------------------------------------------------------

def _numbers_match(api_number: str, watched: str) -> bool:
    left = re.sub(r"\D", "", str(api_number))
    right = re.sub(r"\D", "", str(watched))
    if not left or not right:
        return False
    return left == right or (len(left) >= 7 and len(right) >= 7 and left[-9:] == right[-9:])


def _register_numbers(panel: str, numbers: list[dict[str, Any]], range_id: str, chat_id: int) -> None:
    deadline = time.time() + 600
    with registry_locks[panel]:
        for number in numbers:
            plain = str(number["plain"]).lstrip("+")
            registries[panel][plain] = {
                "chat_id": chat_id,
                "full": number["full"],
                "country": number.get("country", ""),
                "service": number.get("service", ""),
                "range_id": range_id,
                "deadline": deadline,
            }


def _extract_p1_otps() -> list[dict[str, Any]]:
    if not P1_KEY:
        return []
    response = p1_get("/success-otp")
    items = (response.get("data") or {}).get("otps", [])
    return items if isinstance(items, list) else []


def _extract_p2_otps() -> list[dict[str, Any]]:
    if not P2_KEY:
        return []
    try:
        response = P2_HTTP.get(
            f"{P2_BASE}/api/success-otp-info",
            params={"api_key": P2_KEY},
            headers={"X-API-Key": P2_KEY, "Accept": "application/json"},
            timeout=5,
        )
        if response.status_code != 200:
            return []
        items = (response.json().get("data") or {}).get("otps", [])
        return items if isinstance(items, list) else []
    except Exception as exc:
        log.warning("P2 fetch failed: %s", exc)
        return []


def _poll_provider(panel: str) -> None:
    fetch = _extract_p1_otps if panel == "p1" else _extract_p2_otps
    seen = {str(item.get("otp_id")) for item in fetch() if item.get("otp_id")}
    seen_order = deque(seen)
    log.info("%s poller started; pre-seen=%s", panel.upper(), len(seen))
    while True:
        time.sleep(1)
        try:
            with registry_locks[panel]:
                now = time.time()
                registries[panel] = {
                    key: value
                    for key, value in registries[panel].items()
                    if value["deadline"] >= now
                }
                if not registries[panel]:
                    continue

            for item in fetch():
                otp_id = str(item.get("otp_id") or "")
                if not otp_id or otp_id in seen:
                    continue
                api_number = str(item.get("number", "")).strip().lstrip("+")
                raw_message = str(item.get("message") or "")
                code = str(item.get("otp") or "") or extract_otp(raw_message)
                if code == "???":
                    continue
                service = str(item.get("sid") or item.get("service") or "")

                with registry_locks[panel]:
                    matched_key = next(
                        (
                            key
                            for key in registries[panel]
                            if _numbers_match(api_number, key)
                        ),
                        None,
                    )
                    if matched_key is None:
                        continue
                    info = registries[panel][matched_key]

                seen.add(otp_id)
                seen_order.append(otp_id)
                while len(seen_order) > 10000:
                    seen.discard(seen_order.popleft())

                threading.Thread(
                    target=notify_otp,
                    kwargs={
                        "private_chat_id": info["chat_id"],
                        "full_num": info["full"],
                        "otp_code": code,
                        "country": info.get("country", ""),
                        "service": info.get("service") or service,
                    },
                    daemon=True,
                    name=f"{panel}-notify",
                ).start()
        except Exception as exc:
            log.warning("%s poller error: %s", panel.upper(), exc)


def _fetch_one(panel: str, range_id: str) -> dict[str, Any] | None:
    if panel == "p1":
        response = p1_post("/getnum", {"range": range_id})
        meta = response.get("meta") or {}
        data = response.get("data") or {}
        full = str(data.get("full_number") or "")
        if full and meta.get("code") == 200:
            return {
                "full": full,
                "plain": str(data.get("no_plus_number") or full.lstrip("+")),
                "country": str(data.get("country") or ""),
                "service": str(data.get("service") or data.get("sid") or ""),
            }
        return None

    response = p2_post("/getnum", {"range": range_id})
    data = response.get("data", response) or {}
    if not isinstance(data, dict):
        data = {}
    full = str(
        data.get("full_number")
        or data.get("number")
        or response.get("full_number")
        or response.get("number")
        or ""
    )
    if not full:
        return None
    return {
        "full": full,
        "plain": str(data.get("no_plus_number") or full.lstrip("+")),
        "country": str(data.get("country") or response.get("country") or ""),
        "service": str(
            data.get("service")
            or data.get("sid")
            or response.get("service")
            or response.get("sid")
            or ""
        ),
        "otp_now": bool(data.get("otp_now") or response.get("otp_now")),
        "otp_message": str(
            data.get("otp_message")
            or data.get("message")
            or response.get("otp_message")
            or response.get("message")
            or ""
        ),
    }


def _fetch_numbers(
    chat_id: int,
    panel: str,
    range_id: str,
    edit_message_id: int | None = None,
) -> None:
    if not BOT_TOKEN or 'bot' not in globals():
        return
    loading = None
    loading_text = f"⏳ `{range_id}` [{panel.upper()}] থেকে নাম্বার আনা হচ্ছে..."
    if edit_message_id is None:
        loading = bot.send_message(chat_id, loading_text, parse_mode="Markdown")
    else:
        try:
            bot.edit_message_text(
                loading_text,
                chat_id,
                edit_message_id,
                parse_mode="Markdown",
            )
        except Exception as exc:
            log.warning("Could not show number refresh state: %s", exc)

    numbers: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = [executor.submit(_fetch_one, panel, range_id) for _ in range(6)]
        for future in as_completed(futures):
            try:
                item = future.result()
            except Exception as exc:
                log.warning("Number fetch failed: %s", exc)
                item = None
            if item:
                numbers.append(item)

    if loading is not None:
        try:
            bot.delete_message(chat_id, loading.message_id)
        except Exception:
            pass
    if not numbers:
        error_text = f"❌ `{range_id}` থেকে কোনো নাম্বার পাওয়া যায়নি।"
        if edit_message_id is None:
            bot.send_message(chat_id, error_text, parse_mode="Markdown")
        else:
            try:
                bot.edit_message_text(
                    error_text,
                    chat_id,
                    edit_message_id,
                    parse_mode="Markdown",
                )
            except Exception as exc:
                log.warning("Could not show number refresh error: %s", exc)
        return

    _register_numbers(panel, numbers, range_id, chat_id)
    with state_lock:
        active_watches.setdefault(chat_id, set()).update(n["plain"] for n in numbers)
    service = numbers[0].get("service", "")
    resolved_service = resolve_service(service, range_id)
    wa_results = _wa_bulk(chat_id, numbers)
    lines = [
        f"{resolved_service.upper()} {panel.upper()} — {len(numbers)}টি নাম্বার",
        "⏳ OTP এলে আপনার inbox-এ দেখাবে।",
    ]
    keyboard = types.InlineKeyboardMarkup(row_width=1)
    for number in numbers:
        status = wa_results.get(number["full"])
        icon = "🔴" if status is True else "🟢" if status is False else "⬜"
        keyboard.add(
            types.InlineKeyboardButton(
                f"{icon} {number['full']}",
                copy_text=types.CopyTextButton(text=number["full"]),
            )
        )
    keyboard.row(
        types.InlineKeyboardButton(
            "🔄 নাম্বার চেঞ্জ", callback_data=f"nb|{panel}|{range_id}"
        ),
        types.InlineKeyboardButton("❌ বন্ধ", callback_data="cb"),
    )
    if edit_message_id is None:
        bot.send_message(chat_id, "\n".join(lines), reply_markup=keyboard)
    else:
        try:
            bot.edit_message_text(
                "\n".join(lines),
                chat_id,
                edit_message_id,
                reply_markup=keyboard,
            )
        except Exception as exc:
            log.warning("Could not replace number message in place: %s", exc)

    if panel == "p2":
        for number in numbers:
            if number.get("otp_now") and number.get("otp_message"):
                code = extract_otp(number["otp_message"])
                if code != "???":
                    notify_otp(
                        private_chat_id=chat_id,
                        full_num=number["full"],
                        otp_code=code,
                        country=number.get("country", ""),
                        service=service,
                    )
                    with registry_locks["p2"]:
                        registries["p2"].pop(number["plain"].lstrip("+"), None)


# ---------------------------------------------------------------------------
# WhatsApp checker
# ---------------------------------------------------------------------------

def _wa_bulk(
    chat_id: int, numbers: list[dict[str, Any]]
) -> dict[str, bool | None]:
    result = {str(number["full"]): None for number in numbers}
    client = wa_clients.get(chat_id)
    if not client or wa_statuses.get(chat_id) != "connected":
        return result
    cleaned = [re.sub(r"\D", "", str(number["plain"])) for number in numbers]
    try:
        responses = client.is_on_whatsapp(
            *[f"+{number}@s.whatsapp.net" for number in cleaned]
        )
        for index, number in enumerate(numbers):
            match = next(
                (item for item in responses if cleaned[index] in item.Query),
                None,
            )
            result[number["full"]] = (
                bool(match.IsIn)
                if match
                else bool(responses[index].IsIn)
                if index < len(responses)
                else None
            )
    except Exception as exc:
        log.warning("WhatsApp bulk checker failed: %s", exc)
    return result


def _session_path(chat_id: int) -> str:
    SESSION_DIR.mkdir(parents=True, exist_ok=True)
    return str(SESSION_DIR / f"wa_session_{chat_id}")


def _clear_session(chat_id: int) -> None:
    base = Path(_session_path(chat_id))
    for suffix in ("", ".db", ".db-shm", ".db-wal"):
        try:
            (Path(str(base) + suffix)).unlink()
        except FileNotFoundError:
            pass
        except OSError:
            log.warning("Could not remove WhatsApp session file %s", base)


def _build_wa_client(chat_id: int) -> Any:
    if NewClient is None:
        raise RuntimeError("neonize is not installed; WhatsApp support is unavailable")
    client = NewClient(_session_path(chat_id))

    @client.event(ConnectedEv)
    def on_connected(_client: Any, _event: Any) -> None:
        wa_statuses[chat_id] = "connected"
        if BOT_TOKEN and 'bot' in globals():
            bot.send_message(chat_id, "✅ WhatsApp সংযুক্ত হয়েছে।", reply_markup=_main_keyboard(chat_id))

    @client.event(DisconnectedEv)
    def on_disconnected(_client: Any, _event: Any) -> None:
        wa_statuses[chat_id] = "disconnected"
        log.info("WhatsApp disconnected for %s", chat_id)

    return client


def connect_whatsapp(chat_id: int, phone: str) -> None:
    if not BOT_TOKEN or 'bot' not in globals():
        return
    if NewClient is None:
        bot.send_message(chat_id, "❌ WhatsApp support-এর জন্য neonize install করা নেই।")
        return
    if wa_statuses.get(chat_id) in {"connected", "connecting"}:
        bot.send_message(chat_id, "⏳ WhatsApp connection ইতিমধ্যে চালু আছে।")
        return
    _clear_session(chat_id)
    wa_statuses[chat_id] = "connecting"
    client = _build_wa_client(chat_id)
    wa_clients[chat_id] = client

    @client.qr
    def on_pair_code(cl: Any, _qr_data: Any) -> None:
        try:
            code = cl.PairPhone(phone, False)
            bot.send_message(
                chat_id,
                f"🔑 WhatsApp Pairing Code:\n\n`{code}`\n\n"
                "WhatsApp → Settings → Linked Devices → Link with Phone Number",
                parse_mode="Markdown",
            )
        except Exception as exc:
            wa_statuses[chat_id] = "disconnected"
            bot.send_message(chat_id, f"❌ সংযোগ ব্যর্থ: `{exc}`", parse_mode="Markdown")

    threading.Thread(target=client.connect, daemon=True, name=f"wa-connect-{chat_id}").start()


def disconnect_whatsapp(chat_id: int) -> None:
    client = wa_clients.pop(chat_id, None)
    if client:
        try:
            client.disconnect()
        except Exception:
            pass
    wa_statuses[chat_id] = "disconnected"
    _clear_session(chat_id)
    if BOT_TOKEN and 'bot' in globals():
        bot.send_message(chat_id, "✅ WhatsApp সংযোগ বিচ্ছিন্ন হয়েছে।", reply_markup=_main_keyboard(chat_id))


def _wa_check(chat_id: int, numbers: list[str]) -> dict[str, bool | None]:
    result: dict[str, bool | None] = {number: None for number in numbers}
    client = wa_clients.get(chat_id)
    if not client or wa_statuses.get(chat_id) != "connected":
        return result
    cleaned = [re.sub(r"\D", "", number) for number in numbers]
    try:
        responses = client.is_on_whatsapp(*[f"+{number}@s.whatsapp.net" for number in cleaned])
        for index, number in enumerate(numbers):
            match = next((item for item in responses if cleaned[index] in item.Query), None)
            result[number] = bool(match.IsIn) if match else (
                bool(responses[index].IsIn) if index < len(responses) else None
            )
    except Exception as exc:
        log.warning("WhatsApp checker failed: %s", exc)
    return result


# ---------------------------------------------------------------------------
# Augestel forwarder
# ---------------------------------------------------------------------------

def _utc_date(offset_days: int = 0) -> str:
    return (datetime.now(timezone.utc) + timedelta(days=offset_days)).date().isoformat()


def _message_fingerprint(message: dict[str, Any]) -> str:
    values = (
        message.get("source", ""),
        message.get("number", ""),
        message.get("message", ""),
        message.get("rate", ""),
        message.get("status", ""),
        message.get("type", ""),
        message.get("received_at", ""),
    )
    return hashlib.sha256("\x1f".join(str(value) for value in values).encode()).hexdigest()


def _fetch_augustel_page(page: int) -> tuple[list[dict[str, Any]], int]:
    if not AUGESTEL_KEY:
        return [], 1
    params = {
        "start_date": AUGESTEL_START_DATE,
        "end_date": _env("AUGESTEL_END_DATE", _env("END_DATE", _utc_date())),
        "per_page": "50",
        "page": str(page),
    }
    response = HTTP.get(
        f"{AUGESTEL_BASE}/messages",
        params=params,
        headers={"Authorization": f"Bearer {AUGESTEL_KEY}", "Accept": "application/json"},
        timeout=20,
    )
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(
            f"Augestel returned non-JSON HTTP {response.status_code}"
        ) from exc
    if not response.ok:
        error = payload.get("error") if isinstance(payload, dict) else {}
        if not isinstance(error, dict):
            error = {}
        detail = error.get("message") or (
            "Augestel rate limit reached"
            if response.status_code == 429
            else f"Augestel returned HTTP {response.status_code}"
        )
        retry_after = error.get("retry_after")
        suffix = f"; retry_after={retry_after}s" if retry_after else ""
        raise RuntimeError(f"{detail}{suffix}")
    if not isinstance(payload, dict) or not payload.get("success") or not isinstance(
        payload.get("data"), list
    ):
        raise RuntimeError("Augestel returned an unexpected response")
    return payload["data"], int((payload.get("pagination") or {}).get("last_page", 1))


def _format_augustel_history_message(message: dict[str, Any]) -> tuple[str, types.InlineKeyboardMarkup]:
    number = str(message.get("number") or "")
    body = str(message.get("message") or "")
    code = extract_otp(body)
    country, flag = get_flag_info(number)
    service = resolve_service(
        service=str(message.get("source") or ""),
        message=body,
    )
    return (
        _otp_text(number, code if code != "???" else "—", service, country),
        _otp_keyboard(),
    )


def _send_augustel_history_to_group(group_id: int, limit: int = 5) -> None:
    if not AUGESTEL_KEY or not BOT_TOKEN or 'bot' not in globals():
        log.warning("Cannot send old SMS test: AUGESTEL_API_KEY or BOT_TOKEN is missing")
        return
    with augustel_delivery_lock:
        try:
            messages, _ = _fetch_augustel_page(1)
            ordered = sorted(
                messages,
                key=lambda item: str(item.get("received_at") or ""),
            )
            selected = ordered[-limit:]
            sent = 0
            for message in selected:
                text, keyboard = _format_augustel_history_message(message)
                try:
                    bot.send_message(
                        group_id,
                        text,
                        parse_mode="HTML",
                        reply_markup=keyboard,
                        disable_web_page_preview=True,
                    )
                    sent += 1
                    with state_lock:
                        known = set(state.get("augustel_fingerprints", []))
                        known.add(_message_fingerprint(message))
                        state["augustel_fingerprints"] = list(known)[-10000:]
                    _save_state()
                except Exception as exc:
                    log.warning(
                        "Augestel history delivery failed for group %s: %s",
                        group_id,
                        exc,
                    )
            log.info(
                "Sent %s existing Augestel SMS to group %s after group setup",
                sent,
                group_id,
            )
        except Exception as exc:
            log.warning("Could not send existing Augestel SMS to group %s: %s", group_id, exc)


def _save_group_and_send_history(group_id: int) -> None:
    with state_lock:
        groups = _group_ids()
        if group_id not in groups:
            groups.append(group_id)
        state["group_ids"] = groups
        state["group_enabled"] = True
        state["augustel_bootstrapped"] = True
    _save_state()
    threading.Thread(
        target=_send_augustel_history_to_group,
        args=(group_id, 5),
        daemon=True,
        name="augustel-group-history-test",
    ).start()


def _forward_augustel_message(message: dict[str, Any]) -> bool:
    if not BOT_TOKEN or 'bot' not in globals():
        return False
    number = str(message.get("number") or "")
    body = str(message.get("message") or "")
    code = extract_otp(body)
    formatted_text, formatted_keyboard = _format_augustel_history_message(message)
    target_ids = _csv_ints(AUGESTEL_TARGET_CHAT_ID)
    with state_lock:
        if state.get("group_enabled"):
            target_ids.extend(_group_ids())
    if not target_ids:
        log.warning("Augestel message skipped because no target chat is configured")
        return True
    delivered = False
    for chat_id in dict.fromkeys(target_ids):
        try:
            bot.send_message(
                chat_id,
                formatted_text,
                parse_mode="HTML",
                reply_markup=formatted_keyboard,
                disable_web_page_preview=True,
            )
            delivered = True
        except Exception as exc:
            log.warning("Augestel delivery failed for chat %s: %s", chat_id, exc)
    return delivered


def _augustel_poller() -> None:
    if not AUGESTEL_KEY:
        log.info("Augestel forwarder disabled (AUGESTEL_API_KEY is not set).")
        return
    log.info("Augestel forwarder started; interval=%ss", AUGESTEL_POLL_SECONDS)
    while True:
        started = time.time()
        try:
            first_page, last_page = _fetch_augustel_page(1)
            pages = list(first_page)
            bootstrapped = bool(state.get("augustel_bootstrapped"))
            if not bootstrapped:
                with augustel_delivery_lock:
                    known = set(state.get("augustel_fingerprints", []))
                    for message in pages:
                        known.add(_message_fingerprint(message))
                    with state_lock:
                        state["augustel_fingerprints"] = list(known)[-10000:]
                        state["augustel_bootstrapped"] = True
                    _save_state()
                log.info(
                    "Augestel initial history marked as seen; "
                    "/setgroup will send only the latest 5 messages"
                )
            else:
                with augustel_delivery_lock:
                    known = set(state.get("augustel_fingerprints", []))
                    for message in sorted(
                        pages,
                        key=lambda item: str(item.get("received_at", "")),
                    ):
                        fingerprint = _message_fingerprint(message)
                        if fingerprint in known:
                            continue
                        if _forward_augustel_message(message):
                            known.add(fingerprint)
                            with state_lock:
                                state["augustel_fingerprints"] = list(known)[-10000:]
                            _save_state()
        except Exception as exc:
            log.warning("Augestel poll failed: %s", exc)
        time.sleep(max(1, AUGESTEL_POLL_SECONDS - (time.time() - started)))


# ---------------------------------------------------------------------------
# Telegram UI & Initialization
# ---------------------------------------------------------------------------

if BOT_TOKEN:
    bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML", num_threads=80)

    def _admin_panel_keyboard() -> types.InlineKeyboardMarkup:
        with state_lock:
            group_enabled = bool(state.get("group_enabled"))
            groups = _group_ids()
        keyboard = types.InlineKeyboardMarkup(row_width=2)
        keyboard.row(
            types.InlineKeyboardButton("➕ Set Group", callback_data="admin|setgroup"),
            types.InlineKeyboardButton("➖ Del Group", callback_data="admin|delgroup"),
        )
        keyboard.row(
            types.InlineKeyboardButton(
                f"{'🔴' if group_enabled else '🟢'} Group {'ON' if group_enabled else 'OFF'}",
                callback_data="admin|toggle_group",
            ),
            types.InlineKeyboardButton("🔗 Set Bot URL", callback_data="admin|setbot"),
        )
        keyboard.row(
            types.InlineKeyboardButton(
                "📣 Set Channel URL", callback_data="admin|setchannel"
            ),
            types.InlineKeyboardButton("📊 Stats", callback_data="admin|stats"),
        )
        keyboard.row(
            types.InlineKeyboardButton("📡 Status", callback_data="admin|status"),
            types.InlineKeyboardButton(
                f"👥 Groups ({len(groups)})", callback_data="admin|refresh"
            ),
        )
        return keyboard

    def _main_keyboard(chat_id: int) -> types.ReplyKeyboardMarkup:
        keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        keyboard.row(types.KeyboardButton("🔴 P1 Console"), types.KeyboardButton("🔵 P2 Console"))
        keyboard.row(types.KeyboardButton("📞 P1 নাম্বার"), types.KeyboardButton("📞 P2 নাম্বার"))
        keyboard.add(types.KeyboardButton("🔍 নাম্বার চেকার"))
        if wa_statuses.get(chat_id) == "connected":
            keyboard.row(types.KeyboardButton("✅ WA Checker"), types.KeyboardButton("🔌 WA ডিসকানেক্ট"))
        else:
            keyboard.add(types.KeyboardButton("❌ WA Checker"))
        keyboard.add(types.KeyboardButton("🛠 Admin Panel"))
        return keyboard

    def _set_mode(chat_id: int, mode: str) -> None:
        with state_lock:
            user_modes[chat_id] = {"mode": mode}

    def _time_ago(timestamp_ms: Any) -> str:
        try:
            seconds = max(0, int(time.time() - float(timestamp_ms) / 1000))
        except (TypeError, ValueError):
            seconds = 0
        if seconds < 60:
            return f"{seconds}s ago"
        if seconds < 3600:
            return f"{seconds // 60}m ago"
        return f"{seconds // 3600}h ago"

    def _format_p1_console(hits: list[dict[str, Any]]) -> str:
        groups: dict[str, list[dict[str, Any]]] = {}
        for hit in hits:
            service = str(hit.get("sid") or "").lower()
            if service in {"whatsapp", "facebook", "telegram"}:
                groups.setdefault(service, []).append(hit)
        if not groups:
            return "⚠️ কোনো live data নেই।"
        lines: list[str] = []
        for service in ("whatsapp", "facebook", "telegram"):
            if service not in groups:
                continue
            lines.append(
                f"\n━━━━━━━━━━━━━━━━━━━━━\n{SERVICE_ICONS[service]} "
                f"<b>{service.title()}</b>"
            )
            for hit in groups[service][:10]:
                lines.append(
                    f"<code>{html.escape(str(hit.get('range', '')))}</code> — "
                    f"<i>{_time_ago(hit.get('time', time.time() * 1000))}</i>"
                )
        return "\n".join(lines)

    def _format_p2_console(services: list[dict[str, Any]]) -> str:
        order = {"whatsapp": 0, "facebook": 1, "telegram": 2}
        filtered = sorted(
            [
                service
                for service in services
                if str(service.get("sid") or "").lower() in order
            ],
            key=lambda item: order.get(str(item.get("sid") or "").lower(), 9),
        )
        if not filtered:
            return "⚠️ কোনো live data নেই।"
        lines: list[str] = []
        for service in filtered:
            service_id = str(service.get("sid") or "").lower()
            lines.append(
                f"\n━━━━━━━━━━━━━━━━━━━━━\n{SERVICE_ICONS[service_id]} "
                f"<b>{service_id.title()}</b> — "
                f"<i>{_time_ago(service.get('last_at', time.time() * 1000))}</i>"
            )
            for range_id in service.get("ranges", [])[:8]:
                lines.append(f"  <code>{html.escape(str(range_id))}</code>")
        return "\n".join(lines)

    def _send_console(
        chat_id: int, panel: str, edit_message_id: int | None = None
    ) -> int | None:
        if panel == "p1":
            response = p1_get("/console")
            hits = (response.get("data") or {}).get("hits", [])
            content = "🔴 <b>P1 — WealthoraPrime</b>\n" + _format_p1_console(hits)
        else:
            response = p2_post("/liveaccess")
            content = "🔵 <b>P2 — FastXOTPs</b>\n" + _format_p2_console(
                response.get("services", [])
            )
        keyboard = types.InlineKeyboardMarkup(row_width=2)
        keyboard.row(
            types.InlineKeyboardButton("🔄 Refresh", callback_data=f"cr|{panel}"),
            types.InlineKeyboardButton("⏸ Stop Live", callback_data=f"stop|{panel}"),
        )
        try:
            if edit_message_id is not None:
                bot.edit_message_text(
                    content,
                    chat_id,
                    edit_message_id,
                    parse_mode="HTML",
                    reply_markup=keyboard,
                )
                return edit_message_id
            else:
                sent = bot.send_message(
                    chat_id, content, parse_mode="HTML", reply_markup=keyboard
                )
                return sent.message_id
        except Exception as exc:
            log.warning("Console delivery failed: %s", exc)
            return None

    def _stop_live_console(chat_id: int, panel: str) -> None:
        with live_console_lock:
            job = live_console_jobs.pop((chat_id, panel), None)
        if job:
            job[0].set()

    def _live_console_loop(chat_id: int, panel: str, message_id: int) -> None:
        stop_event = threading.Event()
        with live_console_lock:
            previous = live_console_jobs.get((chat_id, panel))
            if previous:
                previous[0].set()
            live_console_jobs[(chat_id, panel)] = (stop_event, message_id)
        while not stop_event.wait(5):
            if _send_console(chat_id, panel, message_id) is None:
                break
        with live_console_lock:
            current = live_console_jobs.get((chat_id, panel))
            if current and current[0] is stop_event:
                live_console_jobs.pop((chat_id, panel), None)

    def _start_live_console(chat_id: int, panel: str) -> None:
        _stop_live_console(chat_id, panel)
        message_id = _send_console(chat_id, panel)
        if message_id is None:
            return
        threading.Thread(
            target=_live_console_loop,
            args=(chat_id, panel, message_id),
            daemon=True,
            name=f"{panel}-live-console",
        ).start()

    def _send_admin_panel_on_startup() -> None:
        for admin_id in ADMIN_IDS:
            try:
                bot.send_message(
                    admin_id,
                    "✅ Bot চালু হয়েছে। নিচের keyboard থেকে Admin Panel খুলুন।",
                    reply_markup=_main_keyboard(admin_id),
                )
            except Exception as exc:
                log.info("Could not send startup keyboard to admin %s: %s", admin_id, exc)

    @bot.message_handler(commands=["start"])
    def command_start(message: types.Message) -> None:
        _save_user(message)
        bot.send_message(
            message.chat.id,
            "🤖 <b>OTP Panel Bot</b>\n\n"
            "🔴 P1 / 🔵 P2 Console — live traffic\n"
            "📞 P1 / 📞 P2 নাম্বার — রেঞ্জ থেকে নাম্বার\n"
            "🔍 নাম্বার চেকার — WhatsApp check\n"
            "❌ WA Checker — phone pairing\n\n"
            "⚡ OTP এলে private inbox-এ পৌঁছাবে।",
            reply_markup=_main_keyboard(message.chat.id),
        )

    @bot.message_handler(func=lambda message: message.text == "🛠 Admin Panel")
    def admin_panel_button(message: types.Message) -> None:
        _save_user(message)
        if not _is_admin(message.from_user.id):
            return
        bot.send_message(
            message.chat.id,
            "🛠 <b>Admin Panel</b>\n"
            "এখানকার সব action button দিয়ে করা যাবে।\n"
            "Group delivery private inbox-এর পাশাপাশি চালু/বন্ধ করুন।",
            reply_markup=_admin_panel_keyboard(),
        )

    @bot.message_handler(commands=["setgroup", "delgroup", "setbot", "setchannel", "toggle_group"])
    def admin_settings(message: types.Message) -> None:
        _save_user(message)
        if not _is_admin(message.from_user.id):
            return
        parts = message.text.split(maxsplit=1)
        command = parts[0].lower()
        argument = parts[1].strip() if len(parts) > 1 else ""
        if command == "/toggle_group":
            with state_lock:
                state["group_enabled"] = not bool(state.get("group_enabled"))
                enabled = state["group_enabled"]
            _save_state()
            bot.reply_to(message, f"✅ Group messaging: {'ON' if enabled else 'OFF'}")
            return
        if command == "/setgroup" and argument:
            if not re.fullmatch(r"-?\d+", argument):
                bot.reply_to(message, "❌ Group ID অবশ্যই একটি integer হতে হবে।")
                return
            group_id = int(argument)
            _save_group_and_send_history(group_id)
            bot.reply_to(message, f"✅ Group added: <code>{group_id}</code>")
            return
        if command == "/setgroup" and not argument:
            chat = getattr(message, "chat", None)
            chat_type = str(getattr(chat, "type", "") or "")
            if chat_type in {"group", "supergroup"} and getattr(chat, "id", None) is not None:
                group_id = int(chat.id)
                _save_group_and_send_history(group_id)
                bot.reply_to(
                    message,
                    f"✅ এই group সেট করা হয়েছে: <code>{group_id}</code>",
                )
                return
        if command == "/delgroup" and argument:
            if not re.fullmatch(r"-?\d+", argument):
                bot.reply_to(message, "❌ Group ID অবশ্যই একটি integer হতে হবে।")
                return
            group_id = int(argument)
            with state_lock:
                state["group_ids"] = [item for item in _group_ids() if item != group_id]
            _save_state()
            bot.reply_to(message, f"✅ Group removed: <code>{group_id}</code>")
            return
        if command in {"/setbot", "/setchannel"} and _valid_url(argument):
            with state_lock:
                state["number_bot_url" if command == "/setbot" else "main_channel_url"] = argument
            _save_state()
            bot.reply_to(message, "✅ URL updated.")
            return
        bot.reply_to(message, "ব্যবহার: /setgroup <id>, /delgroup <id>, /toggle_group, /setbot <url>, /setchannel <url>")

    def _send_stats(chat_id: int) -> None:
        with state_lock:
            snapshot = {
                user_id: {"total": item["total"], "services": dict(item["services"])}
                for user_id, item in otp_stats.items()
            }
        if not snapshot:
            bot.send_message(chat_id, "📊 এখনো কোনো OTP রিসিভ হয়নি।")
            return
        lines = ["📊 <b>OTP Statistics</b>", "━━━━━━━━━━━━━━━━━━━━━"]
        grand_total = 0
        for user_id, record in sorted(snapshot.items(), key=lambda item: -item[1]["total"]):
            grand_total += record["total"]
            service_counts = " | ".join(
                f"{SERVICE_ICONS.get(name, '📲')} {count}"
                for name, count in sorted(record["services"].items())
            )
            lines.extend(
                [
                    f"👤 User: {_label_for(user_id)} (ID: <code>{user_id}</code>)",
                    f"📊 Total: {record['total']} OTPs",
                    service_counts or "📲 Unknown: 0",
                    "",
                ]
            )
        lines.append(f"━━━━━━━━━━━━━━━━━━━━━\n📋 Grand total: <b>{grand_total}</b> OTPs")
        bot.send_message(chat_id, "\n".join(lines), disable_web_page_preview=True)

    def _send_status(chat_id: int) -> None:
        with registry_locks["p1"]:
            p1_count = len(registries["p1"])
        with registry_locks["p2"]:
            p2_count = len(registries["p2"])
        with state_lock:
            groups = _group_ids()
            enabled = state.get("group_enabled")
            total = sum(item["total"] for item in otp_stats.values())
        uptime = int(time.time() - bot_started_at)
        bot.send_message(
            chat_id,
            "🖥 <b>Bot Status</b>\n━━━━━━━━━━━━━━━━━━━━━\n"
            f"⏱ Uptime: <code>{uptime // 3600}h {(uptime % 3600) // 60}m</code>\n"
            f"👁 P1 watching: <code>{p1_count}</code>\n"
            f"👁 P2 watching: <code>{p2_count}</code>\n"
            f"👥 Groups: <code>{len(groups)}</code> ({'ON' if enabled else 'OFF'})\n"
            f"📨 Total OTPs: <code>{total}</code>",
        )

    @bot.message_handler(commands=["stats"])
    def command_stats(message: types.Message) -> None:
        _save_user(message)
        if _is_admin(message.from_user.id):
            _send_stats(message.chat.id)

    @bot.message_handler(commands=["status"])
    def command_status(message: types.Message) -> None:
        _save_user(message)
        if _is_admin(message.from_user.id):
            _send_status(message.chat.id)

    @bot.message_handler(func=lambda message: message.text in {"🔴 P1 Console", "🔵 P2 Console"})
    def console_button(message: types.Message) -> None:
        _save_user(message)
        panel = "p1" if message.text.startswith("🔴") else "p2"
        threading.Thread(target=_start_live_console, args=(message.chat.id, panel), daemon=True).start()

    @bot.message_handler(func=lambda message: message.text in {"📞 P1 নাম্বার", "📞 P2 নাম্বার"})
    def number_button(message: types.Message) -> None:
        _save_user(message)
        _set_mode(message.chat.id, "range_p1" if "P1" in message.text else "range_p2")
        bot.send_message(message.chat.id, "📝 রেঞ্জ লিখুন (যেমন: <code>22501XXX</code>)")

    @bot.message_handler(func=lambda message: message.text in {"✅ WA Checker", "❌ WA Checker"})
    def whatsapp_button(message: types.Message) -> None:
        _save_user(message)
        if wa_statuses.get(message.chat.id) == "connected":
            bot.send_message(message.chat.id, "✅ WhatsApp ইতিমধ্যে সংযুক্ত।", reply_markup=_main_keyboard(message.chat.id))
            return
        _set_mode(message.chat.id, "wa_phone")
        bot.send_message(message.chat.id, "📱 দেশের কোডসহ WhatsApp নম্বর দিন (যেমন: <code>+8801712345678</code>)")

    @bot.message_handler(func=lambda message: message.text == "🔌 WA ডিসকানেক্ট")
    def disconnect_button(message: types.Message) -> None:
        _save_user(message)
        threading.Thread(target=disconnect_whatsapp, args=(message.chat.id,), daemon=True).start()

    @bot.message_handler(func=lambda message: message.text == "🔍 নাম্বার চেকার")
    def checker_button(message: types.Message) -> None:
        _save_user(message)
        if wa_statuses.get(message.chat.id) != "connected":
            bot.send_message(message.chat.id, "❌ আগে WhatsApp সংযুক্ত করুন।", reply_markup=_main_keyboard(message.chat.id))
            return
        _set_mode(message.chat.id, "check_numbers")
        bot.send_message(message.chat.id, "🔍 প্রতি লাইনে একটি করে সর্বোচ্চ ২০টি নাম্বার পাঠান।")

    @bot.message_handler(content_types=["text"])
    def text_handler(message: types.Message) -> None:
        _save_user(message)
        if message.text.startswith("/") or message.text in {
            "🔴 P1 Console", "🔵 P2 Console", "📞 P1 নাম্বার", "📞 P2 নাম্বার",
            "✅ WA Checker", "❌ WA Checker", "🔌 WA ডিসকানেক্ট", "🔍 নাম্বার চেকার",
            "🛠 Admin Panel",
        }:
            return
        with state_lock:
            mode = user_modes.get(message.chat.id, {}).get("mode", "idle")
            user_modes[message.chat.id] = {"mode": "idle"}
        if mode == "range_p1" or mode == "range_p2":
            panel = "p1" if mode == "range_p1" else "p2"
            threading.Thread(target=_fetch_numbers, args=(message.chat.id, panel, message.text.strip()), daemon=True).start()
        elif mode == "wa_phone":
            phone = re.sub(r"[\s-]", "", message.text.strip())
            if not re.fullmatch(r"\+?\d{7,15}", phone):
                bot.send_message(message.chat.id, "❌ নম্বরটি সঠিক নয়।")
                return
            if not phone.startswith("+"):
                phone = "+" + phone
            bot.send_message(message.chat.id, "⏳ WhatsApp pairing code তৈরি হচ্ছে...")
            threading.Thread(target=connect_whatsapp, args=(message.chat.id, phone), daemon=True).start()
        elif mode == "check_numbers":
            numbers = [line.strip() for line in message.text.splitlines() if line.strip()][:20]
            result = _wa_check(message.chat.id, numbers)
            lines = ["🔍 <b>নাম্বার চেকার ফলাফল:</b>", ""]
            for number, is_on in result.items():
                status = "🔴 WhatsApp আছে" if is_on is True else "🟢 WhatsApp নেই" if is_on is False else "⬜ চেক হয়নি"
                lines.append(f"<code>{html.escape(number)}</code> — {status}")
            bot.send_message(message.chat.id, "\n".join(lines))
        elif mode in {"admin_setgroup", "admin_delgroup"}:
            if not _is_admin(message.from_user.id):
                return
            if not re.fullmatch(r"-?\d+", message.text.strip()):
                bot.send_message(message.chat.id, "❌ Group ID অবশ্যই একটি integer হতে হবে।")
                return
            group_id = int(message.text.strip())
            with state_lock:
                groups = _group_ids()
                if mode == "admin_setgroup" and group_id not in groups:
                    groups.append(group_id)
                if mode == "admin_delgroup":
                    groups = [item for item in groups if item != group_id]
                state["group_ids"] = groups
            if mode == "admin_setgroup":
                _save_group_and_send_history(group_id)
            else:
                _save_state()
            bot.send_message(
                message.chat.id,
                f"✅ Group {'added' if mode == 'admin_setgroup' else 'removed'}: "
                f"<code>{group_id}</code>",
                reply_markup=_admin_panel_keyboard(),
            )
        elif mode in {"admin_setbot", "admin_setchannel"}:
            if not _is_admin(message.from_user.id):
                return
            value = message.text.strip()
            if not _valid_url(value):
                bot.send_message(message.chat.id, "❌ একটি valid http/https URL দিন।")
                return
            key = "number_bot_url" if mode == "admin_setbot" else "main_channel_url"
            with state_lock:
                state[key] = value
            _save_state()
            bot.send_message(
                message.chat.id,
                "✅ URL updated.",
                reply_markup=_admin_panel_keyboard(),
            )

    @bot.callback_query_handler(func=lambda call: True)
    def callback_handler(call: types.CallbackQuery) -> None:
        data = call.data or ""
        chat_id = call.message.chat.id
        if data.startswith("admin|"):
            if not _is_admin(call.from_user.id):
                bot.answer_callback_query(call.id, "Admin only")
                return
            action = data.split("|", 1)[1]
            if action == "setgroup":
                _set_mode(chat_id, "admin_setgroup")
                bot.send_message(chat_id, "➕ Group ID পাঠান (যেমন: <code>-1001234567890</code>)")
            elif action == "delgroup":
                _set_mode(chat_id, "admin_delgroup")
                bot.send_message(chat_id, "➖ যে Group ID মুছবেন সেটি পাঠান।")
            elif action == "setbot":
                _set_mode(chat_id, "admin_setbot")
                bot.send_message(chat_id, "🔗 Number Bot-এর নতুন http/https URL পাঠান।")
            elif action == "setchannel":
                _set_mode(chat_id, "admin_setchannel")
                bot.send_message(chat_id, "📣 Main Channel-এর নতুন http/https URL পাঠান।")
            elif action == "toggle_group":
                with state_lock:
                    state["group_enabled"] = not bool(state.get("group_enabled"))
                    enabled = state["group_enabled"]
                _save_state()
                try:
                    bot.edit_message_reply_markup(
                        chat_id,
                        call.message.message_id,
                        reply_markup=_admin_panel_keyboard(),
                    )
                except Exception:
                    pass
                bot.send_message(chat_id, f"✅ Group messaging: {'ON' if enabled else 'OFF'}")
            elif action == "stats":
                _send_stats(chat_id)
            elif action == "status":
                _send_status(chat_id)
            elif action == "refresh":
                try:
                    bot.edit_message_reply_markup(
                        chat_id,
                        call.message.message_id,
                        reply_markup=_admin_panel_keyboard(),
                    )
                except Exception:
                    pass
            bot.answer_callback_query(call.id)
            return
        if data.startswith("cr|"):
            panel = data.split("|", 1)[1]
            threading.Thread(
                target=_send_console,
                args=(chat_id, panel, call.message.message_id),
                daemon=True,
                name=f"{panel}-console-refresh",
            ).start()
        elif data.startswith("stop|"):
            panel = data.split("|", 1)[1]
            _stop_live_console(chat_id, panel)
            bot.send_message(chat_id, f"⏸ {panel.upper()} live view stopped.")
        elif data.startswith("nb|"):
            parts = data.split("|", 2)
            if len(parts) == 3 and parts[1] in {"p1", "p2"}:
                bot.answer_callback_query(call.id, "নতুন নাম্বার আনা হচ্ছে...")
                threading.Thread(
                    target=_fetch_numbers,
                    args=(
                        chat_id,
                        parts[1],
                        parts[2],
                        call.message.message_id,
                    ),
                    daemon=True,
                    name=f"{parts[1]}-number-refresh",
                ).start()
                return
        elif data == "cb":
            try:
                bot.delete_message(chat_id, call.message.message_id)
            except Exception:
                pass
        try:
            bot.answer_callback_query(call.id)
        except Exception:
            pass


def main() -> None:
    _load_state()
    log.info("OTP Panel Bot starting. Admin IDs: %s", sorted(ADMIN_IDS))

    if not BOT_TOKEN:
        log.error("CRITICAL: TELEGRAM_BOT_TOKEN is not set. Please set TELEGRAM_BOT_TOKEN in Railway Variables to run the bot.")
        while True:
            time.sleep(3600)

    _send_admin_panel_on_startup()
    for panel in ("p1", "p2"):
        threading.Thread(
            target=_poll_provider,
            args=(panel,),
            daemon=True,
            name=f"{panel}-global-poller",
        ).start()
    threading.Thread(
        target=_augustel_poller,
        daemon=True,
        name="augustel-poller",
    ).start()
    bot.infinity_polling(timeout=30, long_polling_timeout=20)


if __name__ == "__main__":
    main()
