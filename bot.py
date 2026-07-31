"""
OTP Panel Bot  v5.0  — ULTRA FAST
=========================================================
P1  : WealthoraPrime    → success-otp     (1s global poll)
P2  : FastXOTPs        → success-otp-info (1s global poll)
WA  : neonize – PairPhone code (no QR), per-user session
Admin : ADMIN_ID → /stats  /status
=========================================================
✅  Global P2 poller  — 1 টা thread, সব user-এর জন্য, 1s interval
✅  Global P1 poller  — 1 টা thread, সব user-এর জন্য, 1s interval
✅  70–80 concurrent user support
✅  OTP panel-এ আসার ≤1s এ user পায়
✅  নম্বর fetch — 6টা parallel (fast)
✅  WA Checker unchanged (working)
"""

import os
import re
import time
import threading
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed

import telebot
from telebot import types
from neonize.client import NewClient
from neonize.events import ConnectedEv, DisconnectedEv

# ─────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────
BOT_TOKEN = (
    os.environ.get("WA_CHECKER_BOT_TOKEN")
    or os.environ.get("TELEGRAM_BOT_TOKEN", "")
)
ADMIN_ID = int(os.environ.get("ADMIN_ID", "8523774444"))

P1_BASE = os.environ.get(
    "WEALTHORA_API_BASE",
    "https://api.2oo9.cloud/MXS47FLFX0U/tnevs/@public/api",
)
P1_KEY  = os.environ.get("WEALTHORA_API_KEY", "MWFG9WNAHZQ")
P1_HDRS = {"mauthapi": P1_KEY}

P2_BASE = os.environ.get("FASTXOTPS_API_BASE", "https://fastxotps.com")
P2_KEY  = os.environ.get("FASTXOTPS_API_KEY", "MURAD_69548E938AF8F1D4E0587220")
P2_HDRS = {"X-API-Key": P2_KEY, "Content-Type": "application/json"}

if not BOT_TOKEN:
    raise SystemExit("❌  TELEGRAM_BOT_TOKEN env var is required.")

logging.basicConfig(level=logging.WARNING)
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)
bot = telebot.TeleBot(BOT_TOKEN, parse_mode="Markdown",
                      num_threads=80)   # 70-80 user handle করতে

# ─────────────────────────────────────────────────────────────
#  HTTP SESSIONS
# ─────────────────────────────────────────────────────────────
def _make_session(pool_conn=50, pool_max=100, retries=2,
                  force_list=(502, 503, 504)) -> requests.Session:
    s = requests.Session()
    retry = Retry(total=retries, backoff_factor=0.2,
                  status_forcelist=list(force_list))
    adapter = HTTPAdapter(pool_connections=pool_conn,
                          pool_maxsize=pool_max,
                          max_retries=retry)
    s.mount("http://",  adapter)
    s.mount("https://", adapter)
    return s

# General session (P1 + misc)
_session = _make_session(pool_conn=50, pool_max=100)

# P2 session — no retry, fast fail
_p2_sess  = _make_session(pool_conn=10, pool_max=20,
                           retries=0, force_list=())

def _get(url, params=None, headers=None, timeout=8):
    try:
        return _session.get(url, params=params, headers=headers,
                            timeout=timeout).json()
    except Exception as e:
        return {"error": str(e)}

def _post(url, data=None, headers=None, timeout=8):
    try:
        return _session.post(url, json=data or {}, headers=headers,
                             timeout=timeout).json()
    except Exception as e:
        return {"error": str(e)}

def p1_get(path, params=None):
    return _get(f"{P1_BASE}{path}", params=params, headers=P1_HDRS)

def p1_post(path, data=None):
    return _post(f"{P1_BASE}{path}", data=data, headers=P1_HDRS)

def p2_post(path, data=None):
    return _post(f"{P2_BASE}/api{path}", data=data or {}, headers=P2_HDRS)

# ─────────────────────────────────────────────────────────────
#  GLOBAL STATE
# ─────────────────────────────────────────────────────────────
wa_clients  = {}   # chat_id → NewClient
wa_statuses = {}   # chat_id → "disconnected"|"connecting"|"connected"
otp_stats   = {}   # chat_id → int
bot_start   = time.time()

user_names  = {}
names_lock  = threading.Lock()

user_state  = {}
state_lock  = threading.Lock()

active_watches: dict = {}
watch_lock = threading.Lock()

DEFAULT_SERVICES = {"whatsapp", "facebook", "telegram"}
SVC_ICON = {"whatsapp": "💬", "facebook": "📘", "telegram": "✈️"}

# ─────────────────────────────────────────────────────────────
#  GLOBAL OTP WATCH REGISTRIES
#  প্রতিটি entry: plain_number → info dict
#  info = {chat_id, full, country, service, range_id, deadline, panel}
# ─────────────────────────────────────────────────────────────
_p1_registry: dict = {}   # plain → info
_p2_registry: dict = {}   # plain → info
_p1_reg_lock = threading.Lock()
_p2_reg_lock = threading.Lock()

def _register_numbers(panel: str, results: list, range_id: str,
                      chat_id: int, duration: int = 600):
    """নম্বরগুলো global poller registry-তে add করে।"""
    deadline = time.time() + duration
    lock = _p1_reg_lock if panel == "p1" else _p2_reg_lock
    reg  = _p1_registry  if panel == "p1" else _p2_registry
    with lock:
        for n in results:
            plain = n["plain"].lstrip("+")
            reg[plain] = {
                "chat_id":  chat_id,
                "full":     n["full"],
                "country":  n.get("country", ""),
                "service":  n.get("service", ""),
                "range_id": range_id,
                "deadline": deadline,
            }

def _unregister(panel: str, plains: set):
    lock = _p1_reg_lock if panel == "p1" else _p2_reg_lock
    reg  = _p1_registry  if panel == "p1" else _p2_registry
    with lock:
        for p in plains:
            reg.pop(p, None)

# ─────────────────────────────────────────────────────────────
#  USERNAME / UTILITY HELPERS
# ─────────────────────────────────────────────────────────────
def _save_user(msg_or_user):
    try:
        u = getattr(msg_or_user, "from_user", msg_or_user) or msg_or_user
        if u is None: return
        uid = u.id
        if u.username:
            label = f"@{u.username}"
        elif u.first_name:
            label = u.first_name + (f" {u.last_name}" if u.last_name else "")
        else:
            label = str(uid)
        with names_lock:
            user_names[uid] = label
    except Exception:
        pass

def _get_label(chat_id: int) -> str:
    with names_lock:
        return user_names.get(chat_id, str(chat_id))

def time_ago(ms: float) -> str:
    s = max(0, int(time.time() - ms / 1000))
    if s < 60:   return f"{s}s ago"
    if s < 3600: return f"{s // 60}m ago"
    return f"{s // 3600}h ago"

def uptime_str() -> str:
    s = int(time.time() - bot_start)
    h, m = divmod(s // 60, 60)
    return f"{h}h {m}m"

def extract_otp(text: str) -> str:
    m = re.search(r"\b(\d{3}[- ]\d{3})\b", text)
    if m: return m.group(1).replace(" ", "-")
    m = re.search(r"\b(\d{4,7})\b", text)
    if m: return m.group(1)
    return "???"

def safe_delete(chat_id, msg_id):
    try: bot.delete_message(chat_id, msg_id)
    except: pass

def edit_safe(chat_id, msg_id, text, kb=None):
    try:
        if kb:
            bot.edit_message_text(text, chat_id, msg_id,
                                  reply_markup=kb, parse_mode="Markdown")
        else:
            bot.edit_message_text(text, chat_id, msg_id, parse_mode="Markdown")
    except: pass

def _inc_otp(chat_id):
    otp_stats[chat_id] = otp_stats.get(chat_id, 0) + 1

def get_wa_status(chat_id) -> str:
    return wa_statuses.get(chat_id, "disconnected")

# ─── flags ────────────────────────────────────────────────────
_FLAG = {
    "ivory coast":"🇨🇮","cameroon":"🇨🇲","madagascar":"🇲🇬","nigeria":"🇳🇬",
    "ghana":"🇬🇭","kenya":"🇰🇪","ethiopia":"🇪🇹","tanzania":"🇹🇿","uganda":"🇺🇬",
    "senegal":"🇸🇳","mali":"🇲🇱","burkina faso":"🇧🇫","guinea":"🇬🇳","togo":"🇹🇬",
    "benin":"🇧🇯","niger":"🇳🇪","chad":"🇹🇩","angola":"🇦🇴","mozambique":"🇲🇿",
    "zambia":"🇿🇲","zimbabwe":"🇿🇼","botswana":"🇧🇼","namibia":"🇳🇦",
    "south africa":"🇿🇦","rwanda":"🇷🇼","burundi":"🇧🇮","congo":"🇨🇬",
    "dr congo":"🇨🇩","gabon":"🇬🇦","malawi":"🇲🇼","mauritius":"🇲🇺",
    "cape verde":"🇨🇻","sierra leone":"🇸🇱","eritrea":"🇪🇷","somalia":"🇸🇴",
    "mauritania":"🇲🇷","egypt":"🇪🇬","morocco":"🇲🇦","algeria":"🇩🇿",
    "tunisia":"🇹🇳","libya":"🇱🇾","india":"🇮🇳","pakistan":"🇵🇰","bangladesh":"🇧🇩",
    "indonesia":"🇮🇩","philippines":"🇵🇭","vietnam":"🇻🇳","thailand":"🇹🇭",
    "malaysia":"🇲🇾","myanmar":"🇲🇲","cambodia":"🇰🇭","sri lanka":"🇱🇰",
    "nepal":"🇳🇵","ukraine":"🇺🇦","russia":"🇷🇺","brazil":"🇧🇷","argentina":"🇦🇷",
    "colombia":"🇨🇴","mexico":"🇲🇽","peru":"🇵🇪","chile":"🇨🇱","venezuela":"🇻🇪",
    "united states":"🇺🇸","united kingdom":"🇬🇧","france":"🇫🇷","germany":"🇩🇪",
    "spain":"🇪🇸","china":"🇨🇳","japan":"🇯🇵","south korea":"🇰🇷",
    "saudi arabia":"🇸🇦","turkey":"🇹🇷","iran":"🇮🇷","iraq":"🇮🇶","afghanistan":"🇦🇫",
}
def _flag(c: str) -> str:
    return _FLAG.get((c or "").strip().lower(), "🌍")

def _resolve_service(sid="", service="", range_id="") -> str:
    for raw in [sid, service]:
        v = (raw or "").strip().lower()
        if not v: continue
        if "facebook" in v or v == "fb":  return "Facebook"
        if "telegram" in v or v == "tg":  return "Telegram"
        if "whatsapp" in v or v == "wa":  return "WhatsApp"
    r = (range_id or "").lower()
    if "fb" in r or "facebook" in r: return "Facebook"
    if "tg" in r or "telegram" in r: return "Telegram"
    return "WhatsApp"

def _svc_icon(svc: str) -> str:
    return SVC_ICON.get(svc.lower(), "📲")

def _num_matches(api_num: str, watch: set) -> bool:
    a = api_num.strip().lstrip("+")
    for w in watch:
        wc = w.strip().lstrip("+")
        if a == wc: return True
        sl = min(len(a), len(wc), 9)
        if sl >= 7 and a[-sl:] == wc[-sl:]: return True
    return False

# ─────────────────────────────────────────────────────────────
#  OTP NOTIFICATION
# ─────────────────────────────────────────────────────────────
def _notify_otp(chat_id, full_num, otp_code,
                country="", service="", range_id=""):
    _inc_otp(chat_id)
    svc_name = _resolve_service(service=service, range_id=range_id)
    icon     = _svc_icon(svc_name)
    ctry     = (country or "Unknown").title()
    header   = (f"{_flag(country)}|`{full_num}`| "
                f"{icon} {svc_name} 🌍COUNTRY: {ctry}")
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton(
        f"🔑  {otp_code}",
        copy_text=types.CopyTextButton(text=otp_code)))
    try:
        bot.send_message(chat_id, header,
                         reply_markup=kb, parse_mode="Markdown")
    except Exception as e:
        print(f"[OTP-NOTIFY] {e}")

# ─────────────────────────────────────────────────────────────
#  GLOBAL P2 POLLER  — একটাই thread, সব user-এর জন্য
#  interval: 1 second  |  endpoint: /api/success-otp-info
# ─────────────────────────────────────────────────────────────
def _fetch_p2_otps() -> list:
    try:
        r = _p2_sess.get(
            f"{P2_BASE}/api/success-otp-info",
            params={"api_key": P2_KEY},
            timeout=5,
        )
        if r.status_code != 200:
            return []
        data = r.json().get("data", {}) or {}
        return data.get("otps", []) or []
    except Exception as e:
        print(f"[P2-FETCH] {e}")
        return []

def _global_p2_poller():
    """Bot start হওয়ার সময় যেসব OTP ইতিমধ্যে আছে সেগুলো skip করি।"""
    seen: set = {str(o.get("otp_id","")) for o in _fetch_p2_otps()
                 if o.get("otp_id")}
    print(f"[P2-POLLER] শুরু। pre-seen={len(seen)}")

    while True:
        time.sleep(1)          # ← 1 সেকেন্ড interval
        try:
            with _p2_reg_lock:
                if not _p2_registry:
                    continue   # কোনো active user নেই → API call বাঁচাও
                # expired entries সরাও
                now = time.time()
                for k in [k for k, v in _p2_registry.items()
                           if v["deadline"] < now]:
                    del _p2_registry[k]

            for o in _fetch_p2_otps():
                oid = str(o.get("otp_id") or "")
                if not oid or oid in seen:
                    continue
                seen.add(oid)

                api_num = str(o.get("number", "")).strip().lstrip("+")
                otp_val = str(o.get("otp") or "")
                msg_txt = str(o.get("message") or "")
                code    = otp_val or extract_otp(msg_txt)
                if not code or code == "???":
                    continue

                with _p2_reg_lock:
                    for plain, info in list(_p2_registry.items()):
                        if _num_matches(api_num, {plain}):
                            print(f"[P2-POLLER] ✅ {info['full']} otp={code}")
                            threading.Thread(
                                target=_notify_otp,
                                args=(info["chat_id"],),
                                kwargs=dict(
                                    full_num=info["full"],
                                    otp_code=code,
                                    country=info["country"],
                                    service=info["service"],
                                    range_id=info["range_id"],
                                ),
                                daemon=True,
                            ).start()
                            del _p2_registry[plain]
                            break

        except Exception as e:
            print(f"[P2-POLLER] err: {e}")

# ─────────────────────────────────────────────────────────────
#  GLOBAL P1 POLLER  — একটাই thread, সব user-এর জন্য
#  interval: 1 second  |  endpoint: /success-otp
# ─────────────────────────────────────────────────────────────
def _fetch_p1_otps() -> list:
    try:
        resp = p1_get("/success-otp")
        otps = (resp.get("data") or {}).get("otps", [])
        return otps if isinstance(otps, list) else []
    except:
        return []

def _global_p1_poller():
    seen: set = {str(o.get("otp_id","")) for o in _fetch_p1_otps()
                 if o.get("otp_id")}
    print(f"[P1-POLLER] শুরু। pre-seen={len(seen)}")

    while True:
        time.sleep(1)          # ← 1 সেকেন্ড interval
        try:
            with _p1_reg_lock:
                if not _p1_registry:
                    continue
                now = time.time()
                for k in [k for k, v in _p1_registry.items()
                           if v["deadline"] < now]:
                    del _p1_registry[k]

            for o in _fetch_p1_otps():
                oid = str(o.get("otp_id") or "")
                if not oid or oid in seen:
                    continue
                seen.add(oid)

                api_num = str(o.get("number","")).strip()
                api_sid = str(o.get("sid") or o.get("service") or "")
                code    = extract_otp(str(o.get("message","")))
                if not code or code == "???":
                    continue

                with _p1_reg_lock:
                    for plain, info in list(_p1_registry.items()):
                        if _num_matches(api_num, {plain}):
                            print(f"[P1-POLLER] ✅ {info['full']} otp={code}")
                            threading.Thread(
                                target=_notify_otp,
                                args=(info["chat_id"],),
                                kwargs=dict(
                                    full_num=info["full"],
                                    otp_code=code,
                                    country=info["country"],
                                    service=info.get("service") or api_sid,
                                    range_id=info["range_id"],
                                ),
                                daemon=True,
                            ).start()
                            del _p1_registry[plain]
                            break

        except Exception as e:
            print(f"[P1-POLLER] err: {e}")

# ─────────────────────────────────────────────────────────────
#  PER-USER WHATSAPP CLIENT
# ─────────────────────────────────────────────────────────────
def _session_path(chat_id: int) -> str:
    return f"wa_session_{chat_id}"

def _clear_session(chat_id: int):
    base = _session_path(chat_id)
    for ext in ("", ".db", ".db-shm", ".db-wal"):
        fp = base + ext
        if os.path.exists(fp):
            try: os.remove(fp)
            except: pass

def _build_wa_client(chat_id: int) -> NewClient:
    c = NewClient(_session_path(chat_id))

    @c.event(ConnectedEv)
    def _on_conn(client, event):
        wa_statuses[chat_id] = "connected"
        print(f"[WA] ✅ connected: {chat_id}")
        try:
            bot.send_message(
                chat_id,
                "✅ *WhatsApp সংযুক্ত হয়েছে!*\n\nএখন নম্বর চেক করা যাবে।",
                reply_markup=_main_kb(chat_id),
            )
        except: pass

    @c.event(DisconnectedEv)
    def _on_disc(client, event):
        wa_statuses[chat_id] = "disconnected"
        print(f"[WA] ⚠️ disconnected: {chat_id}")
        time.sleep(5)
        if os.path.exists(_session_path(chat_id) + ".db"):
            threading.Thread(
                target=_reconnect_silent, args=(chat_id,), daemon=True).start()

    return c

def _reconnect_silent(chat_id: int):
    if get_wa_status(chat_id) != "disconnected": return
    wa_statuses[chat_id] = "connecting"
    client = _build_wa_client(chat_id)
    wa_clients[chat_id] = client
    try:
        client.connect()
    except Exception as e:
        wa_statuses[chat_id] = "disconnected"
        print(f"[WA] reconnect fail {chat_id}: {e}")

# ─────────────────────────────────────────────────────────────
#  PAIR PHONE  — neonize 0.4.x  — TESTED & CONFIRMED WORKING
# ─────────────────────────────────────────────────────────────
def connect_with_code(chat_id: int, phone: str):
    if get_wa_status(chat_id) == "connected":
        bot.send_message(chat_id, "✅ *WhatsApp ইতিমধ্যে সংযুক্ত!*",
                         reply_markup=_main_kb(chat_id))
        return
    if get_wa_status(chat_id) == "connecting":
        bot.send_message(chat_id, "⏳ কোড তৈরি হচ্ছে, একটু অপেক্ষা করুন...")
        return

    _clear_session(chat_id)
    wa_statuses[chat_id] = "connecting"
    client = _build_wa_client(chat_id)
    wa_clients[chat_id] = client

    @client.qr
    def _on_qr(cl, qr_data):
        try:
            code = cl.PairPhone(phone, False)
            bot.send_message(
                chat_id,
                f"🔑 *WhatsApp Pairing Code:*\n\n`{code}`\n\n"
                "WhatsApp এ যান:\n"
                "⚙️ *Settings → Linked Devices → Link with Phone Number*\n\n"
                "এই ৮-digit কোড দিন।\n"
                "⏳ দেওয়ার পর স্বয়ংক্রিয়ভাবে সংযুক্ত হবে।",
                parse_mode="Markdown",
            )
        except Exception as e:
            wa_statuses[chat_id] = "disconnected"
            wa_clients.pop(chat_id, None)
            bot.send_message(
                chat_id,
                f"❌ সংযোগ ব্যর্থ:\n`{e}`\n\nআবার *❌ WA Checker* চাপুন।",
                reply_markup=_main_kb(chat_id),
                parse_mode="Markdown",
            )

    threading.Thread(target=client.connect, daemon=True).start()


def disconnect_wa(chat_id: int):
    client = wa_clients.pop(chat_id, None)
    if client:
        try: client.disconnect()
        except: pass
    wa_statuses[chat_id] = "disconnected"
    _clear_session(chat_id)
    bot.send_message(chat_id, "✅ WhatsApp সংযোগ বিচ্ছিন্ন হয়েছে।",
                     reply_markup=_main_kb(chat_id))

# ─────────────────────────────────────────────────────────────
#  KEYBOARDS
# ─────────────────────────────────────────────────────────────
def _main_kb(chat_id=None):
    st = get_wa_status(chat_id) if chat_id else "disconnected"
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    kb.row(types.KeyboardButton("🔴 P1 Console"),
           types.KeyboardButton("🔵 P2 Console"))
    kb.row(types.KeyboardButton("📞 P1 নাম্বার"),
           types.KeyboardButton("📞 P2 নাম্বার"))
    kb.add(types.KeyboardButton("🔍 নাম্বার চেকার"))
    if st == "connected":
        kb.row(types.KeyboardButton("✅ WA Checker"),
               types.KeyboardButton("🔌 WA ডিসকানেক্ট"))
    else:
        kb.add(types.KeyboardButton("❌ WA Checker"))
    return kb

def _console_kb(panel: str):
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton(
        "🔄 Refresh", callback_data=f"cr|{panel}"))
    return kb

def _number_kb(numbers, wa_res, panel, range_id):
    kb = types.InlineKeyboardMarkup(row_width=1)
    for n in numbers:
        v    = wa_res.get(n["full"])
        icon = "🔴" if v is True else ("🟢" if v is False else "⬜")
        kb.add(types.InlineKeyboardButton(
            f"{icon}  {n['full']}",
            copy_text=types.CopyTextButton(text=n["full"])))
    kb.row(
        types.InlineKeyboardButton(
            "🔄 নাম্বার চেঞ্জ", callback_data=f"nb|{panel}|{range_id}"),
        types.InlineKeyboardButton(
            "❌ বন্ধ", callback_data="cb"),
    )
    return kb

def _card_header(range_id, panel, count, failed=0, service=""):
    label    = "P1" if panel == "p1" else "P2"
    svc_name = service or _resolve_service(range_id=range_id)
    icon     = _svc_icon(svc_name)
    h = f"{icon} *{svc_name.upper()}* [{label}]  —  {count}টি নাম্বার"
    if failed: h += f"  _(⚠️ {failed} মিস)_"
    h += "\n⏳ _OTP আসলে আপনার inbox-এ দেখাবে_"
    return h

# ─────────────────────────────────────────────────────────────
#  CONSOLE
# ─────────────────────────────────────────────────────────────
def _fmt_p1(hits):
    groups = {}
    for h in hits:
        sid = str(h.get("sid", "")).lower()
        if sid in DEFAULT_SERVICES:
            groups.setdefault(sid, []).append(h)
    if not groups: return "⚠️ কোনো লাইভ ডেটা নেই।"
    lines = []
    for sid in ("whatsapp", "facebook", "telegram"):
        if sid not in groups: continue
        lines.append(f"\n━━━━━━━━━━━━━━━━━\n"
                     f"{SVC_ICON.get(sid,'📲')} *{sid.capitalize()}*")
        for h in groups[sid][:10]:
            lines.append(f"`{h.get('range','')}` — "
                         f"_{time_ago(h.get('time', time.time()*1000))}_")
    lines.append(f"\n━━━━━━━━━━━━━━━━━\n🔄 _{time.strftime('%H:%M:%S')}_")
    return "\n".join(lines)

def _fmt_p2(services):
    order = {"whatsapp":0,"facebook":1,"telegram":2}
    filt  = sorted(
        [s for s in services if str(s.get("sid","")).lower() in DEFAULT_SERVICES],
        key=lambda s: order.get(str(s.get("sid","")).lower(), 9))
    if not filt: return "⚠️ কোনো লাইভ ডেটা নেই।"
    lines = []
    for svc in filt:
        sid = str(svc.get("sid","")).lower()
        ago = time_ago(svc.get("last_at", time.time()*1000))
        lines.append(f"\n━━━━━━━━━━━━━━━━━\n"
                     f"{SVC_ICON.get(sid,'📲')} *{sid.capitalize()}* — _{ago}_")
        for r in svc.get("ranges",[])[:8]:
            lines.append(f"  `{r}`")
    lines.append(f"\n━━━━━━━━━━━━━━━━━\n🔄 _{time.strftime('%H:%M:%S')}_")
    return "\n".join(lines)

def _send_console(chat_id, panel, edit_id=None):
    if panel == "p1":
        resp = p1_get("/console")
        hits = resp.get("data", {}).get("hits", [])
        text = "🔴 *P1 — WealthoraPrime*\n" + _fmt_p1(hits)
    else:
        resp = p2_post("/liveaccess")
        svcs = resp.get("services", [])
        text = "🔵 *P2 — FastXOTPs*\n" + _fmt_p2(svcs)
    kb = _console_kb(panel)
    if edit_id:
        try:
            bot.edit_message_text(text, chat_id, edit_id,
                                  reply_markup=kb, parse_mode="Markdown")
            return
        except: pass
    bot.send_message(chat_id, text, reply_markup=kb, parse_mode="Markdown")

# ─────────────────────────────────────────────────────────────
#  FETCH NUMBERS  (6টা parallel — fast)
# ─────────────────────────────────────────────────────────────
def _fetch_one(panel: str, range_id: str):
    if panel == "p1":
        resp    = p1_post("/getnum", {"range": range_id})
        code    = resp.get("meta", {}).get("code")
        data    = resp.get("data", {}) or {}
        full    = str(data.get("full_number", ""))
        plain   = str(data.get("no_plus_number") or full.lstrip("+"))
        country = str(data.get("country") or "")
        service = str(data.get("service") or data.get("sid") or "")
        if full and code == 200:
            return dict(full=full, plain=plain, country=country,
                        service=service, rid=None,
                        otp_now=False, otp_msg="")
        return None

    resp    = p2_post("/getnum", {"range": range_id})
    data    = resp.get("data", resp) or {}
    if not isinstance(data, dict): data = {}

    full    = (data.get("full_number") or data.get("number")
               or resp.get("full_number") or resp.get("number") or "")
    plain   = str(data.get("no_plus_number") or str(full).lstrip("+"))
    country = str(data.get("country") or resp.get("country") or "")
    service = str(data.get("service") or data.get("sid")
                  or resp.get("service") or resp.get("sid") or "")
    rid     = str(resp.get("rid") or data.get("rid") or "")
    otp_now = bool(data.get("otp_now") or resp.get("otp_now"))
    otp_msg = str(data.get("otp_message") or data.get("message")
                  or resp.get("otp_message") or resp.get("message") or "")

    print(f"[P2-FETCH] full={full} rid={rid} otp_now={otp_now}")

    if full:
        return dict(full=full, plain=plain, country=country,
                    service=service, rid=rid,
                    otp_now=otp_now, otp_msg=otp_msg)
    return None

def get_6_numbers(chat_id, panel, range_id, edit_msg_id=None):
    label   = "P1" if panel == "p1" else "P2"
    loading = f"⏳ `{range_id}` [{label}] থেকে নাম্বার আনা হচ্ছে..."

    if edit_msg_id:
        edit_safe(chat_id, edit_msg_id, loading)
        st_id = edit_msg_id
    else:
        st    = bot.send_message(chat_id, loading, parse_mode="Markdown")
        st_id = st.message_id

    results, failed = [], 0
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = [ex.submit(_fetch_one, panel, range_id) for _ in range(6)]
        for f in as_completed(futs):
            info = f.result()
            if info: results.append(info)
            else:    failed += 1

    if not results:
        err = (f"❌ `{range_id}` [{label}] থেকে নাম্বার পাওয়া যায়নি।\n"
               "_রেঞ্জ খালি বা API error।_")
        if edit_msg_id: edit_safe(chat_id, st_id, err)
        else:
            safe_delete(chat_id, st_id)
            bot.send_message(chat_id, err, parse_mode="Markdown")
        return

    batch_svc = results[0].get("service","") if results else ""
    wa_res    = _wa_bulk(chat_id, results)
    header    = _card_header(range_id, panel, len(results),
                              failed, service=batch_svc)
    kb        = _number_kb(results, wa_res, panel, range_id)

    if edit_msg_id:
        try:
            bot.edit_message_text(header, chat_id, st_id,
                                  reply_markup=kb, parse_mode="Markdown")
        except:
            bot.send_message(chat_id, header,
                             reply_markup=kb, parse_mode="Markdown")
    else:
        safe_delete(chat_id, st_id)
        bot.send_message(chat_id, header,
                         reply_markup=kb, parse_mode="Markdown")

    # active_watches update (cleanup reference)
    plains = {n["plain"].lstrip("+") for n in results}
    with watch_lock:
        existing = active_watches.get(chat_id, set())
        active_watches[chat_id] = existing | plains

    def _cleanup():
        time.sleep(605)
        with watch_lock:
            cur = active_watches.get(chat_id, set())
            active_watches[chat_id] = cur - plains
    threading.Thread(target=_cleanup, daemon=True).start()

    # ── Global registry-তে register করো ────────────────────────
    _register_numbers(panel, results, range_id, chat_id, duration=600)

    # P2: otp_now=True হলে সাথে সাথে notify করো
    if panel == "p2":
        for n in results:
            if n.get("otp_now") and n.get("otp_msg"):
                code = extract_otp(n["otp_msg"])
                if code and code != "???":
                    _notify_otp(
                        chat_id,
                        full_num=n["full"],
                        otp_code=code,
                        country=n.get("country",""),
                        service=n.get("service",""),
                        range_id=range_id,
                    )
                    # registry থেকে সরাও (ইতিমধ্যে deliver হয়ে গেছে)
                    with _p2_reg_lock:
                        _p2_registry.pop(n["plain"].lstrip("+"), None)

# ─────────────────────────────────────────────────────────────
#  WA CHECK
# ─────────────────────────────────────────────────────────────
def _wa_bulk(chat_id, numbers):
    result = {n["full"]: None for n in numbers}
    client = wa_clients.get(chat_id)
    if not client or get_wa_status(chat_id) != "connected": return result
    try:
        cleaned   = [n["plain"].lstrip("+") for n in numbers]
        jids      = [f"+{c}@s.whatsapp.net" for c in cleaned]
        responses = client.is_on_whatsapp(*jids)
        for i, n in enumerate(numbers):
            matched = next(
                (r for r in responses if cleaned[i] in r.Query), None)
            result[n["full"]] = (
                bool(matched.IsIn) if matched
                else (bool(responses[i].IsIn)
                      if i < len(responses) else None))
    except Exception as e:
        print(f"[WA-BULK] {e}")
    return result

def _wa_check(chat_id, numbers):
    result  = {n: None for n in numbers}
    client  = wa_clients.get(chat_id)
    if not client or get_wa_status(chat_id) != "connected": return result
    cleaned = [n.replace("+","").replace(" ","").replace("-","")
               for n in numbers]
    jids    = [f"+{c}@s.whatsapp.net" for c in cleaned]
    try:
        responses = client.is_on_whatsapp(*jids)
        for i, n in enumerate(numbers):
            matched = next(
                (r for r in responses if cleaned[i] in r.Query), None)
            result[n] = (
                bool(matched.IsIn) if matched
                else (bool(responses[i].IsIn)
                      if i < len(responses) else None))
    except Exception as e:
        print(f"[WA-CHECK] {e}")
    return result

# ─────────────────────────────────────────────────────────────
#  BUTTON LABELS
# ─────────────────────────────────────────────────────────────
_ALL_BTN = {
    "🔴 P1 Console","🔵 P2 Console",
    "📞 P1 নাম্বার","📞 P2 নাম্বার",
    "🔍 নাম্বার চেকার",
    "✅ WA Checker","❌ WA Checker",
    "🔌 WA ডিসকানেক্ট",
}

# ─────────────────────────────────────────────────────────────
#  /start
# ─────────────────────────────────────────────────────────────
@bot.message_handler(commands=["start"])
def cmd_start(msg):
    _save_user(msg)
    bot.send_message(
        msg.chat.id,
        "🤖 *OTP Panel Bot v5*\n\n"
        "🔴 P1 / 🔵 P2 Console — লাইভ ট্র্যাফিক\n"
        "📞 P1 / 📞 P2 নাম্বার — রেঞ্জ → ৬টি নাম্বার\n"
        "🔍 নাম্বার চেকার — WhatsApp check\n"
        "❌ WA Checker — Phone code দিয়ে লগইন (QR নেই)\n"
        "🔌 WA ডিসকানেক্ট — সংযোগ বন্ধ\n\n"
        "⚡ OTP আসামাত্র ≤১ সেকেন্ডে পাবেন",
        reply_markup=_main_kb(msg.chat.id),
    )

# ─────────────────────────────────────────────────────────────
#  /stats  (admin only)
# ─────────────────────────────────────────────────────────────
@bot.message_handler(commands=["stats"])
def cmd_stats(msg):
    if msg.chat.id != ADMIN_ID: return
    if not otp_stats:
        bot.send_message(msg.chat.id, "📊 এখনো কোনো OTP রিসিভ হয়নি।")
        return
    text  = "📊 *OTP Statistics*\n━━━━━━━━━━━━━━━━━\n"
    total = 0
    for uid, cnt in sorted(otp_stats.items(), key=lambda x: -x[1]):
        with names_lock:
            uname = user_names.get(uid, "")
        if uname.startswith("@"):
            link = f"[{uname}](tg://user?id={uid})"
        else:
            link = f"[{uname or uid}](tg://user?id={uid})"
        text  += f"👤 {link} — *{cnt}* OTP\n"
        total += cnt
    text += f"━━━━━━━━━━━━━━━━━\n📋 মোট: *{total}* OTP"
    bot.send_message(msg.chat.id, text, parse_mode="Markdown",
                     disable_web_page_preview=True)

# ─────────────────────────────────────────────────────────────
#  /status  (admin only)
# ─────────────────────────────────────────────────────────────
@bot.message_handler(commands=["status"])
def cmd_status(msg):
    if msg.chat.id != ADMIN_ID: return
    connected  = [cid for cid, st in wa_statuses.items() if st == "connected"]
    connecting = [cid for cid, st in wa_statuses.items() if st == "connecting"]
    with watch_lock:
        watching = sum(len(v) for v in active_watches.values())
    with _p2_reg_lock:
        p2_active = len(_p2_registry)
    with _p1_reg_lock:
        p1_active = len(_p1_registry)
    total_otp = sum(otp_stats.values()) if otp_stats else 0
    text = (
        "🖥 *Bot Status  v5*\n━━━━━━━━━━━━━━━━━\n"
        f"⏱ Uptime: `{uptime_str()}`\n"
        f"👥 WA Connected: `{len(connected)}`\n"
        f"🔄 WA Connecting: `{len(connecting)}`\n"
        f"👁 Active watches: `{watching}`\n"
        f"🔵 P2 watching: `{p2_active}` নম্বর\n"
        f"🔴 P1 watching: `{p1_active}` নম্বর\n"
        f"📨 Total OTPs: `{total_otp}`\n"
        f"━━━━━━━━━━━━━━━━━\n"
    )
    if connected:
        text += "*Connected users:*\n"
        for cid in connected:
            cnt   = otp_stats.get(cid, 0)
            label = _get_label(cid)
            text += f"  • `{cid}` ({label}) — {cnt} OTP\n"
    bot.send_message(msg.chat.id, text, parse_mode="Markdown",
                     disable_web_page_preview=True)

# ─────────────────────────────────────────────────────────────
#  CONSOLE BUTTONS
# ─────────────────────────────────────────────────────────────
@bot.message_handler(func=lambda m: m.text == "🔴 P1 Console")
def btn_p1_console(msg):
    _save_user(msg)
    threading.Thread(target=_send_console,
                     args=(msg.chat.id,"p1"), daemon=True).start()

@bot.message_handler(func=lambda m: m.text == "🔵 P2 Console")
def btn_p2_console(msg):
    _save_user(msg)
    threading.Thread(target=_send_console,
                     args=(msg.chat.id,"p2"), daemon=True).start()

# ─────────────────────────────────────────────────────────────
#  NUMBER BUTTONS
# ─────────────────────────────────────────────────────────────
@bot.message_handler(func=lambda m: m.text == "📞 P1 নাম্বার")
def btn_p1_num(msg):
    _save_user(msg)
    with state_lock:
        user_state[msg.chat.id] = {"mode": "wait_range_p1"}
    bot.send_message(msg.chat.id,
                     "📝 *P1* রেঞ্জ লিখুন  (যেমন: `22501XXX`)",
                     parse_mode="Markdown")

@bot.message_handler(func=lambda m: m.text == "📞 P2 নাম্বার")
def btn_p2_num(msg):
    _save_user(msg)
    with state_lock:
        user_state[msg.chat.id] = {"mode": "wait_range_p2"}
    bot.send_message(msg.chat.id,
                     "📝 *P2* রেঞ্জ লিখুন  (যেমন: `26134XXX`)",
                     parse_mode="Markdown")

# ─────────────────────────────────────────────────────────────
#  ❌ / ✅ WA CHECKER BUTTON
# ─────────────────────────────────────────────────────────────
@bot.message_handler(
    func=lambda m: m.text in ("✅ WA Checker", "❌ WA Checker"))
def btn_wa(msg):
    _save_user(msg)
    chat_id = msg.chat.id
    status  = get_wa_status(chat_id)
    if status == "connected":
        bot.send_message(chat_id, "✅ *WhatsApp ইতিমধ্যে সংযুক্ত!*",
                         reply_markup=_main_kb(chat_id))
        return
    if status == "connecting":
        bot.send_message(chat_id, "⏳ কোড তৈরি হচ্ছে, একটু অপেক্ষা করুন...")
        return
    with state_lock:
        user_state[chat_id] = {"mode": "wait_phone"}
    bot.send_message(
        chat_id,
        "📱 *WhatsApp নম্বর* দিন (দেশের কোড সহ)\n\n"
        "উদাহরণ: `+8801712345678`",
        parse_mode="Markdown",
    )

# ─────────────────────────────────────────────────────────────
#  🔌 DISCONNECT BUTTON
# ─────────────────────────────────────────────────────────────
@bot.message_handler(func=lambda m: m.text == "🔌 WA ডিসকানেক্ট")
def btn_disconnect(msg):
    _save_user(msg)
    threading.Thread(target=disconnect_wa,
                     args=(msg.chat.id,), daemon=True).start()

# ─────────────────────────────────────────────────────────────
#  🔍 NUMBER CHECKER
# ─────────────────────────────────────────────────────────────
@bot.message_handler(func=lambda m: m.text == "🔍 নাম্বার চেকার")
def btn_checker(msg):
    _save_user(msg)
    chat_id = msg.chat.id
    if get_wa_status(chat_id) != "connected":
        bot.send_message(chat_id,
                         "❌ *WhatsApp সংযুক্ত নেই।*\n\n"
                         "প্রথমে *❌ WA Checker* চাপুন।",
                         reply_markup=_main_kb(chat_id))
        return
    with state_lock:
        user_state[chat_id] = {"mode": "wait_check_numbers"}
    bot.send_message(chat_id,
                     "🔍 *নাম্বার চেকার*\n\n"
                     "📞 নাম্বার পাঠান (প্রতি লাইনে একটি, সর্বোচ্চ ২০টি):",
                     parse_mode="Markdown")

# ─────────────────────────────────────────────────────────────
#  FREE TEXT
# ─────────────────────────────────────────────────────────────
@bot.message_handler(func=lambda m: True, content_types=["text"])
def on_text(msg):
    _save_user(msg)
    chat_id = msg.chat.id
    text    = msg.text.strip()
    if text.startswith("/") or text in _ALL_BTN: return

    with state_lock:
        mode = user_state.get(chat_id, {}).get("mode", "idle")

    # ── wait_phone ──────────────────────────────────────────────
    if mode == "wait_phone":
        with state_lock:
            user_state[chat_id] = {"mode": "idle"}
        phone = text.replace(" ", "").replace("-", "")
        if not phone.startswith("+"):
            phone = "+" + phone
        if not re.match(r"^\+\d{7,15}$", phone):
            bot.send_message(
                chat_id,
                "❌ *নম্বর সঠিক নয়।*\n\nদেশের কোড সহ দিন।\nউদাহরণ: `+8801712345678`",
                parse_mode="Markdown",
            )
            return
        bot.send_message(chat_id,
                         f"⏳ `{phone}` এর জন্য pairing code তৈরি হচ্ছে...",
                         parse_mode="Markdown")
        threading.Thread(
            target=connect_with_code, args=(chat_id, phone), daemon=True
        ).start()

    # ── wait_range_p1 ───────────────────────────────────────────
    elif mode == "wait_range_p1":
        with state_lock:
            user_state[chat_id] = {"mode": "idle"}
        threading.Thread(
            target=get_6_numbers, args=(chat_id, "p1", text), daemon=True
        ).start()

    # ── wait_range_p2 ───────────────────────────────────────────
    elif mode == "wait_range_p2":
        with state_lock:
            user_state[chat_id] = {"mode": "idle"}
        threading.Thread(
            target=get_6_numbers, args=(chat_id, "p2", text), daemon=True
        ).start()

    # ── wait_check_numbers ──────────────────────────────────────
    elif mode == "wait_check_numbers":
        with state_lock:
            user_state[chat_id] = {"mode": "idle"}
        lines = [l.strip() for l in text.splitlines() if l.strip()][:20]
        if not lines:
            bot.send_message(chat_id, "❌ কোনো নাম্বার পাওয়া যায়নি।")
            return
        loading = bot.send_message(
            chat_id, f"⏳ {len(lines)}টি নাম্বার চেক হচ্ছে...", parse_mode="Markdown"
        )
        def _do_check():
            results = _wa_check(chat_id, lines)
            out = []
            for n, is_on in results.items():
                if is_on is True:   icon = "🔴 WhatsApp আছে"
                elif is_on is False: icon = "🟢 WhatsApp নেই"
                else:               icon = "⬜ চেক হয়নি"
                out.append(f"`{n}` — {icon}")
            safe_delete(chat_id, loading.message_id)
            bot.send_message(
                chat_id,
                "🔍 *নাম্বার চেকার ফলাফল:*\n\n" + "\n".join(out),
                parse_mode="Markdown",
            )
        threading.Thread(target=_do_check, daemon=True).start()

# ─────────────────────────────────────────────────────────────
#  CALLBACK QUERY HANDLER
# ─────────────────────────────────────────────────────────────
@bot.callback_query_handler(func=lambda c: True)
def on_callback(call):
    _save_user(call.from_user)
    data    = call.data
    chat_id = call.message.chat.id
    msg_id  = call.message.message_id

    if data == "cb":
        safe_delete(chat_id, msg_id)
        bot.answer_callback_query(call.id)
        return

    if data.startswith("cr|"):
        panel = data.split("|")[1]
        bot.answer_callback_query(call.id, "🔄 Refreshing...")
        threading.Thread(
            target=_send_console, args=(chat_id, panel, msg_id), daemon=True
        ).start()
        return

    if data.startswith("nb|"):
        parts    = data.split("|", 2)
        panel    = parts[1]
        range_id = parts[2]
        bot.answer_callback_query(call.id, "🔄 নতুন নাম্বার আনা হচ্ছে...")
        threading.Thread(
            target=get_6_numbers,
            args=(chat_id, panel, range_id, msg_id),
            daemon=True,
        ).start()
        return

    bot.answer_callback_query(call.id)

# ─────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"🤖 OTP Panel Bot v5.0 চালু হচ্ছে... (admin={ADMIN_ID})")

    # Global pollers — daemon thread, একটাই প্রতিটার জন্য
    threading.Thread(target=_global_p2_poller, daemon=True,
                     name="P2-GlobalPoller").start()
    threading.Thread(target=_global_p1_poller, daemon=True,
                     name="P1-GlobalPoller").start()

    bot.infinity_polling(timeout=30, long_polling_timeout=20)
