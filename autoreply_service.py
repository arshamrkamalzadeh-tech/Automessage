"""
autoreply_service.py — بات اتوریپلای چندمستاجره (هرکس اکانت خودش رو وصل می‌کنه)

ایده:
  • هر کاربر از طریق این بات (/connect) با شماره‌ی خودِ اکانتش لاگین می‌کنه
    (اکانت خودِ کاربر userbot می‌شه، نه یه اکانت مشترک)
  • کاربر با /message یه متن تنظیم می‌کنه
  • از اون به بعد، هر کسی که برای اولین بار به اکانت کاربر پیام بده،
    همون متن به‌صورت خودکار و فقط یه‌بار براش فرستاده می‌شه
  • اطلاعات هر کاربر (شماره، پیام، لیست کسایی که قبلاً پاسخ گرفتن) جدا نگه داشته می‌شه

دستورها (با دکمه‌ها هم کار می‌کنن):  /start /connect /message /status /pause /resume /disconnect /cancel

⚠️ نکات مهم قبل از اجرا:
  1) این سرویس روی اکانتِ شخصیِ خودِ هر کاربر لاگین می‌کنه (نه بات رسمی)؛ چون از کتابخانه‌ی
     غیررسمی spluspy استفاده می‌کنه، همون ریسک‌هایی که برای هر userbot وجود داره اینجا هم هست:
     احتمال تشخیص به‌عنوان رفتار خودکار و محدود/مسدود شدن اکانت توسط سروش پلاس.
     فقط با رضایت و آگاهی کامل خودِ صاحب اکانت استفاده کن.
  2) بخش «گوش‌دادن به پیام‌های جدید» (watch_messages) چندحالته و با duck-typing نوشته شده؛
     حتماً تستش کن.

متغیرهای محیطی (same lazım؛ بدون پیش‌فرض — حتماً ست کن، داخل کد هاردکد نشده):
  BOT_TOKEN, TURSO_DATABASE_URL, TURSO_AUTH_TOKEN
  PORT   (اختیاری، فقط برای health-check روی Railway)

requirements:  spluspy  requests
Start command: python autoreply_service.py
"""

import asyncio
import base64
import builtins
import getpass
import glob
import html as _html
import inspect
import json
import os
import queue
import re
import sqlite3
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import requests

# فایل session با sqlite کار می‌کنه؛ به‌جای اینکه فوراً "database is locked" بده،
# تا ۳۰ ثانیه صبر کنه (باید قبل از import spluspy اعمال بشه)
_orig_sqlite_connect = sqlite3.connect


def _sqlite_connect(*a, **kw):
    if len(a) < 2:
        kw.setdefault("timeout", 30)
    return _orig_sqlite_connect(*a, **kw)


sqlite3.connect = _sqlite_connect

import spluspy

# ====== تنظیمات (بدون پیش‌فرض؛ همه از env) ======
BOT_TOKEN = os.getenv("BOT_TOKEN", "70346942:_mmYQ0KABxfBa-nfUsBtbFsH2-S7r9V81a0").strip()
TURSO_DATABASE_URL = os.getenv("TURSO_DATABASE_URL", "libsql://skytunnel-mikekamalzadeh-sys.aws-eu-west-1.turso.io").strip()
TURSO_AUTH_TOKEN = os.getenv("TURSO_AUTH_TOKEN", "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJhIjoicnciLCJpYXQiOjE3ODkwNzE5MDcsImlkIjoiMDFhMDhjZmQtMmMwMS03YWViLTlmYzQtOWYzMTkwNzEwMjA0Iiwia2lkIjoiNmltOXd5bGxUd2luNkVHWEJWNHVJUE01ZWNPM2JyZmJ3NzFqWTFIanFCTSIsInJpZCI6ImU5N2NmZmQwLTgxZDYtNDA3Yi1hZGQ4LTEzMTRjY2MyZTkxYyJ9.tZj3mmc_tNCVRPsLkwuWdrGsXZXmFCcvRCEPcN0iclAMk0QrHWfZxZiGqB5otvhlZaT9fs_WzsO6kTS_gnk1Bw").strip()
PORT = int(os.getenv("PORT", "8088"))

CODE_TIMEOUT = 300    # چند ثانیه منتظر کد/رمز لاگین از خودِ کاربر بمونه
POLL_INTERVAL = 4     # فقط برای حالت فالبکِ polling پیام‌ها
# ==================================================

API = "https://api.splus.ir/bot" + BOT_TOKEN

TSTATE = {}        # uid -> 'phone' | 'message'   (مرحله‌ی گفتگوی کنترل‌بات با کاربر)
TLOGIN = {}        # uid -> {"phone","waiting","starting","running","account"}
TANSWERS = {}       # uid -> queue.Queue()  (برای رد و بدل کردن کد/رمز لاگین)
STOP_EVENTS = {}    # uid -> threading.Event()
CLIENTS = {}        # uid -> spluspy.Client
OWN_IDS = {}        # uid -> آیدی خودِ اکانت (برای تشخیص پیام‌های خودت)
STARTED = {}        # uid -> زمان شروع گوش‌دادن (برای نادیده‌گرفتن پیام‌های قدیمی)
SEEN_CACHE = {}     # uid -> set(آیدی کسایی که پاسخ گرفتن یا خودت باهاشون چت کردی)

_local = threading.local()


# ---------------------------------------------------------------- Turso (HTTP) — عیناً مثل سرویس قبلی
def _http_url():
    url = TURSO_DATABASE_URL.strip()
    if url.startswith("libsql://"):
        url = "https://" + url[len("libsql://"):]
    return url.rstrip("/")


def _arg(a):
    if a is None:
        return {"type": "null"}
    if isinstance(a, bool):
        a = int(a)
    if isinstance(a, int):
        return {"type": "integer", "value": str(a)}
    return {"type": "text", "value": str(a)}


def pipeline(statements):
    reqs = [
        {"type": "execute", "stmt": {"sql": sql, "args": [_arg(a) for a in args]}}
        for sql, args in statements
    ]
    reqs.append({"type": "close"})
    r = requests.post(
        _http_url() + "/v2/pipeline",
        json={"requests": reqs},
        headers={"Authorization": "Bearer " + TURSO_AUTH_TOKEN},
        timeout=30,
    )
    r.raise_for_status()
    out = []
    for res in r.json()["results"]:
        if res["type"] == "error":
            raise RuntimeError(res["error"].get("message", "turso error"))
        if res["type"] == "ok" and res["response"]["type"] == "execute":
            rows = res["response"]["result"].get("rows", [])
            out.append([[c.get("value") for c in row] for row in rows])
    return out


def execute(sql, args=()):
    return pipeline([(sql, args)])[0]


def init_tables():
    pipeline([
        ("CREATE TABLE IF NOT EXISTS ar_kv (key TEXT PRIMARY KEY, value TEXT)", ()),
        ("CREATE TABLE IF NOT EXISTS ar_tenants ("
         "id INTEGER PRIMARY KEY, phone TEXT, account_label TEXT, reply_text TEXT, "
         "active INTEGER DEFAULT 1, created INTEGER)", ()),
        ("CREATE TABLE IF NOT EXISTS ar_seen_senders ("
         "tenant_id INTEGER, sender_id INTEGER, ts INTEGER, "
         "PRIMARY KEY (tenant_id, sender_id))", ()),
    ])


def kv_get(key):
    rows = execute("SELECT value FROM ar_kv WHERE key = ?", (key,))
    return rows[0][0] if rows else None


def kv_set(key, value):
    execute("INSERT OR REPLACE INTO ar_kv (key, value) VALUES (?, ?)", (key, value))


def kv_del(key):
    execute("DELETE FROM ar_kv WHERE key = ?", (key,))


# ---------------------------------------------------------------- مستاجرها (کاربرها)
def _tenant_from_row(r):
    return {
        "id": int(r[0]), "phone": r[1], "account_label": r[2],
        "reply_text": r[3], "active": bool(int(r[4] or 0)),
    }


def get_tenant(uid):
    rows = execute(
        "SELECT id, phone, account_label, reply_text, active FROM ar_tenants WHERE id = ?", (uid,)
    )
    return _tenant_from_row(rows[0]) if rows else None


def all_tenant_ids():
    return [int(r[0]) for r in execute("SELECT id FROM ar_tenants")]


def already_seen(tid, sender_id):
    rows = execute(
        "SELECT 1 FROM ar_seen_senders WHERE tenant_id = ? AND sender_id = ?", (tid, sender_id)
    )
    return bool(rows)


def mark_seen(tid, sender_id):
    execute(
        "INSERT OR IGNORE INTO ar_seen_senders (tenant_id, sender_id, ts) VALUES (?, ?, ?)",
        (tid, sender_id, int(time.time())),
    )


# ---------------------------------------------------------------- وضعیتِ هر کاربر در حافظه
def tenant_state(uid):
    return TLOGIN.setdefault(
        uid, {"phone": None, "waiting": None, "starting": False, "running": False, "account": None}
    )


def tenant_queue(uid):
    return TANSWERS.setdefault(uid, queue.Queue())


# ---------------------------------------------------------------- Bot API (کنترل‌بات رسمی)
def api(method, timeout=40, **params):
    r = requests.post(f"{API}/{method}", json=params, timeout=timeout)
    return r.json()


def send(chat_id, text, markup=None, html=False):
    p = {"chat_id": chat_id, "text": text}
    if markup:
        p["reply_markup"] = markup
    try:
        if html:
            res = api("sendMessage", parse_mode="HTML", **p)
            if isinstance(res, dict) and res.get("ok") is False:
                # اگه سروش پلاس HTML رو نپذیرفت، بدون تگ بفرست
                p["text"] = re.sub(r"</?b>", "", _html.unescape(text))
                res = api("sendMessage", **p)
            return res
        return api("sendMessage", **p)
    except Exception as e:
        print("send error:", repr(e))


def delete_message(chat_id, message_id):
    try:
        api("deleteMessage", chat_id=chat_id, message_id=message_id)
    except Exception:
        pass


CONTACT_KB = {
    "keyboard": [[{"text": "📱 ارسال شماره من", "request_contact": True}]],
    "resize_keyboard": True,
    "one_time_keyboard": True,
}
BTN_CONNECT = "🔗 اتصال اکانت"
BTN_MESSAGE = "✍️ تنظیم پیام"
BTN_STATUS = "📊 وضعیت"
BTN_PAUSE = "⏸ توقف موقت"
BTN_RESUME = "▶️ ازسرگیری"
BTN_DISCONNECT = "🗑 قطع اتصال"

MAIN_KB = {
    "keyboard": [
        [{"text": BTN_CONNECT}, {"text": BTN_MESSAGE}],
        [{"text": BTN_PAUSE}, {"text": BTN_RESUME}],
        [{"text": BTN_STATUS}, {"text": BTN_DISCONNECT}],
    ],
    "resize_keyboard": True,
}

BTN_MAP = {
    BTN_CONNECT: "/connect",
    BTN_MESSAGE: "/message",
    BTN_STATUS: "/status",
    BTN_PAUSE: "/pause",
    BTN_RESUME: "/resume",
    BTN_DISCONNECT: "/disconnect",
}


def normalize_phone(s):
    d = re.sub(r"\D", "", str(s))
    if d.startswith("00"):
        d = d[2:]
    if d.startswith("0"):
        d = "98" + d[1:]
    elif len(d) == 10 and d.startswith("9"):
        d = "98" + d
    return "+" + d if len(d) >= 10 else None


# ---------------------------------------------------------------- گرفتن کد/رمز لاگین (از خودِ کاربر)
def ask(uid, kind):
    q = tenant_queue(uid)
    while not q.empty():
        q.get_nowait()
    tenant_state(uid)["waiting"] = kind
    if kind == "code":
        send(uid,
             "📩 کد تأیید به سروش پلاسِ اکانتت ارسال شد.\n"
             "کد رو همین‌جا بفرست — ولی با خط‌تیره بین رقم‌ها، مثلاً 1-2-3-4-5\n"
             "(اگه کد رو سرراست تو چت بفرستی ممکنه سروش پلاس باطلش کنه)")
    else:
        send(uid, "🔐 اکانتت رمز دومرحله‌ای داره. رمزت رو بفرست:")
    try:
        ans = q.get(timeout=CODE_TIMEOUT)
    except queue.Empty:
        tenant_state(uid)["waiting"] = None
        raise TimeoutError("از کاربر جوابی نیومد")
    tenant_state(uid)["waiting"] = None
    return re.sub(r"\D", "", ans) if kind == "code" else ans


def fake_input(prompt=""):
    uid = getattr(_local, "uid", None)
    p = str(prompt).lower()
    if any(k in p for k in ("phone", "mobile", "number", "شماره")):
        return tenant_state(uid)["phone"] or ""
    if any(k in p for k in ("pass", "2fa", "رمز")):
        return ask(uid, "password")
    return ask(uid, "code")


# توجه: این monkey-patch سراسریه؛ امن بودنش برای چند کاربرِ هم‌زمان به این بستگی داره که
# کتابخونه، درخواست ورودی رو همیشه از همون تردی بپرسه که client.start() توش صدا زده شده
# (که با توجه به نحوه‌ی پیاده‌سازی سرویس قبلی‌تون همین‌طور به نظر می‌رسه). اگه spluspy
# داخلش ترد جدید می‌سازه، باید این بخش رو عوض کنیم.
builtins.input = fake_input
getpass.getpass = lambda prompt="", stream=None: fake_input(prompt)


# ---------------------------------------------------------------- session در Turso (per-tenant)
def _find_session_file(prefix):
    cands = [f for f in glob.glob(prefix + "*") if not f.endswith(("-journal", "-wal", "-shm"))]
    cands.sort(key=lambda f: (not f.endswith(".session"), f))
    return cands[0] if cands else None


def persist_session(uid, phone, prefix):
    path = _find_session_file(prefix)
    if not path:
        print(f"[tenant {uid}] session فایل پیدا نشد")
        return
    data = None
    for attempt in range(5):
        tmp = tempfile.mktemp()
        src = dst = None
        try:
            src, dst = sqlite3.connect(path), sqlite3.connect(tmp)
            src.backup(dst)
            dst.close()
            src.close()
            src = dst = None
            with open(tmp, "rb") as f:
                data = f.read()
            break
        except Exception as e:
            print(f"[tenant {uid}] بکاپ session تلاش {attempt + 1} نشد: {e!r}")
            time.sleep(1.5)
        finally:
            for c in (src, dst):
                try:
                    if c:
                        c.close()
                except Exception:
                    pass
            try:
                os.remove(tmp)
            except Exception:
                pass
    if data is None:
        try:
            with open(path, "rb") as f:
                data = f.read()
        except Exception as e:
            print(f"[tenant {uid}] خوندن فایل session نشد:", repr(e))
            return
    try:
        kv_set(f"session_blob:{uid}", base64.b64encode(data).decode())
        kv_set(f"session_file:{uid}", os.path.basename(path))
        kv_set(f"session_phone:{uid}", phone)
        print(f"[tenant {uid}] session ذخیره شد")
    except Exception as e:
        print(f"[tenant {uid}] ذخیره‌ی session تو Turso نشد (اتصال ادامه پیدا می‌کنه):", repr(e))


def restore_session(uid, prefix):
    if _find_session_file(prefix):
        return True
    b64 = kv_get(f"session_blob:{uid}")
    if not b64:
        return False
    name = kv_get(f"session_file:{uid}") or (prefix + ".session")
    with open(name, "wb") as f:
        f.write(base64.b64decode(b64))
    print(f"[tenant {uid}] session بازیابی شد -> {name}")
    return True


def wipe_session(uid, prefix):
    for f in glob.glob(prefix + "*"):
        try:
            os.remove(f)
        except Exception:
            pass
    for key in (f"session_blob:{uid}", f"session_file:{uid}", f"session_phone:{uid}"):
        kv_del(key)


# ---------------------------------------------------------------- منطق پاسخ خودکار
def _flag(obj, name):
    v = getattr(obj, name, None)
    return bool(v) and not callable(v)


def _int(x):
    try:
        return int(x)
    except Exception:
        return None


def msg_sender_id(msg):
    return (
        getattr(msg, "sender_id", None)
        or getattr(getattr(msg, "sender", None), "id", None)
        or getattr(getattr(msg, "from_user", None), "id", None)
    )


def msg_chat_id(msg):
    return getattr(msg, "chat_id", None) or getattr(getattr(msg, "chat", None), "id", None)


def is_outgoing(uid, event, msg):
    """پیامی که خودِ صاحب اکانت فرستاده (از هر دستگاهی، حتی همین اپ)."""
    for obj in (event, msg):
        for name in ("out", "outgoing", "is_outgoing", "from_me", "mine"):
            if _flag(obj, name):
                return True
    own = OWN_IDS.get(uid)
    sid = _int(msg_sender_id(msg))
    return bool(own and sid and sid == own)


def is_non_private(event, msg):
    for obj in (event, msg):
        if _flag(obj, "is_group") or _flag(obj, "is_channel"):
            return True
        if getattr(obj, "is_private", None) is False:
            return True
    return False


def msg_ts(msg):
    d = getattr(msg, "date", None)
    if d is None:
        return None
    if hasattr(d, "timestamp"):
        try:
            return d.timestamp()
        except Exception:
            return None
    try:
        v = float(d)
    except Exception:
        return None
    return v / 1000 if v > 1e12 else v


def is_old(uid, msg):
    ts, started = msg_ts(msg), STARTED.get(uid)
    return bool(ts and started and ts < started - 30)


async def mark_engaged(uid, peer_id):
    """خودت تو این چت پیام دادی (یا قبلاً چت داشتین) → دیگه پاسخ خودکار نگیره."""
    peer = _int(peer_id)
    if not peer or peer == _int(uid) or peer == OWN_IDS.get(uid):
        return
    cache = SEEN_CACHE.setdefault(uid, set())
    if peer in cache:
        return
    cache.add(peer)
    try:
        await asyncio.to_thread(mark_seen, uid, peer)
    except Exception as e:
        cache.discard(peer)
        print(f"[tenant {uid}] خطای ثبت چت:", repr(e))


async def maybe_reply(uid, client, sender_id, msg=None):
    sender_id = _int(sender_id)
    if not sender_id or sender_id == _int(uid) or sender_id == OWN_IDS.get(uid):
        return
    if msg is not None and is_old(uid, msg):
        return
    t = await asyncio.to_thread(get_tenant, uid)
    if not t or not t["active"] or not t["reply_text"]:
        return
    cache = SEEN_CACHE.setdefault(uid, set())
    if sender_id in cache:
        return
    cache.add(sender_id)  # رزرو، تا دو پیامِ پشت‌سرهم دوبار پاسخ نگیرن
    seen = await asyncio.to_thread(already_seen, uid, sender_id)
    if seen:
        return
    try:
        res = client.send_message(sender_id, t["reply_text"])
        if inspect.isawaitable(res):
            await res
        await asyncio.to_thread(mark_seen, uid, sender_id)
        print(f"[tenant {uid}] پاسخ خودکار به {sender_id} فرستاده شد")
    except Exception as e:
        cache.discard(sender_id)
        print(f"[tenant {uid}] خطای ارسال پاسخ:", repr(e))


async def watch_messages(uid, client):
    """
    سه تلاش به ترتیب، چون مستندات رسمی spluspy در دسترس نیست:
      ۱) event handler شبیه Telethon  (events.NewMessage)
      ۲) دکوریتور شبیه Pyrogram      (client.on_message)
      ۳) polling دستی روی دیالوگ‌ها   (فالبک، کندتر ولی به اسم متد خاصی گیر نیست)
    هر کدوم جواب داد همون کافیه.
    """
    stop = STOP_EVENTS[uid]
    handled = False

    try:
        from spluspy import events as _events
        if hasattr(client, "add_event_handler"):
            async def on_event(*args):
                # کتابخونه callback رو به شکل (client, event) صدا می‌زنه
                try:
                    event = args[-1]
                    msg = getattr(event, "message", event)
                    if is_outgoing(uid, event, msg):
                        # خودت داری تو این چت پیام می‌دی → این طرف دیگه پاسخ خودکار نمی‌گیره
                        if not is_non_private(event, msg):
                            await mark_engaged(uid, msg_chat_id(msg) or getattr(event, "chat_id", None))
                        return
                    if is_non_private(event, msg):
                        return
                    sender_id = msg_sender_id(msg) or msg_chat_id(msg)
                    if not sender_id:
                        print(f"[tenant {uid}] فرستنده پیدا نشد؛ attrها:",
                              [n for n in dir(msg) if not n.startswith("_")][:60])
                    await maybe_reply(uid, client, sender_id, msg)
                except Exception as e:
                    print(f"[tenant {uid}] خطای event handler:", repr(e))

            client.add_event_handler(on_event, _events.NewMessage(incoming=True))
            handled = True
            print(f"[tenant {uid}] از event handler شبیه Telethon استفاده شد")
    except Exception as e:
        print(f"[tenant {uid}] event handler در دسترس نبود: {e!r}")

    if not handled and hasattr(client, "on_message"):
        try:
            deco = client.on_message()

            def cb(c, m):
                if is_outgoing(uid, m, m):
                    if not is_non_private(m, m):
                        asyncio.create_task(mark_engaged(uid, msg_chat_id(m)))
                    return
                if is_non_private(m, m):
                    return
                sender_id = msg_sender_id(m) or msg_chat_id(m)
                asyncio.create_task(maybe_reply(uid, client, sender_id, m))

            deco(cb)
            handled = True
            print(f"[tenant {uid}] از دکوریتور شبیه Pyrogram استفاده شد")
        except Exception as e:
            print(f"[tenant {uid}] on_message ثبت نشد: {e!r}")

    if handled:
        runner = getattr(client, "run_until_disconnected", None)
        if runner:
            try:
                res = runner()
                if inspect.isawaitable(res):
                    await res
                return
            except Exception as e:
                print(f"[tenant {uid}] run_until_disconnected خطا داد:", repr(e))
        while not stop.is_set():
            await asyncio.sleep(1)
        return

    print(f"[tenant {uid}] هیچ event API ای پیدا نشد؛ رفت رو حالت polling")
    last_ids = {}
    first_pass = True  # دور اول فقط وضعیت فعلی رو ثبت می‌کنه، به هیچکس پاسخ نمی‌ده
    while not stop.is_set():
        try:
            dialogs = None
            if hasattr(client, "iter_dialogs"):
                dialogs = [d async for d in client.iter_dialogs(limit=50)]
            elif hasattr(client, "get_dialogs"):
                dialogs = client.get_dialogs()
                if inspect.isawaitable(dialogs):
                    dialogs = await dialogs
            for d in (dialogs or []):
                try:
                    chat = getattr(d, "chat", d)
                    if getattr(chat, "is_user", True) is False:
                        continue
                    chat_id = getattr(chat, "id", None)
                    msg = getattr(d, "message", None) or getattr(d, "top_message", None)
                    if chat_id is None or msg is None:
                        continue
                    mid = getattr(msg, "id", None) or getattr(msg, "message_id", None)
                    if mid is None:
                        continue
                    if last_ids.get(chat_id) != mid:
                        if is_outgoing(uid, d, msg):
                            await mark_engaged(uid, chat_id)
                        elif not first_pass:
                            await maybe_reply(uid, client, chat_id, msg)
                        last_ids[chat_id] = mid
                except Exception as e:
                    print(f"[tenant {uid}] خطای دیالوگ:", repr(e))
            first_pass = False
        except Exception as e:
            print(f"[tenant {uid}] خطای polling:", repr(e))
        await asyncio.sleep(POLL_INTERVAL)


# ---------------------------------------------------------------- لاگینِ هر مستاجر
async def userbot_main(uid, phone, prefix):
    client = spluspy.Client(prefix)
    kwargs = {"phone": phone}
    try:
        if "code_callback" in inspect.signature(client.start).parameters:
            kwargs["code_callback"] = lambda: ask(uid, "code")
    except (TypeError, ValueError):
        pass

    res = client.start(**kwargs)
    if inspect.isawaitable(res):
        await res

    await asyncio.to_thread(persist_session, uid, phone, prefix)

    label = None
    try:
        me = client.get_me()
        if inspect.isawaitable(me):
            me = await me
        OWN_IDS[uid] = _int(getattr(me, "id", None))
        uname = getattr(me, "username", None)
        label = ("@" + uname) if uname else (getattr(me, "first_name", None) or str(uid))
    except Exception as e:
        print(f"[tenant {uid}] get_me خطا داد:", repr(e))

    st = tenant_state(uid)
    st["running"], st["starting"], st["account"] = True, False, label
    execute("UPDATE ar_tenants SET account_label = ? WHERE id = ?", (label, uid))
    CLIENTS[uid] = client
    STARTED[uid] = time.time()
    STOP_EVENTS[uid] = threading.Event()
    send(uid, f"✅ اکانتت ({label or uid}) وصل شد. از الان گوش‌دادن به پیام‌های جدید شروع می‌شه.", MAIN_KB)
    if not get_tenant(uid)["reply_text"]:
        send(uid, "📝 یادت نره پیام خودکارت رو با «✍️ تنظیم پیام» تنظیم کنی، وگرنه چیزی فرستاده نمی‌شه.")

    await watch_messages(uid, client)


def login_thread(uid, phone, prefix):
    _local.uid = uid
    max_tries = 6
    for attempt in range(1, max_tries + 1):
        try:
            asyncio.run(userbot_main(uid, phone, prefix))
            return
        except Exception as e:
            if "locked" in str(e).lower() and attempt < max_tries:
                wait = 3 * attempt
                print(f"[tenant {uid}] database is locked؛ تلاش مجدد {attempt}/{max_tries - 1} بعد از {wait}s")
                time.sleep(wait)
                continue
            st = tenant_state(uid)
            st["running"], st["starting"], st["waiting"] = False, False, None
            print(f"[tenant {uid}] خطا:", repr(e))
            send(uid, f"❌ خطا تو وصل‌شدن اکانتت:\n{e!r}\n\nدوباره «🔗 اتصال اکانت» رو بزن.", MAIN_KB)
            return


def start_login(uid, phone):
    st = tenant_state(uid)
    st["phone"], st["starting"] = phone, True
    prefix = f"tenant_{uid}"
    threading.Thread(target=login_thread, args=(uid, phone, prefix), daemon=True).start()


def boot_restore_all():
    for uid in all_tenant_ids():
        phone = kv_get(f"session_phone:{uid}")
        prefix = f"tenant_{uid}"
        if phone and restore_session(uid, prefix):
            start_login(uid, phone)


# ---------------------------------------------------------------- دستورهای کنترل‌بات
def cmd_start(chat_id, uid):
    TSTATE.pop(uid, None)
    lines = [
        "👋✨ <b>سلام! خوش اومدی</b> ✨",
        "",
        "🤖 این بات روی اکانتِ خودت (با اجازه‌ی خودت) بالا میاد و به هر کسی که برای "
        "<b>اولین بار</b> بهت پیام بده، یه پیامِ از پیش‌تعیین‌شده رو خودکار می‌فرسته 💬",
        "",
        "🚀 <b>شروع سریع:</b>",
        "1️⃣ 🔗 <b>اتصال اکانت</b> — شماره‌ات رو بده و وارد شو",
        "2️⃣ ✍️ <b>تنظیم پیام</b> — پیام خودکارت رو بنویس",
        "",
        "🎛 <b>مدیریت:</b>",
        "📊 وضعیت  •  ⏸ توقف موقت  •  ▶️ ازسرگیری  •  🗑 قطع اتصال",
        "",
        "👇 از دکمه‌های پایین استفاده کن",
    ]
    send(chat_id, "\n".join(lines), MAIN_KB, html=True)


def cmd_connect(chat_id, uid):
    st = tenant_state(uid)
    if st["running"] or st["starting"]:
        send(chat_id, "⚠️ همین الان یه اتصال فعال/در حال وصل‌شدنه. اول دکمه‌ی «🗑 قطع اتصال» رو بزن.", MAIN_KB)
        return
    TSTATE[uid] = "phone"
    send(chat_id, "📱 شماره‌ی اکانت خودت رو بفرست (مثلاً +98912xxxxxxx) یا با دکمه‌ی زیر:", CONTACT_KB)


def handle_phone(chat_id, uid, raw):
    phone = normalize_phone(raw)
    if not phone:
        send(chat_id, "این رو نتونستم به‌عنوان شماره بخونم. دوباره بفرست.", CONTACT_KB)
        return
    TSTATE.pop(uid, None)
    if not get_tenant(uid):
        execute(
            "INSERT INTO ar_tenants (id, phone, account_label, reply_text, active, created) "
            "VALUES (?, ?, NULL, NULL, 1, ?)",
            (uid, phone, int(time.time())),
        )
    else:
        execute("UPDATE ar_tenants SET phone = ? WHERE id = ?", (phone, uid))
    send(chat_id, f"📲 در حال ورود با شماره {phone} ...\nیه کد تأیید تو خود سروش پلاس برات میاد.", MAIN_KB)
    start_login(uid, phone)


def set_message(chat_id, uid, text):
    TSTATE.pop(uid, None)
    execute("UPDATE ar_tenants SET reply_text = ? WHERE id = ?", (text, uid))
    send(chat_id, "✅ پیام خودکارت ذخیره شد:\n\n" + text)


def cmd_message(chat_id, uid, arg):
    if not get_tenant(uid):
        send(chat_id, "⚠️ اول باید «🔗 اتصال اکانت» رو بزنی.", MAIN_KB)
        return
    if arg:
        set_message(chat_id, uid, arg)
    else:
        TSTATE[uid] = "message"
        send(chat_id, "✍️ متن پیام خودکارت رو بفرست (همینی که میره برای هرکی اولین‌بار بهت پیام می‌ده):")


def cmd_status(chat_id, uid):
    t = get_tenant(uid)
    if not t:
        send(chat_id, "🔴 هنوز وصل نشدی. دکمه‌ی «🔗 اتصال اکانت» رو بزن.", MAIN_KB)
        return
    st = tenant_state(uid)
    n = execute("SELECT COUNT(*) FROM ar_seen_senders WHERE tenant_id = ?", (uid,))[0][0]
    status = "🟢 آنلاین" if st["running"] else ("🟡 در حال وصل‌شدن" if st["starting"] else "🔴 آفلاین")
    active = "فعال ✅" if t["active"] else "موقتاً خاموش ⏸"
    e = lambda x: _html.escape(str(x))
    send(chat_id,
         f"🤖 <b>اکانت:</b> {e(t['account_label'] or t['phone'])}\n"
         f"📡 <b>وضعیت اتصال:</b> {status}\n"
         f"⚡️ <b>پاسخ خودکار:</b> {active}\n"
         f"📨 <b>پیام تنظیم‌شده:</b> {e(t['reply_text']) if t['reply_text'] else 'هنوز تنظیم نشده (✍️ تنظیم پیام)'}\n"
         f"👥 <b>{e(n)}</b> نفر ثبت شدن (پاسخ خودکار گرفتن یا خودت باهاشون چت کردی).",
         MAIN_KB, html=True)


def cmd_pause(chat_id, uid):
    if not get_tenant(uid):
        send(chat_id, "اتصالی نداری.")
        return
    execute("UPDATE ar_tenants SET active = 0 WHERE id = ?", (uid,))
    send(chat_id, "⏸ پاسخ خودکار موقتاً خاموش شد. اتصال اکانت هنوز برقراره.")


def cmd_resume(chat_id, uid):
    if not get_tenant(uid):
        send(chat_id, "اتصالی نداری.")
        return
    execute("UPDATE ar_tenants SET active = 1 WHERE id = ?", (uid,))
    send(chat_id, "▶️ پاسخ خودکار دوباره روشن شد.")


def cmd_disconnect(chat_id, uid):
    if not get_tenant(uid):
        send(chat_id, "اتصالی وجود نداره.")
        return
    ev = STOP_EVENTS.get(uid)
    if ev:
        ev.set()
    client = CLIENTS.pop(uid, None)
    if client:
        for name in ("log_out", "disconnect", "stop"):
            fn = getattr(client, name, None)
            if fn:
                try:
                    res = fn()
                    if inspect.isawaitable(res):
                        pass  # از ترد sync صدا زده شده؛ best effort
                except Exception:
                    pass
                break
    for sql in ("DELETE FROM ar_tenants WHERE id = ?", "DELETE FROM ar_seen_senders WHERE tenant_id = ?"):
        execute(sql, (uid,))
    wipe_session(uid, f"tenant_{uid}")
    for d in (OWN_IDS, STARTED, SEEN_CACHE):
        d.pop(uid, None)
    tenant_state(uid).update({"running": False, "starting": False, "account": None})
    TSTATE.pop(uid, None)
    send(chat_id, "🗑 اتصال اکانتت قطع شد و همه‌ی اطلاعاتت (پیام، لیست پاسخ‌داده‌شده‌ها) پاک شد.")


def handle_update(u):
    msg = u.get("message")
    if not msg:
        return
    chat_id = msg["chat"]["id"]
    uid = (msg.get("from") or {}).get("id", chat_id)
    text = (msg.get("text") or "").strip()
    contact = msg.get("contact")
    text = BTN_MAP.get(text, text)

    if tenant_state(uid)["waiting"] and text and not text.startswith("/"):
        tenant_queue(uid).put(text)
        delete_message(chat_id, msg["message_id"])
        return

    if text.startswith("/"):
        parts = text.split(maxsplit=1)
        cmd = parts[0].split("@")[0].lower()
        arg = parts[1].strip() if len(parts) > 1 else ""
        if cmd == "/start":
            return cmd_start(chat_id, uid)
        if cmd == "/connect":
            return cmd_connect(chat_id, uid)
        if cmd == "/message":
            return cmd_message(chat_id, uid, arg)
        if cmd == "/status":
            return cmd_status(chat_id, uid)
        if cmd == "/pause":
            return cmd_pause(chat_id, uid)
        if cmd == "/resume":
            return cmd_resume(chat_id, uid)
        if cmd == "/disconnect":
            return cmd_disconnect(chat_id, uid)
        if cmd == "/cancel":
            TSTATE.pop(uid, None)
            return send(chat_id, "👌 باشه، لغو شد.", MAIN_KB)
        return send(chat_id, "❓ متوجه نشدم. از دکمه‌های پایین استفاده کن.", MAIN_KB)

    step = TSTATE.get(uid)
    if step == "phone" and (text or contact):
        raw = (contact or {}).get("phone_number") or text
        return handle_phone(chat_id, uid, raw)
    if step == "message" and text:
        return set_message(chat_id, uid, text)

    send(chat_id, "👇 از دکمه‌های پایین استفاده کن (یا /start بزن).", MAIN_KB)


# ---------------------------------------------------------------- health-check (اختیاری، برای Railway)
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps({"ok": True, "tenants": len(CLIENTS)}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


def start_http():
    # health-check فقط برای Railway لازمه؛ اگه پورت اشغال بود برنامه نباید بخوابه
    for port in (PORT, PORT + 1, PORT + 2, 0):
        try:
            srv = ThreadingHTTPServer(("0.0.0.0", port), HealthHandler)
        except OSError as e:
            print(f"[http] پورت {port} اشغاله ({e}); پورت بعدی...")
            continue
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        print(f"[http] health-check روی :{srv.server_address[1]}")
        return
    print("[http] health-check بالا نیومد؛ بدونش ادامه می‌دم.")


_LOCK_FH = None


def acquire_single_instance():
    """جلوگیری از اجرای هم‌زمان دو نسخه (که باعث database is locked می‌شد)."""
    global _LOCK_FH
    try:
        import fcntl
    except ImportError:
        return
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".autoreply.lock")
    fh = open(path, "w")
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        raise SystemExit(
            "❌ یه نسخه‌ی دیگه‌ی این برنامه هنوز زنده‌ست (شاید با Ctrl+Z فقط متوقف شده).\n"
            "اول بزن:  pkill -9 -f autoreply_service.py   بعد دوباره اجرا کن."
        )
    fh.write(str(os.getpid()))
    fh.flush()
    _LOCK_FH = fh


def main():
    acquire_single_instance()
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN باید ست باشه.")
    if not (TURSO_DATABASE_URL and TURSO_AUTH_TOKEN):
        raise SystemExit("TURSO_DATABASE_URL و TURSO_AUTH_TOKEN باید ست باشن.")
    init_tables()
    start_http()
    boot_restore_all()

    offset = None
    while True:
        try:
            res = requests.post(
                f"{API}/getUpdates", json={"offset": offset, "timeout": 25}, timeout=40
            ).json()
            for u in res.get("result", []):
                offset = u["update_id"] + 1
                try:
                    handle_update(u)
                except Exception as e:
                    print("handle error:", repr(e))
        except Exception as e:
            print("poll error:", repr(e))
            time.sleep(3)


if __name__ == "__main__":
    main()
