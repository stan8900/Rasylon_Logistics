import hashlib
import hmac
import json
import logging
import os
import re
import secrets
import base64
from datetime import datetime, timedelta
from typing import Any, Awaitable, Callable, Dict, List, Optional
from urllib.parse import parse_qsl

from aiohttp import web
from dotenv import load_dotenv

from app.runtime_config import BASE_DIR, create_storage_from_env


load_dotenv(BASE_DIR / ".env")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PAYMENT_AMOUNT = int(os.getenv("PAYMENT_AMOUNT", "100000"))
PAYMENT_CURRENCY = os.getenv("PAYMENT_CURRENCY", "UZS")
PAYMENT_DESCRIPTION = os.getenv("PAYMENT_DESCRIPTION", "Оплата услуг логистического бота")
PAYMENT_VALID_DAYS = int(os.getenv("PAYMENT_VALID_DAYS", "30"))
PAYMENT_CARD_TARGET = os.getenv("PAYMENT_CARD_TARGET", "9860 1701 1433 3116")
BOT_USERNAME = (os.getenv("BOT_USERNAME") or os.getenv("TELEGRAM_BOT_USERNAME") or "").lstrip("@")
SUPPORT_AGENT_USERNAME = os.getenv("SUPPORT_AGENT_USERNAME", "@rasylon_support")
ADMIN_REDIRECT_URL = os.getenv("ADMIN_REDIRECT_URL", "https://rasylon-support-production.up.railway.app/")
YANDEX_MAPS_API_KEY = os.getenv("YANDEX_MAPS_API_KEY") or os.getenv("VITE_YANDEX_MAPS_API_KEY")
PaymentCreatedCallback = Callable[[int, str], Awaitable[None]]
OrderCreatedCallback = Callable[[Dict[str, Any]], Awaitable[None]]
OtpSenderCallback = Callable[[str, str, Optional[int]], Awaitable[None]]
MailingStartCallback = Callable[[int, str, int, Optional[Dict[str, Any]]], Awaitable[Dict[str, Any]]]
MailingStatusCallback = Callable[[int], Awaitable[Dict[str, Any]]]
MailingStopCallback = Callable[[int], Awaitable[Dict[str, Any]]]
MailingSelectAllCallback = Callable[[int], Awaitable[Dict[str, Any]]]
OTP_TTL_SECONDS = int(os.getenv("OTP_TTL_SECONDS", "300"))
OTP_RESEND_SECONDS = int(os.getenv("OTP_RESEND_SECONDS", "60"))
OTP_MAX_ATTEMPTS = int(os.getenv("OTP_MAX_ATTEMPTS", "5"))
OTP_STORE: Dict[str, Dict[str, Any]] = {}
BROWSER_LOGIN_TTL_SECONDS = int(os.getenv("BROWSER_LOGIN_TTL_SECONDS", "300"))
BROWSER_LOGIN_STORE: Dict[str, Dict[str, Any]] = {}
AUTH_SESSION_TTL_SECONDS = int(os.getenv("AUTH_SESSION_TTL_SECONDS", "604800"))
AUTH_SESSION_STORE: Dict[str, Dict[str, Any]] = {}
AUTH_COOKIE_NAME = os.getenv("AUTH_COOKIE_NAME", "rasylon_auth_token")
AUTH_COOKIE_SECURE = os.getenv("AUTH_COOKIE_SECURE", "true").lower() not in {"0", "false", "no"}
ADMIN_USER_IDS = {
    int(admin_id.strip())
    for admin_id in os.getenv("ADMIN_IDS", "").split(",")
    if admin_id.strip().isdigit()
}
KNOWN_LOCATION_ALIASES = {
    "таш": "Ташкент",
    "tash": "Ташкент",
    "tashkent": "Ташкент",
    "toshkent": "Ташкент",
    "ташкент": "Ташкент",
    "самарканд": "Самарканд",
    "samarqand": "Самарканд",
    "samarkand": "Самарканд",
    "алматы": "Алматы",
    "almata": "Алматы",
    "almaty": "Алматы",
    "бишкек": "Бишкек",
    "bishkek": "Бишкек",
    "москва": "Москва",
    "moscow": "Москва",
}
KNOWN_LOCATION_COORDS = {
    "Ташкент": {"country": "Узбекистан", "lat": 41.31, "lon": 69.28},
    "Самарканд": {"country": "Узбекистан", "lat": 39.65, "lon": 66.96},
    "Алматы": {"country": "Казахстан", "lat": 43.24, "lon": 76.9},
    "Бишкек": {"country": "Кыргызстан", "lat": 42.87, "lon": 74.59},
    "Москва": {"country": "Россия", "lat": 55.75, "lon": 37.62},
}


@web.middleware
async def cors_middleware(request: web.Request, handler: Callable[[web.Request], Awaitable[web.StreamResponse]]) -> web.StreamResponse:
    if request.method == "OPTIONS":
        response = web.Response(status=204)
    else:
        response = await handler(request)
    configured_origins = [
        origin.strip()
        for origin in os.getenv("CORS_ALLOW_ORIGIN", "*").split(",")
        if origin.strip()
    ]
    request_origin = request.headers.get("Origin", "")
    allowed_origin = "*"
    if "*" not in configured_origins:
        allowed_origin = request_origin if request_origin in configured_origins else (configured_origins[0] if configured_origins else "")
    response.headers["Access-Control-Allow-Origin"] = allowed_origin
    response.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    if allowed_origin != "*":
        response.headers["Access-Control-Allow-Credentials"] = "true"
    response.headers["Access-Control-Max-Age"] = "86400"
    return response


def money(amount: int) -> str:
    return f"{amount:,}".replace(",", " ") + f" {PAYMENT_CURRENCY}"


def normalize_phone(raw: str) -> Optional[str]:
    digits = "".join(ch for ch in raw if ch.isdigit())
    if not digits:
        return None
    text = raw.strip()
    if text.startswith("+"):
        return f"+{digits}"
    if digits.startswith("00"):
        return f"+{digits[2:]}"
    return f"+{digits}"


def normalize_card(raw: str) -> Optional[str]:
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) < 12 or len(digits) > 19:
        return None
    return " ".join(digits[i : i + 4] for i in range(0, len(digits), 4))


def fallback_user_id(phone: str) -> int:
    digits = "".join(ch for ch in phone if ch.isdigit())
    if not digits:
        raise ValueError("phone_required")
    return int(digits[-15:])


def now_utc() -> datetime:
    return datetime.utcnow()


def prune_browser_logins() -> None:
    current_time = now_utc()
    expired_tokens = [
        token
        for token, record in BROWSER_LOGIN_STORE.items()
        if current_time > record["expires_at"]
    ]
    for token in expired_tokens:
        BROWSER_LOGIN_STORE.pop(token, None)
    expired_sessions = [
        token
        for token, record in AUTH_SESSION_STORE.items()
        if current_time > record["expires_at"]
    ]
    for token in expired_sessions:
        AUTH_SESSION_STORE.pop(token, None)


def confirm_browser_login(token: str, telegram_user: Dict[str, Any]) -> bool:
    prune_browser_logins()
    record = BROWSER_LOGIN_STORE.get(token)
    if not record or record.get("confirmed_user"):
        return False
    record["confirmed_user"] = telegram_user
    record["confirmed_at"] = now_utc()
    return True


def auth_secret() -> Optional[bytes]:
    raw = os.getenv("AUTH_SESSION_SECRET") or os.getenv("BOT_TOKEN")
    return raw.encode() if raw else None


def b64encode_json(payload: Dict[str, Any]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def b64decode_json(value: str) -> Optional[Dict[str, Any]]:
    try:
        padded = value + "=" * (-len(value) % 4)
        raw = base64.urlsafe_b64decode(padded.encode())
        decoded = json.loads(raw.decode())
    except (ValueError, json.JSONDecodeError):
        return None
    return decoded if isinstance(decoded, dict) else None


def sign_auth_payload(payload_part: str) -> Optional[str]:
    secret = auth_secret()
    if not secret:
        return None
    digest = hmac.new(secret, payload_part.encode(), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest).decode().rstrip("=")


def create_auth_session(telegram_user: Dict[str, Any]) -> str:
    expires_at = now_utc() + timedelta(seconds=AUTH_SESSION_TTL_SECONDS)
    payload = {
        "v": 1,
        "exp": int(expires_at.timestamp()),
        "telegram_user": telegram_user,
    }
    payload_part = b64encode_json(payload)
    signature = sign_auth_payload(payload_part)
    if not signature:
        token = secrets.token_urlsafe(32)
    else:
        token = f"{payload_part}.{signature}"
    AUTH_SESSION_STORE[token] = {
        "telegram_user": telegram_user,
        "created_at": now_utc(),
        "expires_at": expires_at,
    }
    return token


def auth_user_from_token(token: str) -> Optional[Dict[str, Any]]:
    prune_browser_logins()
    record = AUTH_SESSION_STORE.get(token)
    if record:
        return record.get("telegram_user")
    parts = token.split(".")
    if len(parts) != 2:
        return None
    payload_part, received_sig = parts
    expected_sig = sign_auth_payload(payload_part)
    if not expected_sig or not hmac.compare_digest(received_sig, expected_sig):
        return None
    payload = b64decode_json(payload_part)
    if not payload:
        return None
    try:
        expires_at = int(payload.get("exp") or 0)
    except (TypeError, ValueError):
        return None
    if int(now_utc().timestamp()) > expires_at:
        return None
    telegram_user = payload.get("telegram_user")
    return telegram_user if isinstance(telegram_user, dict) else None


def resolve_authenticated_user(data: Dict[str, Any], request: Optional[web.Request] = None) -> Optional[Dict[str, Any]]:
    telegram_user = verify_telegram_init_data(str(data.get("tg_init_data") or ""))
    if telegram_user:
        return telegram_user
    auth_token = str(data.get("auth_token") or "")
    if not auth_token and request is not None:
        auth_token = request.cookies.get(AUTH_COOKIE_NAME, "")
    if auth_token:
        return auth_user_from_token(auth_token)
    return None


def normalize_location_text(value: str) -> Optional[str]:
    clean = re.sub(r"[^a-zA-Zа-яА-ЯёЁ0-9]+", " ", value).strip().lower()
    if not clean:
        return None
    words = clean.split()
    for word in words:
        if word in KNOWN_LOCATION_ALIASES:
            return KNOWN_LOCATION_ALIASES[word]
    for alias, normalized in KNOWN_LOCATION_ALIASES.items():
        if alias in clean:
            return normalized
    return None


def classify_message_locally(message: str) -> Dict[str, Any]:
    text = message.strip()
    lower = text.lower()
    current_location = normalize_location_text(text)
    destination = None
    direction_match = re.search(
        r"(?<![A-Za-zА-Яа-яЁё])(?:на|в|до|->|→)\s+([A-Za-zА-Яа-яЁё]+)",
        text,
        flags=re.IGNORECASE,
    )
    if direction_match:
        destination = normalize_location_text(direction_match.group(1)) or direction_match.group(1).title()
    vehicle_type = None
    for candidate in ("тент", "рефрижератор", "реф", "фура", "изотерм", "самосвал", "газель", "лабо"):
        if candidate in lower:
            vehicle_type = "рефрижератор" if candidate == "реф" else candidate
            break
    availability = None
    for candidate in ("сегодня", "завтра", "сейчас", "утром", "вечером"):
        if candidate in lower:
            availability = candidate
            break
    if any(phrase in lower for phrase in ("ищу водителя", "ищем водителя", "нужен водитель", "нужна машина", "ищу машину", "ищем машину", "нужен транспорт", "ищу перевозчика")):
        intent = "cargo_searching_driver"
    elif any(word in lower for word in ("ищу", "свобод", "стою", "загрузка", "груз")):
        intent = "driver_searching_cargo"
    else:
        intent = "unknown"
    return {
        "intent": intent,
        "current_location": current_location,
        "destination": destination,
        "vehicle_type": vehicle_type,
        "availability": availability,
        "contact": None,
        "source": "local_ai",
        "should_map": bool(current_location and intent != "unknown"),
    }


def public_locations_payload() -> Dict[str, Any]:
    return {
        "locations": [],
        "activities": [],
    }


def parse_message_datetime(value: Any) -> datetime:
    if not value:
        return now_utc()
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return now_utc()
    if parsed.tzinfo is not None:
        return parsed.astimezone().replace(tzinfo=None)
    return parsed


def minutes_since(value: Any) -> int:
    delta = now_utc() - parse_message_datetime(value)
    return max(0, int(delta.total_seconds() // 60))


def location_id(location: str) -> str:
    aliases = {
        "Ташкент": "tashkent",
        "Самарканд": "samarkand",
        "Алматы": "almaty",
        "Бишкек": "bishkek",
        "Москва": "moscow",
    }
    return aliases.get(location) or re.sub(r"[^a-z0-9]+", "-", location.lower(), flags=re.IGNORECASE).strip("-") or location


def build_locations_payload(messages: List[Dict[str, Any]]) -> Dict[str, Any]:
    locations: Dict[str, Dict[str, Any]] = {}
    activities = []
    for message in messages:
        location = str(message.get("current_location") or "").strip()
        if not location:
            continue
        coords = KNOWN_LOCATION_COORDS.get(location, {"country": "—", "lat": 41.31, "lon": 69.28})
        item = locations.setdefault(
            location,
            {
                "id": location_id(location),
                "name": location,
                "country": coords["country"],
                "lat": coords["lat"],
                "lon": coords["lon"],
                "drivers": 0,
                "messages": 0,
                "updated_at": "только что",
                "trend": [],
                "subscribed": False,
                "favorite": False,
                "_latest_minutes": None,
            },
        )
        item["messages"] += 1
        if message.get("intent") == "driver_searching_cargo":
            item["drivers"] += 1
        age = minutes_since(message.get("created_at"))
        if item["_latest_minutes"] is None or age < item["_latest_minutes"]:
            item["_latest_minutes"] = age
            item["updated_at"] = "только что" if age < 1 else f"{age} мин назад"
        author_username = str(message.get("author_username") or "").strip()
        source = str(message.get("chat_username") or message.get("chat_title") or "telegram").strip()
        activities.append(
            {
                "id": str(message.get("id") or f"{message.get('chat_id')}:{message.get('message_id')}"),
                "driver": message.get("author_name") or (f"@{author_username}" if author_username else "Telegram user"),
                "username": f"@{author_username}" if author_username else "",
                "location": location,
                "destination": message.get("destination") or "не указано",
                "vehicle_type": message.get("vehicle_type") or "не указан",
                "availability": message.get("availability") or "не указано",
                "intent": message.get("intent") or "unknown",
                "message": message.get("text") or "",
                "source": source,
                "minutes_ago": age,
            }
        )
    output_locations = []
    for item in locations.values():
        count = max(1, int(item["messages"]))
        item["trend"] = [count]
        item.pop("_latest_minutes", None)
        output_locations.append(item)
    output_locations.sort(key=lambda item: (-int(item["messages"]), item["name"]))
    activities.sort(key=lambda item: int(item["minutes_ago"]))
    return {"locations": output_locations, "activities": activities}


async def resolve_signal_chat_scope(
    storage: Any,
    user_id: int,
    shared_chat_ids: Optional[Any] = None,
) -> Dict[str, Any]:
    auto = await storage.get_auto(user_id)
    target_chat_ids = auto.get("target_chat_ids") or []
    if target_chat_ids:
        return {"scope": "selected_groups", "chat_ids": [int(chat_id) for chat_id in target_chat_ids]}
    account_id = auto.get("sender_account_id")
    if account_id is None:
        shared_ids = [int(chat_id) for chat_id in (shared_chat_ids or [])]
        if shared_ids and user_id in ADMIN_USER_IDS:
            return {"scope": "admin_shared_sender_groups", "chat_ids": shared_ids}
        return {"scope": "no_selected_groups", "chat_ids": []}
    account_chats = await storage.list_known_chats(account_id=account_id, owner_id=user_id)
    return {
        "scope": "sender_account_groups",
        "chat_ids": [int(chat_id) for chat_id in account_chats.keys()],
    }


def verify_telegram_init_data(init_data: str) -> Optional[Dict[str, Any]]:
    bot_token = os.getenv("BOT_TOKEN")
    if not init_data or not bot_token:
        return None
    parsed = dict(parse_qsl(init_data, keep_blank_values=True))
    received_hash = parsed.pop("hash", None)
    if not received_hash:
        return None
    data_check_string = "\n".join(f"{key}={parsed[key]}" for key in sorted(parsed))
    secret_key = hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()
    calculated = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(calculated, received_hash):
        return None
    user_raw = parsed.get("user")
    if not user_raw:
        return None
    try:
        return json.loads(user_raw)
    except json.JSONDecodeError:
        return None


async def index(request: web.Request) -> web.Response:
    app_path = BASE_DIR / "public_app.html"
    if app_path.exists():
        return web.Response(text=app_path.read_text(encoding="utf-8"), content_type="text/html")
    return web.Response(text=PUBLIC_APP_HTML, content_type="text/html")


async def telegram_lottie(request: web.Request) -> web.StreamResponse:
    return web.FileResponse(BASE_DIR / "Telegram.lottie")


async def config_api(request: web.Request) -> web.Response:
    return web.json_response(
        {
            "payment": {
                "amount": PAYMENT_AMOUNT,
                "amount_text": money(PAYMENT_AMOUNT),
                "currency": PAYMENT_CURRENCY,
                "description": PAYMENT_DESCRIPTION,
                "card_target": PAYMENT_CARD_TARGET,
                "valid_days": PAYMENT_VALID_DAYS,
            },
            "bot": {
                "username": BOT_USERNAME,
                "url": f"https://t.me/{BOT_USERNAME}" if BOT_USERNAME else None,
                "support": SUPPORT_AGENT_USERNAME,
            },
            "maps": {
                "yandex_api_key": YANDEX_MAPS_API_KEY,
            },
        }
    )


async def locations_api(request: web.Request) -> web.Response:
    return web.json_response(public_locations_payload())


async def signals_api(request: web.Request) -> web.Response:
    data = await request.json()
    telegram_user = resolve_authenticated_user(data, request)
    user_id = None
    if telegram_user:
        try:
            user_id = int(telegram_user.get("id"))
        except (TypeError, ValueError):
            user_id = None
    if user_id is None:
        return web.json_response({"error": "auth_required"}, status=401)
    try:
        limit = int(data.get("limit") or 100)
    except (TypeError, ValueError):
        limit = 100
    scope = await resolve_signal_chat_scope(
        request.app["storage"],
        user_id,
        request.app.get("shared_signal_chat_ids"),
    )
    messages = await request.app["storage"].list_logistics_messages(limit=limit, chat_ids=scope["chat_ids"])
    return web.json_response({"ok": True, "scope": scope["scope"], **build_locations_payload(messages)})


async def classify_message_api(request: web.Request) -> web.Response:
    data = await request.json()
    message = str(data.get("message") or "").strip()
    if len(message) < 3:
        return web.json_response({"error": "message_required"}, status=400)
    return web.json_response({"ok": True, "classification": classify_message_locally(message)})


async def mailing_start_api(request: web.Request) -> web.Response:
    data = await request.json()
    telegram_user = resolve_authenticated_user(data, request)
    user_id = None
    if telegram_user:
        try:
            user_id = int(telegram_user.get("id"))
        except (TypeError, ValueError):
            user_id = None
    if user_id is None:
        return web.json_response({"error": "auth_required"}, status=401)

    message = str(data.get("message") or "").strip()
    if len(message) < 3:
        return web.json_response({"error": "message_required"}, status=400)
    try:
        interval_minutes = int(data.get("interval_minutes") or 10)
    except (TypeError, ValueError):
        interval_minutes = 10
    interval_minutes = max(1, min(1440, interval_minutes))
    classification = classify_message_locally(message)

    callback: Optional[MailingStartCallback] = request.app.get("mailing_start_callback")
    if callback:
        result = await callback(user_id, message, interval_minutes, classification)
    else:
        await request.app["storage"].set_auto_message(user_id, message)
        await request.app["storage"].set_auto_interval(user_id, interval_minutes)
        await request.app["storage"].set_auto_enabled(user_id, True)
        result = {"ok": True, "started": True}
    return web.json_response({"ok": True, "classification": classification, **result})


async def mailing_status_api(request: web.Request) -> web.Response:
    data = await request.json()
    telegram_user = resolve_authenticated_user(data, request)
    user_id = None
    if telegram_user:
        try:
            user_id = int(telegram_user.get("id"))
        except (TypeError, ValueError):
            user_id = None
    if user_id is None:
        return web.json_response({"error": "auth_required"}, status=401)
    callback: Optional[MailingStatusCallback] = request.app.get("mailing_status_callback")
    if callback:
        return web.json_response({"ok": True, **await callback(user_id)})
    auto = await request.app["storage"].get_auto(user_id)
    target_count = len(auto.get("target_chat_ids") or [])
    return web.json_response(
        {
            "ok": True,
            "is_enabled": bool(auto.get("is_enabled")),
            "message": auto.get("message") or "",
            "interval_minutes": int(auto.get("interval_minutes") or 0),
            "target_count": target_count,
            "can_start": target_count > 0,
            "reasons": [] if target_count > 0 else ["no_targets"],
        }
    )


async def mailing_stop_api(request: web.Request) -> web.Response:
    data = await request.json()
    telegram_user = resolve_authenticated_user(data, request)
    user_id = None
    if telegram_user:
        try:
            user_id = int(telegram_user.get("id"))
        except (TypeError, ValueError):
            user_id = None
    if user_id is None:
        return web.json_response({"error": "auth_required"}, status=401)
    callback: Optional[MailingStopCallback] = request.app.get("mailing_stop_callback")
    if callback:
        result = await callback(user_id)
    else:
        await request.app["storage"].set_auto_enabled(user_id, False)
        result = {"stopped": True}
    return web.json_response({"ok": True, **result})


async def mailing_select_all_api(request: web.Request) -> web.Response:
    data = await request.json()
    telegram_user = resolve_authenticated_user(data, request)
    user_id = None
    if telegram_user:
        try:
            user_id = int(telegram_user.get("id"))
        except (TypeError, ValueError):
            user_id = None
    if user_id is None:
        return web.json_response({"error": "auth_required"}, status=401)
    callback: Optional[MailingSelectAllCallback] = request.app.get("mailing_select_all_callback")
    if callback:
        result = await callback(user_id)
    else:
        auto = await request.app["storage"].get_auto(user_id)
        account_id = auto.get("sender_account_id")
        known = await request.app["storage"].list_known_chats(account_id=account_id, owner_id=user_id if account_id is not None else None)
        chat_ids = [int(chat_id) for chat_id in known.keys()]
        await request.app["storage"].set_target_chats(user_id, chat_ids, account_id=account_id)
        result = {"selected_count": len(chat_ids)}
    return web.json_response({"ok": True, **result})


async def browser_login_start_api(request: web.Request) -> web.Response:
    prune_browser_logins()
    token = secrets.token_urlsafe(24)
    BROWSER_LOGIN_STORE[token] = {
        "created_at": now_utc(),
        "expires_at": now_utc() + timedelta(seconds=BROWSER_LOGIN_TTL_SECONDS),
        "confirmed_user": None,
    }
    bot_url = f"https://t.me/{BOT_USERNAME}?start=login_{token}" if BOT_USERNAME else None
    return web.json_response(
        {
            "ok": True,
            "token": token,
            "bot_url": bot_url,
            "expires_in": BROWSER_LOGIN_TTL_SECONDS,
        }
    )


async def browser_login_check_api(request: web.Request) -> web.Response:
    prune_browser_logins()
    token = str(request.query.get("token") or "")
    if not token:
        return web.json_response({"error": "token_required"}, status=400)
    record = BROWSER_LOGIN_STORE.get(token)
    if not record:
        return web.json_response({"status": "expired"}, status=404)
    confirmed_user = record.get("confirmed_user")
    if not confirmed_user:
        return web.json_response({"status": "pending"})
    BROWSER_LOGIN_STORE.pop(token, None)
    auth_token = create_auth_session(confirmed_user)
    response = web.json_response(
        {
            "status": "confirmed",
            "telegram_user": confirmed_user,
            "auth_token": auth_token,
        }
    )
    response.set_cookie(
        AUTH_COOKIE_NAME,
        auth_token,
        max_age=AUTH_SESSION_TTL_SECONDS,
        httponly=True,
        secure=AUTH_COOKIE_SECURE,
        samesite="None" if AUTH_COOKIE_SECURE else "Lax",
        path="/",
    )
    return response


async def logout_api(request: web.Request) -> web.Response:
    response = web.json_response({"ok": True})
    response.del_cookie(AUTH_COOKIE_NAME, path="/")
    return response


async def request_otp_api(request: web.Request) -> web.Response:
    data = await request.json()
    phone = normalize_phone(str(data.get("phone") or ""))
    if not phone:
        return web.json_response({"error": "phone_required"}, status=400)
    telegram_user = verify_telegram_init_data(str(data.get("tg_init_data") or ""))
    if telegram_user is None and isinstance(data.get("telegram_user"), dict):
        telegram_user = data["telegram_user"]
    telegram_user_id: Optional[int] = None
    if telegram_user:
        try:
            telegram_user_id = int(telegram_user.get("id"))
        except (TypeError, ValueError):
            telegram_user_id = None

    record = OTP_STORE.get(phone)
    current_time = now_utc()
    if record and record.get("last_sent_at"):
        elapsed = (current_time - record["last_sent_at"]).total_seconds()
        if elapsed < OTP_RESEND_SECONDS:
            return web.json_response({"error": "rate_limited", "retry_after": int(OTP_RESEND_SECONDS - elapsed)}, status=429)

    otp = f"{secrets.randbelow(1_000_000):06d}"
    OTP_STORE[phone] = {
        "otp_hash": hashlib.sha256(otp.encode()).hexdigest(),
        "expires_at": current_time + timedelta(seconds=OTP_TTL_SECONDS),
        "last_sent_at": current_time,
        "attempts": 0,
    }

    sender: Optional[OtpSenderCallback] = request.app.get("otp_sender_callback")
    if sender:
        try:
            await sender(phone, otp, telegram_user_id)
        except Exception:
            logger.exception("Failed to send OTP to Telegram user.")
            OTP_STORE.pop(phone, None)
            return web.json_response(
                {
                    "error": "telegram_not_linked",
                    "bot_url": f"https://t.me/{BOT_USERNAME}" if BOT_USERNAME else None,
                },
                status=404,
            )
    else:
        logger.info("OTP generated for %s; no sender callback is configured.", phone)

    return web.json_response({"ok": True, "expires_in": OTP_TTL_SECONDS})


async def verify_otp_api(request: web.Request) -> web.Response:
    data = await request.json()
    phone = normalize_phone(str(data.get("phone") or ""))
    otp = "".join(ch for ch in str(data.get("otp") or "") if ch.isdigit())
    if not phone or not otp:
        return web.json_response({"error": "otp_required"}, status=400)

    record = OTP_STORE.get(phone)
    if not record:
        return web.json_response({"error": "otp_expired"}, status=400)
    if now_utc() > record["expires_at"]:
        OTP_STORE.pop(phone, None)
        return web.json_response({"error": "otp_expired"}, status=400)
    if int(record.get("attempts") or 0) >= OTP_MAX_ATTEMPTS:
        return web.json_response({"error": "too_many_attempts"}, status=429)

    record["attempts"] = int(record.get("attempts") or 0) + 1
    expected = str(record["otp_hash"])
    received = hashlib.sha256(otp.encode()).hexdigest()
    if not hmac.compare_digest(expected, received):
        return web.json_response({"error": "bad_otp", "attempts_left": OTP_MAX_ATTEMPTS - record["attempts"]}, status=403)

    OTP_STORE.pop(phone, None)
    return web.json_response({"ok": True, "phone": phone})


async def admin_login_api(request: web.Request) -> web.Response:
    data = await request.json()
    code = str(data.get("code") or "")
    expected = os.getenv("ADMIN_CODE")
    if not expected or not hmac.compare_digest(code, expected):
        return web.json_response({"error": "bad_code"}, status=403)
    return web.json_response({"ok": True, "redirect_url": ADMIN_REDIRECT_URL})


async def payment_api(request: web.Request) -> web.Response:
    data = await request.json()
    telegram_user = verify_telegram_init_data(str(data.get("tg_init_data") or ""))
    if telegram_user is None and isinstance(data.get("telegram_user"), dict):
        telegram_user = data["telegram_user"]

    telegram_phone = normalize_phone(str(data.get("telegram_phone") or ""))
    whatsapp_phone = normalize_phone(str(data.get("whatsapp_phone") or ""))
    primary_phone = telegram_phone or whatsapp_phone
    if not primary_phone:
        return web.json_response({"error": "phone_required"}, status=400)

    card_number = normalize_card(str(data.get("card_number") or ""))
    card_name = str(data.get("card_name") or "").strip()
    if not card_number:
        return web.json_response({"error": "bad_card"}, status=400)
    if len(card_name) < 3:
        return web.json_response({"error": "bad_name"}, status=400)

    user_id = None
    username = None
    full_name_parts: List[str] = []
    if telegram_user:
        try:
            user_id = int(telegram_user.get("id"))
        except (TypeError, ValueError):
            user_id = None
        username = telegram_user.get("username")
        full_name_parts = [
            str(telegram_user.get("first_name") or "").strip(),
            str(telegram_user.get("last_name") or "").strip(),
        ]
    if user_id is None:
        user_id = fallback_user_id(primary_phone)
    full_name = " ".join(part for part in full_name_parts if part) or username or primary_phone
    if whatsapp_phone and whatsapp_phone != telegram_phone:
        full_name = f"{full_name} / WhatsApp {whatsapp_phone}"

    request_id = await request.app["storage"].create_payment_request(
        user_id=user_id,
        username=username,
        full_name=full_name,
        card_number=card_number,
        card_name=card_name,
    )
    callback: Optional[PaymentCreatedCallback] = request.app.get("payment_created_callback")
    if callback:
        try:
            await callback(user_id, request_id)
        except Exception:
            logger.exception("Failed to notify admins about Mini App payment request.")
    return web.json_response(
        {
            "ok": True,
            "request_id": request_id,
            "bot_url": f"https://t.me/{BOT_USERNAME}" if BOT_USERNAME else None,
        }
    )


async def order_api(request: web.Request) -> web.Response:
    data = await request.json()
    telegram_user = verify_telegram_init_data(str(data.get("tg_init_data") or ""))
    if telegram_user is None and isinstance(data.get("telegram_user"), dict):
        telegram_user = data["telegram_user"]

    contact_phone = normalize_phone(str(data.get("phone") or ""))
    if not contact_phone:
        return web.json_response({"error": "phone_required"}, status=400)

    order = {
        "from": str(data.get("from") or "").strip(),
        "to": str(data.get("to") or "").strip(),
        "truck_type": str(data.get("truck_type") or "").strip(),
        "weight": str(data.get("weight") or "").strip(),
        "date": str(data.get("date") or "").strip(),
        "phone": contact_phone,
        "note": str(data.get("note") or "").strip(),
        "telegram_user": telegram_user,
    }
    if not order["from"] or not order["to"] or not order["truck_type"]:
        return web.json_response({"error": "route_required"}, status=400)

    callback: Optional[OrderCreatedCallback] = request.app.get("order_created_callback")
    if callback:
        try:
            await callback(order)
        except Exception:
            logger.exception("Failed to notify admins about Mini App order.")
    return web.json_response({"ok": True, "bot_url": f"https://t.me/{BOT_USERNAME}" if BOT_USERNAME else None})


async def health(request: web.Request) -> web.Response:
    return web.json_response({"ok": True})


def create_app(
    storage: Optional[Any] = None,
    *,
    payment_created_callback: Optional[PaymentCreatedCallback] = None,
    order_created_callback: Optional[OrderCreatedCallback] = None,
    otp_sender_callback: Optional[OtpSenderCallback] = None,
    mailing_start_callback: Optional[MailingStartCallback] = None,
    mailing_status_callback: Optional[MailingStatusCallback] = None,
    mailing_stop_callback: Optional[MailingStopCallback] = None,
    mailing_select_all_callback: Optional[MailingSelectAllCallback] = None,
) -> web.Application:
    app = web.Application(middlewares=[cors_middleware])
    app["storage"] = storage or create_storage_from_env()
    app["payment_created_callback"] = payment_created_callback
    app["order_created_callback"] = order_created_callback
    app["otp_sender_callback"] = otp_sender_callback
    app["mailing_start_callback"] = mailing_start_callback
    app["mailing_status_callback"] = mailing_status_callback
    app["mailing_stop_callback"] = mailing_stop_callback
    app["mailing_select_all_callback"] = mailing_select_all_callback
    app["shared_signal_chat_ids"] = set()
    app.router.add_get("/health", health)
    app.router.add_get("/assets/telegram.lottie", telegram_lottie)
    app.router.add_get("/", index)
    app.router.add_get("/app", index)
    app.router.add_get("/api/mini/config", config_api)
    app.router.add_get("/api/mini/locations", locations_api)
    app.router.add_post("/api/mini/signals", signals_api)
    app.router.add_post("/api/ai/classify-message", classify_message_api)
    app.router.add_post("/api/mini/mailing/start", mailing_start_api)
    app.router.add_post("/api/mini/mailing/status", mailing_status_api)
    app.router.add_post("/api/mini/mailing/stop", mailing_stop_api)
    app.router.add_post("/api/mini/mailing/select-all", mailing_select_all_api)
    app.router.add_post("/api/auth/browser-login/start", browser_login_start_api)
    app.router.add_get("/api/auth/browser-login/check", browser_login_check_api)
    app.router.add_post("/api/auth/logout", logout_api)
    app.router.add_post("/api/auth/request-otp", request_otp_api)
    app.router.add_post("/api/auth/verify-otp", verify_otp_api)
    app.router.add_post("/api/mini/admin-login", admin_login_api)
    app.router.add_post("/api/mini/payment", payment_api)
    app.router.add_post("/api/mini/order", order_api)
    return app


PUBLIC_APP_HTML = r"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Rasylon Logistics</title>
  <script src="https://telegram.org/js/telegram-web-app.js"></script>
  <script type="module" src="https://unpkg.com/@dotlottie/player-component@2.7.12/dist/dotlottie-player.mjs"></script>
  <style>
    :root { font-family: Inter, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color: #1d2939; background: #f2f5f8; }
    * { box-sizing: border-box; }
    html, body { margin: 0; width: 100%; min-height: 100%; overflow: hidden; background: #f2f5f8; }
    button { border: 1px solid #c5cfdb; background: #fff; color: #233044; border-radius: 6px; padding: 12px; font-size: 15px; font-weight: 800; cursor: pointer; }
    button.primary { background: #1565c0; border-color: #1565c0; color: #fff; }
    button.active { border-color: #1565c0; box-shadow: inset 0 0 0 1px #1565c0; }
    button.icon { width: 48px; height: 48px; display: grid; place-items: center; padding: 0; border-radius: 999px; }
    button:disabled { opacity: .36; cursor: default; }
    main { width: min(760px, 100%); height: 100vh; height: 100dvh; margin: 0 auto; display: grid; grid-template-rows: 1fr auto; padding: 16px; }
    .viewport { position: relative; min-height: 0; overflow: hidden; }
    .slide { position: absolute; inset: 0; display: grid; align-content: center; gap: 14px; opacity: 0; transform: translateX(24px); pointer-events: none; transition: opacity .22s ease, transform .22s ease; overflow-y: auto; padding: 6px 0 12px; }
    .slide.active { opacity: 1; transform: translateX(0); pointer-events: auto; }
    .panel { background: #fff; border: 1px solid #d9e1ea; border-radius: 8px; padding: 16px; }
    .intro { text-align: center; justify-items: center; }
    .intro-player { width: min(280px, 78vw); height: min(280px, 78vw); display: block; }
    h1 { margin: 0; font-size: 32px; line-height: 1.05; letter-spacing: 0; }
    h2 { margin: 0; font-size: 22px; line-height: 1.15; letter-spacing: 0; }
    p { margin: 8px 0 0; color: #667085; line-height: 1.45; }
    label { display: block; font-size: 13px; font-weight: 800; margin: 12px 0 6px; }
    input { width: 100%; border: 1px solid #b9c4d0; border-radius: 6px; padding: 12px; font-size: 16px; background: #fff; color: #101828; }
    .language, .roles, .summary { display: grid; grid-template-columns: repeat(2, 1fr); gap: 10px; }
    .language { grid-template-columns: repeat(3, 1fr); }
    .mini { background: #f7f9fc; border: 1px solid #e3e9f1; border-radius: 8px; padding: 12px; }
    .mini span { display: block; color: #667085; font-size: 12px; font-weight: 800; }
    .mini strong { display: block; margin-top: 4px; font-size: 18px; overflow-wrap: anywhere; }
    .error { min-height: 20px; margin-top: 10px; color: #b42318; font-size: 13px; }
    .success { color: #067647; }
    .about-list { margin: 12px 0 0; padding-left: 18px; color: #344054; line-height: 1.5; }
    .nav { display: grid; grid-template-columns: 48px 1fr 48px; align-items: center; gap: 10px; padding-top: 10px; }
    .dots { display: flex; justify-content: center; gap: 7px; }
    .dot { width: 7px; height: 7px; border-radius: 999px; background: #b9c4d0; }
    .dot.active { width: 22px; background: #1565c0; }
    @media (max-width: 560px) {
      main { padding: 14px; }
      h1 { font-size: 28px; }
      h2 { font-size: 20px; }
      .language, .roles, .summary { grid-template-columns: 1fr; }
      .slide { align-content: start; padding-top: 10px; }
      .intro { align-content: center; }
    }
  </style>
</head>
<body>
  <main>
    <div class="viewport">
      <section class="slide intro active" data-step="intro">
        <dotlottie-player class="intro-player" src="/assets/telegram.lottie" background="transparent" speed="1" autoplay loop></dotlottie-player>
        <div>
          <h1 id="title">Rasylon Logistics</h1>
          <p id="lead">Auto-mailing, audience tools, payment control, and logistics communication in one Telegram bot.</p>
        </div>
      </section>

      <section class="slide" data-step="language">
        <div class="panel">
          <h2 id="langTitle">Language</h2>
          <p id="langText">Choose the language for this Mini App session.</p>
          <div class="language" style="margin-top:14px;">
            <button data-lang="ru" class="active">Русский</button>
            <button data-lang="uz">O'zbekcha</button>
            <button data-lang="en">English</button>
          </div>
        </div>
      </section>

      <section class="slide" data-step="info">
        <div class="panel">
          <h2 id="aboutTitle">What we do</h2>
          <p id="aboutText">We help teams send Telegram campaigns, manage sender accounts, parse audiences, invite users, and track paid access.</p>
          <ul class="about-list">
            <li id="point1">Auto-mailing campaigns with delivery statistics.</li>
            <li id="point2">Telegram and WhatsApp contact based onboarding.</li>
            <li id="point3">Payment requests are sent to admins for confirmation.</li>
          </ul>
        </div>
      </section>

      <section class="slide" data-step="role">
        <div class="panel">
          <h2 id="roleTitle">Continue as</h2>
          <p id="roleText">Choose the route you need.</p>
          <div class="roles" style="margin-top:14px;">
            <button id="userBtn" class="primary">User</button>
            <button id="adminBtn">Admin</button>
          </div>
        </div>
      </section>

      <section class="slide" data-step="admin">
        <div class="panel">
          <h2 id="adminTitle">Admin access</h2>
          <label for="adminCode" id="adminCodeLabel">Admin code</label>
          <input id="adminCode" type="password" autocomplete="current-password">
          <button class="primary" id="adminSubmit" style="width:100%; margin-top:12px;">Continue</button>
          <div class="error" id="adminError"></div>
        </div>
      </section>

      <section class="slide" data-step="payment">
        <div class="panel">
          <h2 id="payTitle">Payment request</h2>
          <div class="summary" style="margin-top:12px;">
            <div class="mini"><span id="amountLabel">Amount</span><strong id="amount">...</strong></div>
            <div class="mini"><span id="cardTargetLabel">Pay to card</span><strong id="cardTarget">...</strong></div>
          </div>
          <label for="telegramPhone" id="telegramPhoneLabel">Telegram phone number</label>
          <input id="telegramPhone" inputmode="tel" autocomplete="tel" placeholder="+998...">
          <label for="whatsappPhone" id="whatsappPhoneLabel">WhatsApp phone number</label>
          <input id="whatsappPhone" inputmode="tel" autocomplete="tel" placeholder="+998...">
          <label for="cardNumber" id="cardNumberLabel">Card number used for payment</label>
          <input id="cardNumber" inputmode="numeric" autocomplete="cc-number">
          <label for="cardName" id="cardNameLabel">Name on card</label>
          <input id="cardName" autocomplete="cc-name">
          <button class="primary" id="paySubmit" style="width:100%; margin-top:12px;">Send payment request</button>
          <div class="error" id="payError"></div>
        </div>
      </section>
    </div>

    <nav class="nav" aria-label="Page navigation">
      <button class="icon" id="prevBtn" aria-label="Previous page">‹</button>
      <div class="dots" id="dots"></div>
      <button class="icon primary" id="nextBtn" aria-label="Next page">›</button>
    </nav>
  </main>

  <script>
    const tg = window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;
    if (tg) { tg.ready(); tg.expand(); }
    const i18n = {
      ru: { title:'Rasylon Logistics', lead:'Авторассылка, аудитории, контроль оплат и коммуникация для логистики в Telegram-боте.', langTitle:'Язык', langText:'Выберите язык для Mini App.', aboutTitle:'Что мы делаем', aboutText:'Мы помогаем запускать Telegram-рассылки, управлять номерами, собирать аудитории, приглашать пользователей и контролировать оплаченный доступ.', point1:'Авторассылки со статистикой доставленных сообщений.', point2:'Онбординг по Telegram и WhatsApp номеру.', point3:'Заявки на оплату отправляются администраторам на подтверждение.', roleTitle:'Продолжить как', roleText:'Выберите нужный маршрут.', user:'Пользователь', admin:'Админ', adminTitle:'Доступ администратора', adminCodeLabel:'Код администратора', adminSubmit:'Продолжить', payTitle:'Заявка на оплату', amountLabel:'Сумма', cardTargetLabel:'Карта для оплаты', telegramPhoneLabel:'Номер Telegram', whatsappPhoneLabel:'Номер WhatsApp', cardNumberLabel:'Номер карты, с которой оплатили', cardNameLabel:'Имя на карте', paySubmit:'Отправить заявку', badCode:'Неверный код администратора.', adminOk:'Код принят. Используйте меню бота для статистики и управления.', phoneRequired:'Укажите номер Telegram или WhatsApp.', badCard:'Номер карты должен содержать 12-19 цифр.', badName:'Укажите имя на карте минимум из 3 символов.', saved:'Заявка отправлена. Mini App сейчас закроется.' },
      uz: { title:'Rasylon Logistics', lead:'Telegram botda avto-xabarlar, auditoriya, tolov nazorati va logistika aloqalari.', langTitle:'Til', langText:'Mini App uchun tilni tanlang.', aboutTitle:'Nima qilamiz', aboutText:'Telegram kampaniyalari, raqamlar, auditoriya, takliflar va pullik kirishni boshqarishga yordam beramiz.', point1:'Yetkazilgan xabarlar statistikasi bilan avto-xabarlar.', point2:'Telegram va WhatsApp raqami orqali ulanish.', point3:'Tolov sorovlari admin tasdigiga yuboriladi.', roleTitle:'Davom etish', roleText:'Kerakli yonalishni tanlang.', user:'Foydalanuvchi', admin:'Admin', adminTitle:'Admin kirishi', adminCodeLabel:'Admin kodi', adminSubmit:'Davom etish', payTitle:'Tolov sorovi', amountLabel:'Summa', cardTargetLabel:'Tolov kartasi', telegramPhoneLabel:'Telegram raqami', whatsappPhoneLabel:'WhatsApp raqami', cardNumberLabel:'Tolov qilingan karta raqami', cardNameLabel:'Kartadagi ism', paySubmit:'Sorovni yuborish', badCode:'Admin kodi notogri.', adminOk:'Kod qabul qilindi. Statistika va boshqaruv uchun bot menyusidan foydalaning.', phoneRequired:'Telegram yoki WhatsApp raqamini kiriting.', badCard:'Karta raqami 12-19 ta raqamdan iborat bolishi kerak.', badName:'Kartadagi ism kamida 3 belgidan iborat bolishi kerak.', saved:'Sorov yuborildi. Mini App yopiladi.' },
      en: { title:'Rasylon Logistics', lead:'Auto-mailing, audience tools, payment control, and logistics communication in one Telegram bot.', langTitle:'Language', langText:'Choose the language for this Mini App session.', aboutTitle:'What we do', aboutText:'We help teams send Telegram campaigns, manage sender accounts, parse audiences, invite users, and track paid access.', point1:'Auto-mailing campaigns with delivery statistics.', point2:'Telegram and WhatsApp contact based onboarding.', point3:'Payment requests are sent to admins for confirmation.', roleTitle:'Continue as', roleText:'Choose the route you need.', user:'User', admin:'Admin', adminTitle:'Admin access', adminCodeLabel:'Admin code', adminSubmit:'Continue', payTitle:'Payment request', amountLabel:'Amount', cardTargetLabel:'Pay to card', telegramPhoneLabel:'Telegram phone number', whatsappPhoneLabel:'WhatsApp phone number', cardNumberLabel:'Card number used for payment', cardNameLabel:'Name on card', paySubmit:'Send payment request', badCode:'Invalid admin code.', adminOk:'Code accepted. Use the bot menu for statistics and management.', phoneRequired:'Enter Telegram or WhatsApp number.', badCard:'Card number must contain 12-19 digits.', badName:'Enter at least 3 characters for the card name.', saved:'Request sent. The Mini App will close now.' }
    };
    const state = { lang: 'ru', role: 'user', step: 0 };
    const baseSteps = ['intro', 'language', 'info', 'role'];
    const $ = id => document.getElementById(id);
    const ids = ['title','lead','langTitle','langText','aboutTitle','aboutText','point1','point2','point3','roleTitle','roleText','adminTitle','adminCodeLabel','adminSubmit','payTitle','amountLabel','cardTargetLabel','telegramPhoneLabel','whatsappPhoneLabel','cardNumberLabel','cardNameLabel','paySubmit'];
    function steps() { return [...baseSteps, state.role === 'admin' ? 'admin' : 'payment']; }
    function t(key) { return i18n[state.lang][key]; }
    function applyLang() {
      ids.forEach(id => { const el = $(id); if (el) el.textContent = t(id); });
      $('userBtn').textContent = t('user');
      $('adminBtn').textContent = t('admin');
      document.querySelectorAll('[data-lang]').forEach(btn => btn.classList.toggle('active', btn.dataset.lang === state.lang));
    }
    function renderRole() {
      $('userBtn').classList.toggle('primary', state.role === 'user');
      $('adminBtn').classList.toggle('primary', state.role === 'admin');
    }
    function renderStep() {
      const currentSteps = steps();
      state.step = Math.max(0, Math.min(state.step, currentSteps.length - 1));
      document.querySelectorAll('.slide').forEach(slide => slide.classList.toggle('active', slide.dataset.step === currentSteps[state.step]));
      $('prevBtn').disabled = state.step === 0;
      $('nextBtn').disabled = state.step === currentSteps.length - 1;
      $('dots').innerHTML = currentSteps.map((_, index) => `<span class="dot ${index === state.step ? 'active' : ''}"></span>`).join('');
    }
    function go(offset) {
      state.step += offset;
      renderStep();
    }
    function errorText(code) { return ({phone_required:t('phoneRequired'), bad_card:t('badCard'), bad_name:t('badName'), bad_code:t('badCode')})[code] || code || 'Error'; }
    async function loadConfig() { const response = await fetch('/api/mini/config'); const config = await response.json(); $('amount').textContent = config.payment.amount_text; $('cardTarget').textContent = config.payment.card_target; }
    async function adminLogin() { $('adminError').classList.remove('success'); $('adminError').textContent = ''; const response = await fetch('/api/mini/admin-login', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({code:$('adminCode').value}) }); const data = await response.json(); if (!response.ok) { $('adminError').textContent = t('badCode'); return; } if (data.redirect_url) { location.href = data.redirect_url; return; } $('adminError').classList.add('success'); $('adminError').textContent = t('adminOk'); }
    async function submitPayment() { $('payError').classList.remove('success'); $('payError').textContent = ''; const response = await fetch('/api/mini/payment', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({ language:state.lang, tg_init_data:tg ? tg.initData : '', telegram_user:tg && tg.initDataUnsafe ? tg.initDataUnsafe.user : null, telegram_phone:$('telegramPhone').value, whatsapp_phone:$('whatsappPhone').value, card_number:$('cardNumber').value, card_name:$('cardName').value }) }); const data = await response.json(); if (!response.ok) { $('payError').textContent = errorText(data.error); return; } $('payError').classList.add('success'); $('payError').textContent = t('saved'); setTimeout(() => { if (tg) tg.close(); else if (data.bot_url) location.href = data.bot_url; }, 1200); }
    document.querySelectorAll('[data-lang]').forEach(button => button.addEventListener('click', () => { state.lang = button.dataset.lang; applyLang(); }));
    $('userBtn').addEventListener('click', () => { state.role = 'user'; renderRole(); go(1); });
    $('adminBtn').addEventListener('click', () => { state.role = 'admin'; renderRole(); go(1); });
    $('prevBtn').addEventListener('click', () => go(-1));
    $('nextBtn').addEventListener('click', () => go(1));
    $('adminSubmit').addEventListener('click', adminLogin);
    $('paySubmit').addEventListener('click', submitPayment);
    applyLang(); renderRole(); renderStep(); loadConfig();
  </script>
</body>
</html>"""


if __name__ == "__main__":
    port = int(os.getenv("PORT", os.getenv("APP_PORT", "8080")))
    host = os.getenv("APP_HOST", "0.0.0.0")
    web.run_app(create_app(), host=host, port=port)
