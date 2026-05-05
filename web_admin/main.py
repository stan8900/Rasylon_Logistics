import hashlib
import hmac
import json
import logging
import os
import secrets
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qsl

from aiohttp import web
from dotenv import load_dotenv

try:
    from .runtime_config import BASE_DIR, WEB_DIR, create_storage_from_env
except ImportError:  # pragma: no cover - allows `python web_admin/main.py`
    from runtime_config import BASE_DIR, WEB_DIR, create_storage_from_env


load_dotenv(BASE_DIR / ".env")
load_dotenv(WEB_DIR / ".env", override=True)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PAYMENT_AMOUNT = int(os.getenv("PAYMENT_AMOUNT", "100000"))
PAYMENT_CURRENCY = os.getenv("PAYMENT_CURRENCY", "UZS")
PAYMENT_DESCRIPTION = os.getenv("PAYMENT_DESCRIPTION", "Оплата услуг логистического бота")
PAYMENT_VALID_DAYS = int(os.getenv("PAYMENT_VALID_DAYS", "30"))
PAYMENT_CARD_TARGET = os.getenv("PAYMENT_CARD_TARGET", "9860 1701 1433 3116")
BOT_USERNAME = (os.getenv("BOT_USERNAME") or os.getenv("TELEGRAM_BOT_USERNAME") or "").lstrip("@")
SUPPORT_AGENT_USERNAME = os.getenv("SUPPORT_AGENT_USERNAME", "@rasylon_support")
SESSION_COOKIE = "sendertistics_admin"


def parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def period_since(period: str) -> tuple[str, Optional[datetime], int]:
    now = datetime.utcnow()
    if period == "week":
        return "7 дней", now - timedelta(days=7), 7
    if period == "month":
        return "30 дней", now - timedelta(days=30), 30
    if period == "all":
        return "всё время", None, 30
    return "сегодня", now.replace(hour=0, minute=0, second=0, microsecond=0), 1


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


def day_key(value: Optional[datetime]) -> Optional[str]:
    return value.date().isoformat() if value else None


def build_series(
    *,
    days: int,
    payments: List[Dict[str, Any]],
    campaign_events: List[Dict[str, Any]],
    delivery_events: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    start = datetime.utcnow().date() - timedelta(days=max(0, days - 1))
    buckets: Dict[str, Dict[str, Any]] = {}
    for offset in range(days):
        key = (start + timedelta(days=offset)).isoformat()
        buckets[key] = {"date": key, "payments": 0, "subscriptions": 0, "campaigns": 0, "deliveries": 0}

    for payment in payments:
        created = day_key(parse_iso(payment.get("created_at")))
        resolved = day_key(parse_iso(payment.get("resolved_at")))
        if created in buckets:
            buckets[created]["payments"] += 1
        if payment.get("status") == "approved" and resolved in buckets:
            buckets[resolved]["subscriptions"] += 1
    for event in campaign_events:
        key = day_key(parse_iso(event.get("started_at")))
        if key in buckets:
            buckets[key]["campaigns"] += 1
    for event in delivery_events:
        key = day_key(parse_iso(event.get("delivered_at")))
        if key in buckets:
            buckets[key]["deliveries"] += int(event.get("sent_count") or 0)
    return list(buckets.values())


def public_payment(payment: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "request_id": payment.get("request_id"),
        "user_id": payment.get("user_id"),
        "username": payment.get("username"),
        "full_name": payment.get("full_name"),
        "status": payment.get("status"),
        "created_at": payment.get("created_at"),
        "resolved_at": payment.get("resolved_at"),
        "expires_at": (
            (parse_iso(payment.get("resolved_at")) + timedelta(days=PAYMENT_VALID_DAYS)).isoformat()
            if payment.get("status") == "approved" and parse_iso(payment.get("resolved_at"))
            else None
        ),
    }


async def require_auth(request: web.Request) -> Optional[web.Response]:
    token = request.cookies.get(SESSION_COOKIE)
    expected = request.app["session_token"]
    if token and hmac.compare_digest(token, expected):
        return None
    if request.path.startswith("/api/"):
        return web.json_response({"error": "unauthorized"}, status=401)
    raise web.HTTPFound("/login")


async def login_page(request: web.Request) -> web.Response:
    if request.cookies.get(SESSION_COOKIE) == request.app["session_token"]:
        raise web.HTTPFound("/")
    return web.Response(text=LOGIN_HTML, content_type="text/html")


async def login_api(request: web.Request) -> web.Response:
    data = await request.json()
    password = str(data.get("password") or "")
    expected = request.app["admin_password"]
    if not expected or not hmac.compare_digest(password, expected):
        return web.json_response({"error": "bad_password"}, status=403)
    response = web.json_response({"ok": True})
    response.set_cookie(
        SESSION_COOKIE,
        request.app["session_token"],
        httponly=True,
        samesite="Strict",
        max_age=60 * 60 * 12,
    )
    return response


async def mini_app(request: web.Request) -> web.Response:
    return web.Response(text=MINI_APP_HTML, content_type="text/html")


async def mini_config_api(request: web.Request) -> web.Response:
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
        }
    )


async def mini_admin_login_api(request: web.Request) -> web.Response:
    data = await request.json()
    code = str(data.get("code") or "")
    expected = request.app["admin_password"]
    if not expected or not hmac.compare_digest(code, expected):
        return web.json_response({"error": "bad_code"}, status=403)
    response = web.json_response({"ok": True, "dashboard_url": "/"})
    response.set_cookie(
        SESSION_COOKIE,
        request.app["session_token"],
        httponly=True,
        samesite="Strict",
        max_age=60 * 60 * 12,
    )
    return response


async def mini_payment_api(request: web.Request) -> web.Response:
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
    return web.json_response(
        {
            "ok": True,
            "request_id": request_id,
            "bot_url": f"https://t.me/{BOT_USERNAME}" if BOT_USERNAME else None,
        }
    )


async def logout_api(request: web.Request) -> web.Response:
    response = web.json_response({"ok": True})
    response.del_cookie(SESSION_COOKIE)
    return response


async def index(request: web.Request) -> web.Response:
    auth = await require_auth(request)
    if auth:
        return auth
    return web.Response(text=DASHBOARD_HTML, content_type="text/html")


async def analytics_api(request: web.Request) -> web.Response:
    auth = await require_auth(request)
    if auth:
        return auth
    period = request.query.get("period", "day")
    if period not in {"day", "week", "month", "all"}:
        period = "day"
    title, since, chart_days = period_since(period)
    storage = request.app["storage"]

    payments = await storage.get_all_payments()
    campaign_events = await storage.list_auto_campaign_events(since=since)
    delivery_events = await storage.list_auto_delivery_events(since=since)

    period_payments = []
    approved = []
    pending = []
    declined = []
    active_users = set()
    active_threshold = datetime.utcnow() - timedelta(days=PAYMENT_VALID_DAYS)
    for payment in payments:
        created = parse_iso(payment.get("created_at"))
        resolved = parse_iso(payment.get("resolved_at"))
        status = payment.get("status")
        if since is None or (created and created >= since):
            period_payments.append(payment)
            if status == "pending":
                pending.append(payment)
            elif status == "declined":
                declined.append(payment)
        if status == "approved" and resolved:
            if since is None or resolved >= since:
                approved.append(payment)
            if resolved >= active_threshold:
                active_users.add(int(payment.get("user_id")))

    deliveries = sum(int(event.get("sent_count") or 0) for event in delivery_events)
    active_campaigns = await storage.count_active_auto_campaigns()
    latest = await storage.latest_payment_timestamp()
    latest_due = (latest + timedelta(days=PAYMENT_VALID_DAYS)).isoformat() if latest else None
    recent_payments = [public_payment(payment) for payment in payments[:50]]

    return web.json_response(
        {
            "period": period,
            "period_title": title,
            "currency": PAYMENT_CURRENCY,
            "cards": {
                "payment_requests": len(period_payments),
                "subscriptions": len(approved),
                "pending": len(pending),
                "declined": len(declined),
                "active_subscriptions": len(active_users),
                "revenue": len(approved) * PAYMENT_AMOUNT,
                "revenue_text": money(len(approved) * PAYMENT_AMOUNT),
                "campaign_starts": len(campaign_events),
                "deliveries": deliveries,
                "active_campaigns": active_campaigns,
                "latest_global_payment_due": latest_due,
            },
            "series": build_series(
                days=chart_days,
                payments=payments,
                campaign_events=campaign_events,
                delivery_events=delivery_events,
            ),
            "payments": recent_payments,
        }
    )


async def health(request: web.Request) -> web.Response:
    return web.json_response({"ok": True})


def create_app() -> web.Application:
    password = (
        os.getenv("ADMIN_WEB_PASSWORD")
        or os.getenv("WEB_DASHBOARD_PASSWORD")
        or os.getenv("ADMIN_CODE")
    )
    if not password:
        raise RuntimeError(
            "Set ADMIN_WEB_PASSWORD, WEB_DASHBOARD_PASSWORD, or ADMIN_CODE to protect the web admin panel."
        )
    app = web.Application()
    app["storage"] = create_storage_from_env()
    app["admin_password"] = password
    app["session_token"] = os.getenv("WEB_DASHBOARD_SECRET") or secrets.token_urlsafe(32)
    app.router.add_get("/health", health)
    app.router.add_get("/app", mini_app)
    app.router.add_get("/login", login_page)
    app.router.add_post("/api/login", login_api)
    app.router.add_get("/api/mini/config", mini_config_api)
    app.router.add_post("/api/mini/admin-login", mini_admin_login_api)
    app.router.add_post("/api/mini/payment", mini_payment_api)
    app.router.add_post("/api/logout", logout_api)
    app.router.add_get("/api/analytics", analytics_api)
    app.router.add_get("/", index)
    return app


MINI_APP_HTML = r"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Rasylon Logistics</title>
  <script src="https://telegram.org/js/telegram-web-app.js"></script>
  <style>
    :root {
      font-family: Inter, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      color: #1d2939;
      background: #f2f5f8;
    }
    * { box-sizing: border-box; }
    body { margin: 0; min-height: 100vh; background: #f2f5f8; }
    main { width: min(760px, 100%); margin: 0 auto; padding: 18px 16px 28px; }
    header { padding: 14px 0 18px; }
    h1 { margin: 0; font-size: 30px; line-height: 1.05; letter-spacing: 0; }
    h2 { margin: 0 0 12px; font-size: 18px; letter-spacing: 0; }
    p { margin: 8px 0 0; color: #667085; line-height: 1.45; }
    .panel { background: #fff; border: 1px solid #d9e1ea; border-radius: 8px; padding: 16px; margin-bottom: 12px; }
    .language, .roles, .pay-grid { display: grid; gap: 10px; }
    .language { grid-template-columns: repeat(3, 1fr); }
    .roles { grid-template-columns: repeat(2, 1fr); }
    button {
      border: 1px solid #c5cfdb; background: #fff; color: #233044; border-radius: 6px;
      padding: 12px; font-size: 15px; font-weight: 800; cursor: pointer;
    }
    button.primary { background: #1565c0; border-color: #1565c0; color: #fff; }
    button.active { border-color: #1565c0; box-shadow: inset 0 0 0 1px #1565c0; }
    button:disabled { opacity: .55; cursor: not-allowed; }
    label { display: block; font-size: 13px; font-weight: 800; margin: 12px 0 6px; }
    input {
      width: 100%; border: 1px solid #b9c4d0; border-radius: 6px;
      padding: 12px; font-size: 16px; background: #fff; color: #101828;
    }
    .hidden { display: none; }
    .summary { display: grid; grid-template-columns: repeat(2, 1fr); gap: 10px; }
    .mini { background: #f7f9fc; border: 1px solid #e3e9f1; border-radius: 8px; padding: 12px; }
    .mini span { display: block; color: #667085; font-size: 12px; font-weight: 800; }
    .mini strong { display: block; margin-top: 4px; font-size: 18px; overflow-wrap: anywhere; }
    .error { min-height: 20px; margin-top: 10px; color: #b42318; font-size: 13px; }
    .success { color: #067647; }
    .about-list { margin: 12px 0 0; padding-left: 18px; color: #344054; line-height: 1.5; }
    @media (max-width: 560px) {
      h1 { font-size: 26px; }
      .language, .roles, .summary { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <main>
    <header>
      <h1 id="title">Rasylon Logistics</h1>
      <p id="lead">Auto-mailing, audience tools, payment control, and logistics communication in one Telegram bot.</p>
    </header>

    <section class="panel">
      <h2 id="langTitle">Language</h2>
      <div class="language">
        <button data-lang="ru" class="active">Русский</button>
        <button data-lang="uz">O'zbekcha</button>
        <button data-lang="en">English</button>
      </div>
    </section>

    <section class="panel">
      <h2 id="aboutTitle">What we do</h2>
      <p id="aboutText">We help teams send Telegram campaigns, manage sender accounts, parse audiences, invite users, and track paid access.</p>
      <ul class="about-list">
        <li id="point1">Auto-mailing campaigns with delivery statistics.</li>
        <li id="point2">Telegram and WhatsApp contact based onboarding.</li>
        <li id="point3">Admin analytics for payments and subscriptions.</li>
      </ul>
    </section>

    <section class="panel">
      <h2 id="roleTitle">Continue as</h2>
      <div class="roles">
        <button id="userBtn" class="primary">User</button>
        <button id="adminBtn">Admin</button>
      </div>
    </section>

    <section class="panel" id="adminPanel">
      <h2 id="adminTitle">Admin access</h2>
      <label for="adminCode" id="adminCodeLabel">Admin code</label>
      <input id="adminCode" type="password" autocomplete="current-password">
      <button class="primary" id="adminSubmit">Open analytics dashboard</button>
      <div class="error" id="adminError"></div>
    </section>

    <section class="panel hidden" id="userPanel">
      <h2 id="payTitle">Payment request</h2>
      <div class="summary">
        <div class="mini"><span id="amountLabel">Amount</span><strong id="amount">...</strong></div>
        <div class="mini"><span id="cardTargetLabel">Pay to card</span><strong id="cardTarget">...</strong></div>
      </div>
      <label for="telegramPhone" id="telegramPhoneLabel">Telegram phone number</label>
      <input id="telegramPhone" inputmode="tel" autocomplete="tel" placeholder="+998...">
      <label for="whatsappPhone" id="whatsappPhoneLabel">WhatsApp phone number</label>
      <input id="whatsappPhone" inputmode="tel" autocomplete="tel" placeholder="+998...">
      <label for="cardNumber" id="cardNumberLabel">Your payment card number</label>
      <input id="cardNumber" inputmode="numeric" autocomplete="cc-number">
      <label for="cardName" id="cardNameLabel">Name on card</label>
      <input id="cardName" autocomplete="cc-name">
      <button class="primary" id="paySubmit">Send payment request</button>
      <div class="error" id="payError"></div>
    </section>
  </main>

  <script>
    const tg = window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;
    if (tg) { tg.ready(); tg.expand(); }

    const i18n = {
      ru: {
        title: 'Rasylon Logistics',
        lead: 'Авторассылка, аудитории, контроль оплат и коммуникация для логистики в Telegram-боте.',
        langTitle: 'Язык',
        aboutTitle: 'Что мы делаем',
        aboutText: 'Мы помогаем командам запускать Telegram-рассылки, управлять номерами, собирать аудитории, приглашать пользователей и контролировать оплаченный доступ.',
        point1: 'Авторассылки со статистикой доставленных сообщений.',
        point2: 'Онбординг по Telegram и WhatsApp номеру.',
        point3: 'Админ-аналитика оплат, подписок и заявок.',
        roleTitle: 'Продолжить как',
        user: 'Пользователь',
        admin: 'Админ',
        adminTitle: 'Доступ администратора',
        adminCodeLabel: 'Код администратора',
        adminSubmit: 'Открыть аналитику',
        payTitle: 'Заявка на оплату',
        amountLabel: 'Сумма',
        cardTargetLabel: 'Карта для оплаты',
        telegramPhoneLabel: 'Номер Telegram',
        whatsappPhoneLabel: 'Номер WhatsApp',
        cardNumberLabel: 'Номер карты, с которой оплатили',
        cardNameLabel: 'Имя на карте',
        paySubmit: 'Отправить заявку',
        badCode: 'Неверный код администратора.',
        phoneRequired: 'Укажите номер Telegram или WhatsApp.',
        badCard: 'Номер карты должен содержать 12-19 цифр.',
        badName: 'Укажите имя на карте минимум из 3 символов.',
        saved: 'Заявка отправлена. Откройте бот и дождитесь подтверждения администратора.'
      },
      uz: {
        title: 'Rasylon Logistics',
        lead: 'Telegram botda avto-xabarlar, auditoriya, tolov nazorati va logistika aloqalari.',
        langTitle: 'Til',
        aboutTitle: 'Nima qilamiz',
        aboutText: 'Jamoalarga Telegram kampaniyalarini yuborish, raqamlarni boshqarish, auditoriya yigish, foydalanuvchilarni taklif qilish va pullik kirishni nazorat qilishda yordam beramiz.',
        point1: 'Yetkazilgan xabarlar statistikasi bilan avto-xabarlar.',
        point2: 'Telegram va WhatsApp raqami orqali ulanish.',
        point3: 'Tolovlar, obunalar va sorovlar admin analitikasi.',
        roleTitle: 'Davom etish',
        user: 'Foydalanuvchi',
        admin: 'Admin',
        adminTitle: 'Admin kirishi',
        adminCodeLabel: 'Admin kodi',
        adminSubmit: 'Analitikani ochish',
        payTitle: 'Tolov sorovi',
        amountLabel: 'Summa',
        cardTargetLabel: 'Tolov kartasi',
        telegramPhoneLabel: 'Telegram raqami',
        whatsappPhoneLabel: 'WhatsApp raqami',
        cardNumberLabel: 'Tolov qilingan karta raqami',
        cardNameLabel: 'Kartadagi ism',
        paySubmit: 'Sorovni yuborish',
        badCode: 'Admin kodi notogri.',
        phoneRequired: 'Telegram yoki WhatsApp raqamini kiriting.',
        badCard: 'Karta raqami 12-19 ta raqamdan iborat bolishi kerak.',
        badName: 'Kartadagi ism kamida 3 belgidan iborat bolishi kerak.',
        saved: 'Sorov yuborildi. Botni oching va admin tasdigini kuting.'
      },
      en: {
        title: 'Rasylon Logistics',
        lead: 'Auto-mailing, audience tools, payment control, and logistics communication in one Telegram bot.',
        langTitle: 'Language',
        aboutTitle: 'What we do',
        aboutText: 'We help teams send Telegram campaigns, manage sender accounts, parse audiences, invite users, and track paid access.',
        point1: 'Auto-mailing campaigns with delivery statistics.',
        point2: 'Telegram and WhatsApp contact based onboarding.',
        point3: 'Admin analytics for payments, subscriptions, and requests.',
        roleTitle: 'Continue as',
        user: 'User',
        admin: 'Admin',
        adminTitle: 'Admin access',
        adminCodeLabel: 'Admin code',
        adminSubmit: 'Open analytics',
        payTitle: 'Payment request',
        amountLabel: 'Amount',
        cardTargetLabel: 'Pay to card',
        telegramPhoneLabel: 'Telegram phone number',
        whatsappPhoneLabel: 'WhatsApp phone number',
        cardNumberLabel: 'Card number used for payment',
        cardNameLabel: 'Name on card',
        paySubmit: 'Send payment request',
        badCode: 'Invalid admin code.',
        phoneRequired: 'Enter Telegram or WhatsApp number.',
        badCard: 'Card number must contain 12-19 digits.',
        badName: 'Enter at least 3 characters for the card name.',
        saved: 'Request sent. Open the bot and wait for admin confirmation.'
      }
    };

    const state = { lang: 'ru', config: null };
    const $ = id => document.getElementById(id);
    const textIds = ['title','lead','langTitle','aboutTitle','aboutText','point1','point2','point3','roleTitle','adminTitle','adminCodeLabel','adminSubmit','payTitle','amountLabel','cardTargetLabel','telegramPhoneLabel','whatsappPhoneLabel','cardNumberLabel','cardNameLabel','paySubmit'];

    function t(key) { return i18n[state.lang][key]; }
    function applyLang() {
      textIds.forEach(id => { $(id).textContent = t(id); });
      $('userBtn').textContent = t('user');
      $('adminBtn').textContent = t('admin');
      document.querySelectorAll('[data-lang]').forEach(btn => btn.classList.toggle('active', btn.dataset.lang === state.lang));
    }
    function showRole(role) {
      $('adminPanel').classList.toggle('hidden', role !== 'admin');
      $('userPanel').classList.toggle('hidden', role !== 'user');
      $('adminBtn').classList.toggle('primary', role === 'admin');
      $('userBtn').classList.toggle('primary', role === 'user');
    }
    function errorText(code) {
      return ({phone_required: t('phoneRequired'), bad_card: t('badCard'), bad_name: t('badName'), bad_code: t('badCode')})[code] || code || 'Error';
    }
    async function loadConfig() {
      const response = await fetch('/api/mini/config');
      state.config = await response.json();
      $('amount').textContent = state.config.payment.amount_text;
      $('cardTarget').textContent = state.config.payment.card_target;
    }
    async function adminLogin() {
      $('adminError').textContent = '';
      const response = await fetch('/api/mini/admin-login', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({code: $('adminCode').value})
      });
      if (!response.ok) {
        $('adminError').textContent = t('badCode');
        return;
      }
      location.href = '/';
    }
    async function submitPayment() {
      $('payError').classList.remove('success');
      $('payError').textContent = '';
      const response = await fetch('/api/mini/payment', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          language: state.lang,
          tg_init_data: tg ? tg.initData : '',
          telegram_user: tg && tg.initDataUnsafe ? tg.initDataUnsafe.user : null,
          telegram_phone: $('telegramPhone').value,
          whatsapp_phone: $('whatsappPhone').value,
          card_number: $('cardNumber').value,
          card_name: $('cardName').value
        })
      });
      const data = await response.json();
      if (!response.ok) {
        $('payError').textContent = errorText(data.error);
        return;
      }
      $('payError').classList.add('success');
      $('payError').textContent = t('saved');
      setTimeout(() => {
        if (tg) tg.close();
        else if (data.bot_url) location.href = data.bot_url;
      }, 1200);
    }

    document.querySelectorAll('[data-lang]').forEach(button => button.addEventListener('click', () => {
      state.lang = button.dataset.lang;
      applyLang();
    }));
    $('userBtn').addEventListener('click', () => showRole('user'));
    $('adminBtn').addEventListener('click', () => showRole('admin'));
    $('adminSubmit').addEventListener('click', adminLogin);
    $('paySubmit').addEventListener('click', submitPayment);
    applyLang();
    showRole('user');
    loadConfig();
  </script>
</body>
</html>"""


LOGIN_HTML = r"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Sendertistics Admin</title>
  <style>
    :root { color-scheme: light; font-family: Inter, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
    body { margin: 0; min-height: 100vh; display: grid; place-items: center; background: #eef2f6; color: #17202a; }
    main { width: min(380px, calc(100vw - 32px)); background: #fff; border: 1px solid #d7dee8; border-radius: 8px; padding: 24px; box-shadow: 0 18px 50px rgba(23,32,42,.10); }
    h1 { margin: 0 0 6px; font-size: 24px; letter-spacing: 0; }
    p { margin: 0 0 20px; color: #667085; }
    label { display: block; font-size: 13px; font-weight: 700; margin-bottom: 8px; }
    input { width: 100%; box-sizing: border-box; border: 1px solid #b9c4d0; border-radius: 6px; padding: 11px 12px; font-size: 15px; }
    button { width: 100%; margin-top: 14px; border: 0; border-radius: 6px; padding: 12px; font-weight: 800; color: white; background: #1565c0; cursor: pointer; }
    .error { min-height: 20px; color: #b42318; font-size: 13px; margin-top: 10px; }
  </style>
</head>
<body>
  <main>
    <h1>Sendertistics Admin</h1>
    <p>Вход в веб-панель аналитики</p>
    <form id="form">
      <label for="password">Пароль администратора</label>
      <input id="password" name="password" type="password" autocomplete="current-password" autofocus>
      <button type="submit">Войти</button>
      <div class="error" id="error"></div>
    </form>
  </main>
  <script>
    document.querySelector('#form').addEventListener('submit', async (event) => {
      event.preventDefault();
      const error = document.querySelector('#error');
      error.textContent = '';
      const response = await fetch('/api/login', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({password: document.querySelector('#password').value})
      });
      if (response.ok) location.href = '/';
      else error.textContent = 'Неверный пароль.';
    });
  </script>
</body>
</html>"""


DASHBOARD_HTML = r"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Sendertistics Analytics</title>
  <style>
    :root {
      font-family: Inter, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      color: #182230; background: #eef2f6;
    }
    * { box-sizing: border-box; }
    body { margin: 0; min-height: 100vh; }
    header { position: sticky; top: 0; z-index: 5; display: flex; align-items: center; justify-content: space-between; gap: 16px; padding: 14px 24px; background: rgba(255,255,255,.94); border-bottom: 1px solid #d9e1ea; backdrop-filter: blur(10px); }
    h1 { margin: 0; font-size: 22px; letter-spacing: 0; }
    .subtitle { margin: 2px 0 0; color: #667085; font-size: 13px; }
    .actions { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; justify-content: flex-end; }
    button { border: 1px solid #c5cfdb; background: #fff; color: #233044; border-radius: 6px; padding: 9px 11px; font-weight: 800; cursor: pointer; }
    button.active { background: #1565c0; border-color: #1565c0; color: #fff; }
    button.icon { width: 38px; height: 38px; display: grid; place-items: center; padding: 0; }
    main { width: min(1440px, 100%); margin: 0 auto; padding: 20px 24px 28px; }
    .grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; }
    .card { background: #fff; border: 1px solid #d9e1ea; border-radius: 8px; padding: 16px; min-width: 0; }
    .metric { display: grid; gap: 6px; min-height: 104px; }
    .metric span { color: #667085; font-size: 13px; font-weight: 700; }
    .metric strong { font-size: 28px; line-height: 1.05; letter-spacing: 0; overflow-wrap: anywhere; }
    .metric small { color: #667085; }
    .layout { display: grid; grid-template-columns: minmax(0, 1.35fr) minmax(360px, .65fr); gap: 12px; margin-top: 12px; align-items: start; }
    .panel-title { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-bottom: 10px; }
    h2 { margin: 0; font-size: 16px; letter-spacing: 0; }
    canvas { width: 100%; height: 320px; display: block; }
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th, td { padding: 10px 8px; border-bottom: 1px solid #edf1f5; text-align: left; vertical-align: top; }
    th { color: #667085; font-size: 12px; }
    .status { display: inline-flex; align-items: center; border-radius: 999px; padding: 3px 8px; font-size: 12px; font-weight: 800; }
    .approved { background: #dcfae6; color: #067647; }
    .pending { background: #fef0c7; color: #93370d; }
    .declined { background: #fee4e2; color: #b42318; }
    .muted { color: #667085; }
    .split { display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; }
    .mini { padding: 12px; background: #f7f9fc; border: 1px solid #e3e9f1; border-radius: 8px; }
    .mini span { display: block; color: #667085; font-size: 12px; font-weight: 700; }
    .mini strong { display: block; margin-top: 5px; font-size: 20px; }
    @media (max-width: 1050px) { .grid { grid-template-columns: repeat(2, minmax(0, 1fr)); } .layout { grid-template-columns: 1fr; } }
    @media (max-width: 680px) { header { align-items: flex-start; flex-direction: column; padding: 14px 16px; } main { padding: 14px 16px 22px; } .grid, .split { grid-template-columns: 1fr; } .actions { justify-content: flex-start; } canvas { height: 260px; } table { font-size: 12px; } }
  </style>
</head>
<body>
  <header>
    <div>
      <h1>Sendertistics Analytics</h1>
      <p class="subtitle" id="subtitle">Загрузка данных</p>
    </div>
    <div class="actions">
      <button data-period="day" class="active">День</button>
      <button data-period="week">Неделя</button>
      <button data-period="month">Месяц</button>
      <button data-period="all">Всё</button>
      <button class="icon" id="refresh" title="Обновить" aria-label="Обновить">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" aria-hidden="true"><path d="M20 12a8 8 0 1 1-2.34-5.66M20 4v6h-6" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>
      </button>
      <button class="icon" id="logout" title="Выйти" aria-label="Выйти">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" aria-hidden="true"><path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4M16 17l5-5-5-5M21 12H9" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>
      </button>
    </div>
  </header>
  <main>
    <section class="grid">
      <div class="card metric"><span>Выручка</span><strong id="revenue">0</strong><small>по подтверждённым оплатам</small></div>
      <div class="card metric"><span>Подписки</span><strong id="subscriptions">0</strong><small id="activeSubs">активных сейчас: 0</small></div>
      <div class="card metric"><span>Запуски рассылок</span><strong id="campaigns">0</strong><small id="activeCampaigns">активно сейчас: 0</small></div>
      <div class="card metric"><span>Сообщения</span><strong id="deliveries">0</strong><small>успешно отправлено</small></div>
    </section>
    <section class="layout">
      <div class="card">
        <div class="panel-title"><h2>Динамика</h2><span class="muted" id="chartLabel"></span></div>
        <canvas id="chart" width="1000" height="360"></canvas>
      </div>
      <div class="card">
        <div class="panel-title"><h2>Сводка периода</h2></div>
        <div class="split">
          <div class="mini"><span>Заявки</span><strong id="requests">0</strong></div>
          <div class="mini"><span>Ожидают</span><strong id="pending">0</strong></div>
          <div class="mini"><span>Отклонено</span><strong id="declined">0</strong></div>
        </div>
        <p class="muted" id="globalDue" style="margin:14px 0 0;"></p>
      </div>
    </section>
    <section class="card" style="margin-top:12px;">
      <div class="panel-title"><h2>Последние оплаты</h2><span class="muted">до 50 записей</span></div>
      <table>
        <thead><tr><th>Пользователь</th><th>Статус</th><th>Создано</th><th>Активна до</th></tr></thead>
        <tbody id="payments"></tbody>
      </table>
    </section>
  </main>
  <script>
    const state = { period: 'day', data: null };
    const fmt = new Intl.NumberFormat('ru-RU');
    const dateFmt = new Intl.DateTimeFormat('ru-RU', {day:'2-digit', month:'2-digit', year:'numeric', hour:'2-digit', minute:'2-digit'});
    const dayFmt = new Intl.DateTimeFormat('ru-RU', {day:'2-digit', month:'2-digit'});

    function parseDate(value) { return value ? new Date(value) : null; }
    function safeDate(value) { const d = parseDate(value); return d && !Number.isNaN(d) ? dateFmt.format(d) : '—'; }
    function statusLabel(status) {
      return {approved: 'Оплачено', pending: 'Ожидает', declined: 'Отклонено'}[status] || status || '—';
    }
    function escapeHtml(value) {
      return String(value ?? '').replace(/[&<>"']/g, char => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
      }[char]));
    }
    function setText(id, text) { document.getElementById(id).textContent = text; }

    async function load() {
      const response = await fetch(`/api/analytics?period=${state.period}`);
      if (response.status === 401) { location.href = '/login'; return; }
      state.data = await response.json();
      render();
    }

    function render() {
      const data = state.data;
      const cards = data.cards;
      setText('subtitle', `Период: ${data.period_title}. Обновлено ${new Date().toLocaleTimeString('ru-RU', {hour:'2-digit', minute:'2-digit'})}`);
      setText('revenue', cards.revenue_text);
      setText('subscriptions', fmt.format(cards.subscriptions));
      setText('activeSubs', `активных сейчас: ${fmt.format(cards.active_subscriptions)}`);
      setText('campaigns', fmt.format(cards.campaign_starts));
      setText('activeCampaigns', `активно сейчас: ${fmt.format(cards.active_campaigns)}`);
      setText('deliveries', fmt.format(cards.deliveries));
      setText('requests', fmt.format(cards.payment_requests));
      setText('pending', fmt.format(cards.pending));
      setText('declined', fmt.format(cards.declined));
      setText('globalDue', cards.latest_global_payment_due ? `Общая оплата активна до ${safeDate(cards.latest_global_payment_due)}` : 'Общая оплата не найдена');
      setText('chartLabel', data.period === 'day' ? 'сегодня' : 'по дням');
      drawChart(data.series);
      renderPayments(data.payments);
    }

    function drawChart(series) {
      const canvas = document.getElementById('chart');
      const ctx = canvas.getContext('2d');
      const w = canvas.width, h = canvas.height;
      ctx.clearRect(0, 0, w, h);
      const pad = {l: 46, r: 18, t: 24, b: 42};
      const plotW = w - pad.l - pad.r, plotH = h - pad.t - pad.b;
      const max = Math.max(1, ...series.flatMap(d => [d.subscriptions, d.campaigns, d.deliveries]));
      ctx.strokeStyle = '#d9e1ea'; ctx.lineWidth = 1;
      ctx.font = '12px system-ui';
      ctx.fillStyle = '#667085';
      for (let i = 0; i <= 4; i++) {
        const y = pad.t + plotH * i / 4;
        ctx.beginPath(); ctx.moveTo(pad.l, y); ctx.lineTo(w - pad.r, y); ctx.stroke();
        ctx.fillText(String(Math.round(max * (4 - i) / 4)), 8, y + 4);
      }
      const keys = [
        ['subscriptions', '#067647', 'Подписки'],
        ['campaigns', '#1565c0', 'Запуски'],
        ['deliveries', '#c11574', 'Сообщения']
      ];
      keys.forEach(([key, color]) => {
        ctx.strokeStyle = color; ctx.lineWidth = key === 'deliveries' ? 3 : 2;
        ctx.beginPath();
        series.forEach((d, i) => {
          const x = pad.l + (series.length === 1 ? plotW / 2 : plotW * i / (series.length - 1));
          const y = pad.t + plotH - (Number(d[key]) / max) * plotH;
          if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
        });
        ctx.stroke();
      });
      const step = Math.max(1, Math.ceil(series.length / 8));
      ctx.fillStyle = '#667085';
      series.forEach((d, i) => {
        if (i % step !== 0 && i !== series.length - 1) return;
        const x = pad.l + (series.length === 1 ? plotW / 2 : plotW * i / (series.length - 1));
        ctx.fillText(dayFmt.format(new Date(d.date)), Math.max(4, x - 16), h - 14);
      });
      let legendX = pad.l;
      keys.forEach(([, color, label]) => {
        ctx.fillStyle = color; ctx.fillRect(legendX, 8, 10, 10);
        ctx.fillStyle = '#344054'; ctx.fillText(label, legendX + 14, 17);
        legendX += ctx.measureText(label).width + 42;
      });
    }

    function renderPayments(payments) {
      const tbody = document.getElementById('payments');
      tbody.innerHTML = payments.map(payment => {
        const userName = escapeHtml(payment.full_name || '—');
        const username = payment.username ? ' @' + escapeHtml(payment.username) : '';
        const userId = escapeHtml(payment.user_id);
        const user = `${userName}${username}<br><span class="muted">ID ${userId}</span>`;
        const statusClass = ['approved', 'pending', 'declined'].includes(payment.status) ? payment.status : '';
        return `<tr>
          <td>${user}</td>
          <td><span class="status ${statusClass}">${escapeHtml(statusLabel(payment.status))}</span></td>
          <td>${safeDate(payment.created_at)}</td>
          <td>${safeDate(payment.expires_at)}</td>
        </tr>`;
      }).join('') || '<tr><td colspan="4" class="muted">Оплат пока нет</td></tr>';
    }

    document.querySelectorAll('[data-period]').forEach(button => {
      button.addEventListener('click', () => {
        state.period = button.dataset.period;
        document.querySelectorAll('[data-period]').forEach(item => item.classList.toggle('active', item === button));
        load();
      });
    });
    document.getElementById('refresh').addEventListener('click', load);
    document.getElementById('logout').addEventListener('click', async () => {
      await fetch('/api/logout', {method: 'POST'});
      location.href = '/login';
    });
    load();
  </script>
</body>
</html>"""


if __name__ == "__main__":
    port = int(os.getenv("PORT", os.getenv("ADMIN_WEB_PORT", "8080")))
    host = os.getenv("ADMIN_WEB_HOST", os.getenv("WEB_DASHBOARD_HOST", "0.0.0.0"))
    web.run_app(create_app(), host=host, port=port)
