import hashlib
import hmac
import json
import logging
import os
from typing import Any, Dict, List, Optional
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


async def index(request: web.Request) -> web.Response:
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
        }
    )


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
    return web.json_response(
        {
            "ok": True,
            "request_id": request_id,
            "bot_url": f"https://t.me/{BOT_USERNAME}" if BOT_USERNAME else None,
        }
    )


async def health(request: web.Request) -> web.Response:
    return web.json_response({"ok": True})


def create_app(storage: Optional[Any] = None) -> web.Application:
    app = web.Application()
    app["storage"] = storage or create_storage_from_env()
    app.router.add_get("/health", health)
    app.router.add_get("/assets/telegram.lottie", telegram_lottie)
    app.router.add_get("/", index)
    app.router.add_get("/app", index)
    app.router.add_get("/api/mini/config", config_api)
    app.router.add_post("/api/mini/admin-login", admin_login_api)
    app.router.add_post("/api/mini/payment", payment_api)
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
