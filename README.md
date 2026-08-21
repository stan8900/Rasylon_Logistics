# Rasylon Logistics

Telegram logistics platform for auto-mailing cargo/route offers to selected Telegram groups, parsing logistics messages, and managing user access from a web dashboard and Telegram bot.

## Production

- Frontend: https://www.rasylon.uz
- Frontend Railway URL: https://rasylonlogisticsfrontend-production.up.railway.app
- Backend Railway URL: https://rasylonlogistics-production.up.railway.app
- Telegram bot: https://t.me/atRasylon_bot
- Railway project: `bb2fa46e-2b2f-4796-8b71-70e4fc55bc0e`
- Railway environment: `production` / `4424d856-16c5-4d0a-ba38-a41c389f7efb`
- Backend service: `Rasylon_Logistics` / `a9f6546f-4efd-46d5-aa32-e2d5f5752b3f`
- Frontend service: `Rasylon_Logistics_Frontend` / `27cfed9b-7772-4ca6-8bc4-2d6d89a9cfb6`

## What It Does

- Auto-mailing to selected Telegram groups.
- Group selection per user and per sender account.
- Message interval control.
- Start/stop mailing from Telegram bot and web dashboard.
- Browser login through Telegram deep links.
- OTP login fallback through Telegram.
- Payment request creation and admin approval flow.
- Real logistics message parsing and classification.
- Dashboard tabs for map, mailing, parsed messages, and profile.
- Yandex Maps support on the frontend.
- Mandatory onboarding spotlight tour.
- Shared Telegram user-session safeguards for high-concurrency starts.

## Architecture

The app has two deployable services.

Backend:

- Python 3.
- `aiogram` bot.
- `aiohttp` public API and Mini App routes.
- `Telethon` user-session sender.
- PostgreSQL on Railway in production.
- Local SQLite for tests/dev.

Frontend:

- TypeScript.
- Vite.
- Static dashboard served by `vite preview` on Railway.

Core files:

- `bot.py` - Telegram bot handlers, mailing actions, admin notifications.
- `public_web.py` - public API, browser login, OTP, payment/order APIs, CORS.
- `railway_start.py` - starts web API and bot polling together.
- `app/auto_sender.py` - auto-mailing worker and limits.
- `app/user_sender.py` - Telethon user-session wrapper and send serialization.
- `app/storage.py` - SQLite/Postgres storage layer.
- `frontend/src/main.ts` - dashboard UI logic.
- `frontend/src/styles.css` - frontend theme and layout.
- `frontend/vite.config.ts` - Vite allowed host list.

## Session Safety

Telegram user sessions are fragile. A single `TG_USER_SESSION` must not be used from multiple services, multiple IP addresses, or parallel send loops.

Current safeguards:

- All `UserSender.send_message()` calls are serialized with one async lock per Telethon session.
- One Telegram user-session sends one message at a time.
- A global per-session delay is enforced between sends.
- Invalid/revoked/duplicated sessions are marked invalid and disconnected.
- Auto-sender disables campaigns when the selected personal/shared sender becomes invalid.
- Group and readiness checks run before mailing starts.
- Delivery limits prevent repeated sends to the same chat too quickly.

Important production rule:

- Do not run the same `TG_USER_SESSION` in Sendistics and Rasylon_Logistics at the same time.
- If a session is moved to another service, stop the old service or remove the env var there first.

Recommended production pacing:

```txt
TG_USER_SEND_MIN_INTERVAL_SECONDS=180
```

The default in code is `60` seconds if the env var is missing.

## Mailing Limits

Current constants in `app/auto_sender.py`:

```txt
AUTO_DAILY_MESSAGE_LIMIT=50
AUTO_CHAT_MIN_INTERVAL_SECONDS=600
AUTO_SEND_PACE_SECONDS=600
AUTO_CHAT_REFRESH_COOLDOWN_SECONDS=600
```

Meaning:

- Daily cap is 50 messages per user campaign.
- Same chat cannot receive more than 1 message per 10 minutes.
- Auto-sender waits 10 minutes between target sends inside a campaign.
- Chat list refresh is cooled down for 10 minutes.

`TG_USER_SEND_MIN_INTERVAL_SECONDS` adds an extra global safety gate per Telegram user-session.

## Browser Login

The browser login flow:

1. Frontend calls `POST /api/auth/browser-login/start`.
2. Backend creates a short-lived token.
3. Backend returns `https://t.me/atRasylon_bot?start=login_<token>`.
4. User opens Telegram and presses Start.
5. Bot confirms the token through `confirm_browser_login()`.
6. Frontend polls `GET /api/auth/browser-login/check?token=...`.
7. Backend returns signed auth token and sets an auth cookie.

Required env:

```txt
BOT_USERNAME=atRasylon_bot
BOT_TOKEN=...
AUTH_SESSION_SECRET=...
AUTH_COOKIE_SECURE=true
```

## OTP Login

OTP is sent through Telegram only to the Telegram user/account linked to the phone.

OTP protections:

- TTL: `OTP_TTL_SECONDS` default `300`.
- Resend cooldown: `OTP_RESEND_SECONDS` default `60`.
- Max attempts: `OTP_MAX_ATTEMPTS` default `5`.
- OTP hashes are stored server-side, not plaintext.

If phone is not linked to Telegram, API returns `telegram_not_linked`.

## Payments

Payment requests are created from the Mini App/web flow and sent to admins for confirmation.

Default config:

```txt
PAYMENT_AMOUNT=100000
PAYMENT_CURRENCY=UZS
PAYMENT_VALID_DAYS=30
PAYMENT_CARD_TARGET=<payment-card-number>
ADMIN_IDS=268248500
```

Users need a recent confirmed user payment and a recent global balance payment before auto-mailing can start.

## DNS And Domains

DNS is managed in Cloudflare. The registrar remains Airnet.

Cloudflare nameservers:

```txt
beau.ns.cloudflare.com
dell.ns.cloudflare.com
```

Cloudflare DNS records:

```txt
CNAME  @    xxs1wqej.up.railway.app      DNS only
CNAME  www  7e1usk1j.up.railway.app      DNS only
TXT    _railway-verify      railway-verify=<root-verification-token>
TXT    _railway-verify.www  railway-verify=<www-verification-token>
```

Use `DNS only` for Railway records, not Cloudflare proxy mode.

Vite allowed hosts:

```txt
rasylonlogisticsfrontend-production.up.railway.app
rasylon.uz
www.rasylon.uz
```

Backend CORS origins:

```txt
CORS_ALLOW_ORIGIN=https://rasylonlogisticsfrontend-production.up.railway.app,https://rasylon.uz,https://www.rasylon.uz
MINI_APP_URL=https://www.rasylon.uz
```

## Required Railway Env

Backend service `Rasylon_Logistics`:

```txt
BOT_TOKEN=...
ADMIN_BOT_TOKEN=...
BOT_USERNAME=atRasylon_bot
TELEGRAM_BOT_USERNAME=atRasylon_bot
ADMIN_IDS=268248500
ADMIN_CODE=...
DATABASE_URL=...
DATABASE_URL_REQUIRED=true
WEB_DASHBOARD_ENABLED=true
WEB_DASHBOARD_HOST=0.0.0.0
WEB_DASHBOARD_SECRET=...
WEB_DASHBOARD_PASSWORD=...
MINI_APP_URL=https://www.rasylon.uz
CORS_ALLOW_ORIGIN=https://rasylonlogisticsfrontend-production.up.railway.app,https://rasylon.uz,https://www.rasylon.uz
YANDEX_MAPS_API_KEY=...
TG_USER_API_ID=...
TG_USER_API_HASH=...
TG_USER_SESSION=...
TG_USER_FOLDER_NAME=Logistika_1
TG_USER_SEND_MIN_INTERVAL_SECONDS=180
```

Optional:

```txt
TG_USER_PROXY=...
AUTH_SESSION_SECRET=...
AUTH_SESSION_TTL_SECONDS=604800
AUTH_COOKIE_NAME=rasylon_auth_token
AUTH_COOKIE_SECURE=true
OTP_TTL_SECONDS=300
OTP_RESEND_SECONDS=60
OTP_MAX_ATTEMPTS=5
SUPPORT_AGENT_USERNAME=@rasylon_support
```

Frontend service `Rasylon_Logistics_Frontend`:

```txt
VITE_API_BASE_URL=https://rasylonlogistics-production.up.railway.app
```

If `VITE_API_BASE_URL` is not set, frontend defaults to the Railway backend URL.

## Local Development

Backend:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python railway_start.py
```

Frontend:

```bash
cd frontend
npm ci
npm run dev
```

Generate a Telethon session:

```bash
python generate_session.py
```

Do not generate sessions repeatedly unless the old one is invalid. Prefer keeping one stable session, one service, one IP/proxy.

## Tests

Run all tests:

```bash
python -m unittest discover
```

Current suite coverage:

- 50 mailing/group/message/limit cases in `tests/test_bulk_mailing_cases.py`.
- 20 payment/auth/register/OTP cases in `tests/test_payment_auth_cases.py`.
- Session serialization tests in `tests/test_session_safeguards.py`.
- Existing invalid session, OTP routing, storage, logistics message, public payload, and daily report tests.

Expected current result:

```txt
Ran 108 tests
OK
```

Locale warnings about `LC_ALL=C.UTF-8` are harmless in the current local environment.

## Deployment

Backend deploy:

```bash
railway up \
  --project bb2fa46e-2b2f-4796-8b71-70e4fc55bc0e \
  --environment 4424d856-16c5-4d0a-ba38-a41c389f7efb \
  --service a9f6546f-4efd-46d5-aa32-e2d5f5752b3f \
  --detach --yes
```

Frontend deploy:

```bash
cd frontend
railway up \
  --project bb2fa46e-2b2f-4796-8b71-70e4fc55bc0e \
  --environment 4424d856-16c5-4d0a-ba38-a41c389f7efb \
  --service 27cfed9b-7772-4ca6-8bc4-2d6d89a9cfb6 \
  --detach --yes
```

Check deployment:

```bash
railway deployment list \
  --project bb2fa46e-2b2f-4796-8b71-70e4fc55bc0e \
  --environment 4424d856-16c5-4d0a-ba38-a41c389f7efb \
  --service <service-id> \
  --json
```

## Troubleshooting

### `Blocked request. This host is not allowed.`

Add the host to `frontend/vite.config.ts`:

```ts
preview: {
  allowedHosts: ["rasylonlogisticsfrontend-production.up.railway.app", "rasylon.uz", "www.rasylon.uz"],
}
```

Deploy frontend after changing this.

### `Не удалось создать ссылку для входа`

Check:

- Backend is deployed.
- `BOT_USERNAME=atRasylon_bot`.
- `CORS_ALLOW_ORIGIN` includes the frontend origin.
- `POST /api/auth/browser-login/start` returns `bot_url`.

Manual check:

```bash
curl -sS https://rasylonlogistics-production.up.railway.app/api/auth/browser-login/start \
  -H 'Origin: https://www.rasylon.uz' \
  -H 'Content-Type: application/json' \
  --data '{}'
```

### `The authorization key was used under two different IP addresses`

This means one Telegram user-session was used from more than one runtime/IP at the same time.

Actions:

- Stop the old service using the session.
- Remove duplicate `TG_USER_SESSION` from other services.
- Regenerate the session only after the old runtime is stopped.
- Keep `TG_USER_SEND_MIN_INTERVAL_SECONDS` conservative.
- Prefer one stable proxy/IP when proxy support is enabled.

### Root domain still opens Airnet or Linkserv

Check nameservers:

```bash
dig +short NS rasylon.uz
```

Expected:

```txt
beau.ns.cloudflare.com.
dell.ns.cloudflare.com.
```

Check DNS:

```bash
dig +short rasylon.uz
dig +short www.rasylon.uz
```

Cloudflare and Railway propagation can take time. Keep Railway records `DNS only`.

### Telegram session is invalid

Regenerate:

```bash
python generate_session.py
```

Then update Railway `TG_USER_SESSION` and redeploy backend. Do not run the same session in another service.
