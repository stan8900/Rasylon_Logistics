import asyncio
import json
import unittest
from datetime import timedelta
from unittest.mock import patch

from public_web import (
    BROWSER_LOGIN_STORE,
    OTP_STORE,
    browser_login_check_api,
    browser_login_start_api,
    fallback_user_id,
    normalize_card,
    normalize_phone,
    now_utc,
    payment_api,
    request_otp_api,
    verify_otp_api,
)


class FakeRequest:
    def __init__(self, payload=None, *, app=None, query=None, cookies=None) -> None:
        self._payload = payload or {}
        self.app = app or {}
        self.query = query or {}
        self.cookies = cookies or {}

    async def json(self):
        return self._payload


class FakePaymentStorage:
    def __init__(self) -> None:
        self.requests = []

    async def create_payment_request(self, **kwargs):
        self.requests.append(kwargs)
        return f"pay-{len(self.requests)}"


async def response_json(response):
    return json.loads(response.text)


AUTH_CASES = [
    ("normalize_plus_phone", lambda: normalize_phone("+998 91 764 77 68"), "+998917647768"),
    ("normalize_plain_phone", lambda: normalize_phone("998907777777"), "+998907777777"),
    ("normalize_00_phone", lambda: normalize_phone("00998907777777"), "+998907777777"),
    ("normalize_empty_phone", lambda: normalize_phone("abc"), None),
    ("normalize_card_spaced", lambda: normalize_card("9860 1701 1433 3116"), "9860 1701 1433 3116"),
    ("normalize_card_compact", lambda: normalize_card("9860170114333116"), "9860 1701 1433 3116"),
    ("normalize_card_short", lambda: normalize_card("123"), None),
    ("fallback_user_id", lambda: fallback_user_id("+998901234567"), 998901234567),
]


class PaymentAuthCaseTest(unittest.TestCase):
    def test_browser_login_start_returns_telegram_deep_link(self) -> None:
        async def runner() -> None:
            with patch("public_web.BOT_USERNAME", "atRasylon_bot"):
                data = await response_json(await browser_login_start_api(FakeRequest()))

            self.assertTrue(data["ok"])
            self.assertIn(data["token"], BROWSER_LOGIN_STORE)
            self.assertEqual(data["bot_url"], f"https://t.me/atRasylon_bot?start=login_{data['token']}")

        asyncio.run(runner())

    def test_browser_login_check_pending_confirmed_and_consumed(self) -> None:
        async def runner() -> None:
            token = "unit-token"
            BROWSER_LOGIN_STORE[token] = {
                "created_at": now_utc(),
                "expires_at": now_utc() + timedelta(seconds=300),
                "confirmed_user": None,
            }
            pending = await response_json(await browser_login_check_api(FakeRequest(query={"token": token})))
            self.assertEqual(pending["status"], "pending")

            BROWSER_LOGIN_STORE[token]["confirmed_user"] = {"id": 42, "username": "tester"}
            confirmed = await response_json(await browser_login_check_api(FakeRequest(query={"token": token})))
            self.assertEqual(confirmed["status"], "confirmed")
            self.assertEqual(confirmed["telegram_user"]["id"], 42)
            self.assertNotIn(token, BROWSER_LOGIN_STORE)

        asyncio.run(runner())

    def test_browser_login_check_requires_token(self) -> None:
        async def runner() -> None:
            response = await browser_login_check_api(FakeRequest())
            data = await response_json(response)
            self.assertEqual(response.status, 400)
            self.assertEqual(data["error"], "token_required")

        asyncio.run(runner())

    def test_request_otp_sends_to_callback_and_stores_hash(self) -> None:
        async def runner() -> None:
            sent = []

            async def sender(phone, otp, telegram_user_id):
                sent.append((phone, otp, telegram_user_id))

            with patch("public_web.secrets.randbelow", return_value=123456):
                response = await request_otp_api(
                    FakeRequest(
                        {"phone": "+998901111111", "telegram_user": {"id": 99}},
                        app={"otp_sender_callback": sender},
                    )
                )
            data = await response_json(response)

            self.assertTrue(data["ok"])
            self.assertEqual(sent, [("+998901111111", "123456", 99)])
            self.assertIn("+998901111111", OTP_STORE)

        asyncio.run(runner())

    def test_request_otp_rejects_missing_phone(self) -> None:
        async def runner() -> None:
            response = await request_otp_api(FakeRequest({"phone": ""}))
            data = await response_json(response)
            self.assertEqual(response.status, 400)
            self.assertEqual(data["error"], "phone_required")

        asyncio.run(runner())

    def test_request_otp_rate_limits_resend(self) -> None:
        async def runner() -> None:
            OTP_STORE["+998902222222"] = {
                "otp_hash": "hash",
                "expires_at": now_utc() + timedelta(seconds=300),
                "last_sent_at": now_utc(),
                "attempts": 0,
            }
            response = await request_otp_api(FakeRequest({"phone": "+998902222222"}))
            data = await response_json(response)
            self.assertEqual(response.status, 429)
            self.assertEqual(data["error"], "rate_limited")

        asyncio.run(runner())

    def test_verify_otp_accepts_current_code(self) -> None:
        async def runner() -> None:
            OTP_STORE.pop("+998903333333", None)
            with patch("public_web.secrets.randbelow", return_value=777):
                await request_otp_api(FakeRequest({"phone": "+998903333333"}))
            response = await verify_otp_api(FakeRequest({"phone": "+998903333333", "otp": "000777"}))
            data = await response_json(response)
            self.assertTrue(data["ok"])
            self.assertEqual(data["phone"], "+998903333333")

        asyncio.run(runner())

    def test_verify_otp_rejects_bad_code(self) -> None:
        async def runner() -> None:
            OTP_STORE.pop("+998904444444", None)
            with patch("public_web.secrets.randbelow", return_value=111111):
                await request_otp_api(FakeRequest({"phone": "+998904444444"}))
            response = await verify_otp_api(FakeRequest({"phone": "+998904444444", "otp": "222222"}))
            data = await response_json(response)
            self.assertEqual(response.status, 403)
            self.assertEqual(data["error"], "bad_otp")

        asyncio.run(runner())

    def test_payment_creates_request_with_telegram_user(self) -> None:
        async def runner() -> None:
            storage = FakePaymentStorage()
            response = await payment_api(
                FakeRequest(
                    {
                        "telegram_user": {"id": 268248500, "username": "rasul", "first_name": "Rasul"},
                        "telegram_phone": "+998901111111",
                        "card_number": "9860170114333116",
                        "card_name": "RASUL",
                    },
                    app={"storage": storage},
                )
            )
            data = await response_json(response)
            self.assertTrue(data["ok"])
            self.assertEqual(storage.requests[0]["user_id"], 268248500)
            self.assertEqual(storage.requests[0]["card_number"], "9860 1701 1433 3116")

        asyncio.run(runner())

    def test_payment_uses_phone_fallback_without_telegram_user(self) -> None:
        async def runner() -> None:
            storage = FakePaymentStorage()
            response = await payment_api(
                FakeRequest(
                    {
                        "telegram_phone": "+998901234567",
                        "card_number": "9860170114333116",
                        "card_name": "CLIENT",
                    },
                    app={"storage": storage},
                )
            )
            data = await response_json(response)
            self.assertTrue(data["ok"])
            self.assertEqual(storage.requests[0]["user_id"], 998901234567)

        asyncio.run(runner())

    def test_payment_rejects_missing_phone(self) -> None:
        async def runner() -> None:
            response = await payment_api(FakeRequest({"card_number": "9860170114333116", "card_name": "CLIENT"}, app={"storage": FakePaymentStorage()}))
            data = await response_json(response)
            self.assertEqual(response.status, 400)
            self.assertEqual(data["error"], "phone_required")

        asyncio.run(runner())

    def test_payment_rejects_bad_card(self) -> None:
        async def runner() -> None:
            response = await payment_api(FakeRequest({"telegram_phone": "+998901111111", "card_number": "123", "card_name": "CLIENT"}, app={"storage": FakePaymentStorage()}))
            data = await response_json(response)
            self.assertEqual(response.status, 400)
            self.assertEqual(data["error"], "bad_card")

        asyncio.run(runner())


def _make_simple_case(name: str, func, expected):
    def test(self: PaymentAuthCaseTest) -> None:
        self.assertEqual(func(), expected)

    test.__name__ = f"test_auth_payment_simple_{name}"
    return test


for _name, _func, _expected in AUTH_CASES:
    setattr(PaymentAuthCaseTest, f"test_auth_payment_simple_{_name}", _make_simple_case(_name, _func, _expected))


if __name__ == "__main__":
    unittest.main()
