import asyncio
import os
import unittest
from unittest.mock import patch

os.environ.setdefault("BOT_TOKEN", "123:abc")

import railway_start


class FakeStorage:
    def __init__(self, owner_id=None) -> None:
        self.owner_id = owner_id
        self.requested_phone = None

    async def find_account_owner_by_phone(self, phone: str):
        self.requested_phone = phone
        return self.owner_id


class FakeBot:
    def __init__(self) -> None:
        self.sent = []

    async def send_message(self, user_id: int, text: str) -> None:
        self.sent.append((user_id, text))


class OtpRoutingTest(unittest.TestCase):
    def test_otp_does_not_fallback_to_current_browser_user(self) -> None:
        async def runner() -> None:
            storage = FakeStorage(owner_id=None)
            bot = FakeBot()
            with patch.object(railway_start.bot_module, "storage", storage), patch.object(
                railway_start.bot_module, "bot", bot
            ):
                with self.assertRaises(LookupError):
                    await railway_start.send_otp_to_telegram("+998901111111", "123456", telegram_user_id=268248500)

            self.assertEqual(storage.requested_phone, "+998901111111")
            self.assertEqual(bot.sent, [])

        asyncio.run(runner())

    def test_otp_routes_to_owner_found_by_phone(self) -> None:
        async def runner() -> None:
            storage = FakeStorage(owner_id=777)
            bot = FakeBot()
            with patch.object(railway_start.bot_module, "storage", storage), patch.object(
                railway_start.bot_module, "bot", bot
            ):
                await railway_start.send_otp_to_telegram("+998901111111", "123456", telegram_user_id=268248500)

            self.assertEqual(bot.sent[0][0], 777)
            self.assertIn("123456", bot.sent[0][1])

        asyncio.run(runner())


if __name__ == "__main__":
    unittest.main()
