import asyncio
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from app.daily_reports import (
    build_daily_report_text,
    calculate_daily_report_stats,
    collect_report_user_ids,
)
from app.storage import Storage


class DailyReportsTest(unittest.TestCase):
    def test_collect_report_user_ids_uses_auto_payments_and_sessions(self) -> None:
        data = {
            "auto": {"10": {}, "20": {}},
            "payments": {"abc": {"user_id": 30}, "bad": {"user_id": None}},
            "sessions": {"40": {"role": "admin"}},
        }

        self.assertEqual(collect_report_user_ids(data), [10, 20, 30, 40])

    def test_calculate_daily_report_stats_filters_by_user_and_local_date(self) -> None:
        timezone = ZoneInfo("Asia/Tashkent")
        stats = calculate_daily_report_stats(
            user_id=1,
            report_date=date(2026, 5, 23),
            timezone=timezone,
            auto_data={
                "is_enabled": True,
                "target_chat_ids": [-100, -200],
                "stats": {"sent_total": 9, "last_sent_at": "2026-05-23T18:00:00+05:00"},
            },
            delivery_events=[
                {"user_id": 1, "sent_count": 2, "delivered_at": "2026-05-23T08:00:00+05:00"},
                {"user_id": 1, "sent_count": 3, "delivered_at": "2026-05-22T23:30:00+00:00"},
                {"user_id": 2, "sent_count": 7, "delivered_at": "2026-05-23T08:00:00+05:00"},
            ],
            campaign_events=[
                {"user_id": 1, "started_at": "2026-05-23T07:50:00+05:00"},
                {"user_id": 1, "started_at": "2026-05-24T01:00:00+05:00"},
            ],
        )

        self.assertEqual(stats.deliveries, 5)
        self.assertEqual(stats.campaign_starts, 1)
        self.assertTrue(stats.active_now)
        self.assertEqual(stats.target_count, 2)
        self.assertEqual(stats.sent_total, 9)

    def test_build_daily_report_text_from_storage(self) -> None:
        async def runner() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                storage = Storage(Path(tmp) / "storage.db")
                await storage.set_auto_message(1535189323, "hello")
                await storage.upsert_known_chat(-100, "First")
                await storage.upsert_known_chat(-200, "Second")
                await storage.upsert_known_chat(-300, "Third")
                await storage.set_target_chats(1535189323, [-100, -200, -300])
                await storage.record_auto_campaign_start(
                    1535189323,
                    started_at="2026-05-23T08:00:00+05:00",
                )
                await storage.update_stats(
                    1535189323,
                    sent=3,
                    errors=[],
                    delivered_at="2026-05-23T08:10:00+05:00",
                )

                text = await build_daily_report_text(
                    storage,
                    1535189323,
                    date(2026, 5, 23),
                    ZoneInfo("Asia/Tashkent"),
                )

                self.assertIn("Ежедневный отчёт по рассылке за 23.05.2026", text)
                self.assertIn("Отправлено сообщений: <b>3</b>", text)
                self.assertIn("Запусков рассылки: <b>1</b>", text)
                self.assertIn("Выбранных групп: <b>3</b>", text)
                self.assertIn("<code>██████████</code>", text)

        asyncio.run(runner())

    def test_report_date_for_midnight_sleep_uses_previous_day(self) -> None:
        import os

        os.environ.setdefault("BOT_TOKEN", "123:abc")
        import bot

        original_from = bot.BOT_SLEEP_FROM_RAW
        original_timezone = bot.BOT_SLEEP_TIMEZONE_RAW
        try:
            bot.BOT_SLEEP_FROM_RAW = "00:00"
            bot.BOT_SLEEP_TIMEZONE_RAW = "Asia/Tashkent"
            report_date = bot.report_date_for_sleep_start(
                datetime(2026, 5, 24, 0, 2, tzinfo=ZoneInfo("Asia/Tashkent"))
            )
            self.assertEqual(report_date, date(2026, 5, 23))
        finally:
            bot.BOT_SLEEP_FROM_RAW = original_from
            bot.BOT_SLEEP_TIMEZONE_RAW = original_timezone


if __name__ == "__main__":
    unittest.main()
