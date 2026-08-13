import asyncio
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from app.storage import Storage


class StorageExtensionsTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        db_path = Path(self._tmp.name) / "storage.db"
        self.storage = Storage(db_path)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_register_and_list_audience_dump(self) -> None:
        async def runner() -> None:
            dump = await self.storage.register_audience_dump(
                123,
                source="test_channel",
                file_path="/tmp/sample.txt",
                total_users=42,
            )
            self.assertEqual(dump["source"], "test_channel")
            dumps = await self.storage.list_audience_dumps(123, limit=5)
            self.assertEqual(len(dumps), 1)
            self.assertEqual(dumps[0]["total_users"], 42)
            self.assertTrue(dumps[0]["file_path"].endswith("sample.txt"))

        asyncio.run(runner())

    def test_create_update_and_list_invite_jobs(self) -> None:
        async def runner() -> None:
            job = await self.storage.create_invite_job(
                555,
                target_chat="@target",
                usernames_file="/tmp/users.txt",
                settings={"thread_limit": 2},
                total_users=10,
            )
            self.assertEqual(job["status"], "pending")
            await self.storage.update_invite_job(
                job["id"],
                status="running",
                invited_count=4,
                failed_count=1,
            )
            updated = await self.storage.get_invite_job(job["id"])
            self.assertIsNotNone(updated)
            self.assertEqual(updated["status"], "running")
            self.assertEqual(updated["invited_count"], 4)
            jobs = await self.storage.list_invite_jobs(555)
            self.assertEqual(len(jobs), 1)
            self.assertEqual(jobs[0]["failed_count"], 1)

        asyncio.run(runner())

    def test_update_account_proxy(self) -> None:
        async def runner() -> None:
            account = await self.storage.create_user_account(
                42,
                phone="+10000000000",
                session="session-string",
                title="Test",
                username="tester",
            )
            proxy = {
                "type": "socks5",
                "host": "127.0.0.1",
                "port": 9050,
                "username": "user",
                "password": "pass",
            }
            updated = await self.storage.update_user_account_proxy(42, account["id"], proxy=proxy)
            self.assertEqual(updated["proxy_type"], "socks5")
            self.assertEqual(updated["proxy_host"], "127.0.0.1")
            self.assertEqual(updated["proxy_port"], 9050)
            self.assertEqual(updated["proxy_username"], "user")
            self.assertEqual(updated["proxy_password"], "pass")
            cleared = await self.storage.update_user_account_proxy(42, account["id"], proxy=None)
            self.assertIsNotNone(cleared)
            self.assertIsNone(cleared["proxy_type"])
            self.assertIsNone(cleared["proxy_host"])

        asyncio.run(runner())

    def test_find_account_owner_by_phone_matches_normalized_number(self) -> None:
        async def runner() -> None:
            await self.storage.create_user_account(
                268248500,
                phone="+998 91 764 77 68",
                session="session-string",
                title="Rasylon",
                username="rasylon2",
            )

            self.assertEqual(await self.storage.find_account_owner_by_phone("+998917647768"), 268248500)
            self.assertEqual(await self.storage.find_account_owner_by_phone("91 764 77 68"), 268248500)
            self.assertIsNone(await self.storage.find_account_owner_by_phone("+998900000000"))

        asyncio.run(runner())

    def test_account_specific_targets_are_separate_from_bot_targets(self) -> None:
        async def runner() -> None:
            await self.storage.upsert_known_chat(-1001, "Bot group")
            account = await self.storage.create_user_account(
                42,
                phone="+10000000000",
                session="session-string",
                title="Personal",
                username="personal",
            )
            account_id = int(account["id"])
            await self.storage.replace_account_chats(
                account_id,
                [(-2001, "Personal group"), (-2002, "Second personal group")],
            )

            await self.storage.set_target_chats(42, [-1001])
            bot_auto = await self.storage.get_auto(42)
            self.assertEqual(bot_auto["target_chat_ids"], [-1001])

            await self.storage.set_user_sender_account(42, account_id)
            await self.storage.set_target_chats(42, [-2002], account_id=account_id)
            account_auto = await self.storage.get_auto(42)
            self.assertEqual(account_auto["sender_account_id"], account_id)
            self.assertEqual(account_auto["target_chat_ids"], [-2002])

            await self.storage.set_user_sender_account(42, None)
            bot_auto_again = await self.storage.get_auto(42)
            self.assertEqual(bot_auto_again["target_chat_ids"], [-1001])

        asyncio.run(runner())

    def test_record_and_filter_logistics_messages_by_chat(self) -> None:
        async def runner() -> None:
            classification = {
                "intent": "cargo_searching_driver",
                "current_location": "Ташкент",
                "destination": "Алматы",
                "vehicle_type": "тент",
                "availability": "сегодня",
                "source": "local_ai",
                "should_map": True,
            }

            inserted = await self.storage.record_logistics_message(
                chat_id=-100,
                message_id=10,
                chat_title="Loads",
                chat_username="loads_chat",
                author_id=55,
                author_name="Dispatcher",
                author_username="dispatcher",
                text="Ташкент, нужен водитель на Алматы, тент сегодня",
                classification=classification,
                created_at="2026-08-13T10:00:00",
            )
            duplicate = await self.storage.record_logistics_message(
                chat_id=-100,
                message_id=10,
                chat_title="Loads",
                chat_username="loads_chat",
                author_id=55,
                author_name="Dispatcher",
                author_username="dispatcher",
                text="Ташкент, нужен водитель на Алматы, тент сегодня",
                classification=classification,
                created_at="2026-08-13T10:00:00",
            )

            self.assertTrue(inserted)
            self.assertFalse(duplicate)
            self.assertEqual(await self.storage.list_logistics_messages(chat_ids=[]), [])

            messages = await self.storage.list_logistics_messages(chat_ids=[-100])
            self.assertEqual(len(messages), 1)
            self.assertEqual(messages[0]["intent"], "cargo_searching_driver")
            self.assertEqual(messages[0]["classification"]["destination"], "Алматы")
            self.assertEqual(await self.storage.list_logistics_messages(chat_ids=[-200]), [])

        asyncio.run(runner())

    def test_logistics_message_storage_handles_15_filtering_cases(self) -> None:
        async def runner() -> None:
            cases = [
                (-1001, 1, "cargo_searching_driver", "Ташкент", "Алматы", "тент"),
                (-1001, 2, "driver_searching_cargo", "Ташкент", "Алматы", "тент"),
                (-1002, 3, "cargo_searching_driver", "Самарканд", "Ташкент", "рефрижератор"),
                (-1002, 4, "driver_searching_cargo", "Самарканд", "Ташкент", "рефрижератор"),
                (-1003, 5, "cargo_searching_driver", "Алматы", "Бишкек", "фура"),
                (-1003, 6, "driver_searching_cargo", "Алматы", "Ташкент", "изотерм"),
                (-1004, 7, "cargo_searching_driver", "Бишкек", "Ташкент", "изотерм"),
                (-1004, 8, "driver_searching_cargo", "Бишкек", "Москва", "фура"),
                (-1005, 9, "cargo_searching_driver", "Москва", "Самарканд", "газель"),
                (-1005, 10, "driver_searching_cargo", "Москва", "Алматы", "газель"),
                (-1006, 11, "cargo_searching_driver", "Ташкент", "Бишкек", "тент"),
                (-1007, 12, "cargo_searching_driver", "Самарканд", "Алматы", "рефрижератор"),
                (-1008, 13, "driver_searching_cargo", "Алматы", "Москва", "фура"),
                (-1009, 14, "cargo_searching_driver", "Бишкек", "Алматы", "изотерм"),
                (-1010, 15, "driver_searching_cargo", "Ташкент", "Москва", "тент"),
            ]

            self.assertEqual(len(cases), 15)
            for chat_id, message_id, intent, location, destination, vehicle in cases:
                with self.subTest(message_id=message_id):
                    inserted = await self.storage.record_logistics_message(
                        chat_id=chat_id,
                        message_id=message_id,
                        chat_title=f"Chat {chat_id}",
                        chat_username=f"chat_{abs(chat_id)}",
                        author_id=message_id,
                        author_name=f"Author {message_id}",
                        author_username=f"author_{message_id}",
                        text=f"{location} -> {destination}",
                        classification={
                            "intent": intent,
                            "current_location": location,
                            "destination": destination,
                            "vehicle_type": vehicle,
                            "availability": "сегодня",
                            "source": "local_ai",
                            "should_map": True,
                        },
                        created_at=f"2026-08-13T10:{message_id:02d}:00",
                    )
                    self.assertTrue(inserted)

            selected = await self.storage.list_logistics_messages(chat_ids=[-1001, -1002, -1003])
            self.assertEqual(len(selected), 6)
            self.assertEqual({message["chat_id"] for message in selected}, {-1001, -1002, -1003})

            cargo_requests = await self.storage.list_logistics_messages(
                intent="cargo_searching_driver",
                chat_ids=[chat_id for chat_id, *_ in cases],
            )
            self.assertEqual(len(cargo_requests), 8)
            self.assertTrue(all(message["intent"] == "cargo_searching_driver" for message in cargo_requests))

            limited = await self.storage.list_logistics_messages(limit=3, chat_ids=[chat_id for chat_id, *_ in cases])
            self.assertEqual(len(limited), 3)
            self.assertEqual([message["message_id"] for message in limited], [15, 14, 13])

            duplicate = await self.storage.record_logistics_message(
                chat_id=-1001,
                message_id=1,
                chat_title="Duplicate",
                chat_username="duplicate",
                author_id=1,
                author_name="Duplicate",
                author_username="duplicate",
                text="duplicate",
                classification={
                    "intent": "cargo_searching_driver",
                    "current_location": "Ташкент",
                    "destination": "Алматы",
                    "source": "local_ai",
                    "should_map": True,
                },
            )
            self.assertFalse(duplicate)
            self.assertEqual(await self.storage.list_logistics_messages(chat_ids=[]), [])

        asyncio.run(runner())

    def test_disable_all_auto_turns_off_every_user(self) -> None:
        async def runner() -> None:
            await self.storage.set_auto_message(1, "first")
            await self.storage.set_auto_interval(1, 10)
            await self.storage.set_auto_enabled(1, True)
            await self.storage.set_auto_message(2, "second")
            await self.storage.set_auto_interval(2, 20)
            await self.storage.set_auto_enabled(2, True)

            disabled_count = await self.storage.disable_all_auto()

            self.assertEqual(disabled_count, 2)
            self.assertFalse((await self.storage.get_auto(1))["is_enabled"])
            self.assertFalse((await self.storage.get_auto(2))["is_enabled"])

        asyncio.run(runner())

    def test_auto_delivery_reservation_limits_daily_and_per_chat_rate(self) -> None:
        async def runner() -> None:
            first_reserved, first_reason = await self.storage.reserve_auto_delivery(
                user_id=1,
                chat_id=-100,
                day_key="2026-04-29",
                now_iso="2026-04-29T08:00:00+05:00",
                daily_limit=2,
                chat_interval_seconds=60,
            )
            self.assertTrue(first_reserved)
            self.assertEqual(first_reason, "reserved")

            second_reserved, second_reason = await self.storage.reserve_auto_delivery(
                user_id=2,
                chat_id=-100,
                day_key="2026-04-29",
                now_iso="2026-04-29T08:00:30+05:00",
                daily_limit=2,
                chat_interval_seconds=60,
            )
            self.assertFalse(second_reserved)
            self.assertEqual(second_reason, "chat_rate_limit")

            third_reserved, _ = await self.storage.reserve_auto_delivery(
                user_id=1,
                chat_id=-101,
                day_key="2026-04-29",
                now_iso="2026-04-29T08:01:01+05:00",
                daily_limit=2,
                chat_interval_seconds=60,
            )
            self.assertTrue(third_reserved)

            fourth_reserved, fourth_reason = await self.storage.reserve_auto_delivery(
                user_id=1,
                chat_id=-102,
                day_key="2026-04-29",
                now_iso="2026-04-29T08:02:01+05:00",
                daily_limit=2,
                chat_interval_seconds=60,
            )
            self.assertFalse(fourth_reserved)
            self.assertEqual(fourth_reason, "daily_limit")

        asyncio.run(runner())

    def test_admin_analytics_counts_campaigns_deliveries_and_active_auto(self) -> None:
        async def runner() -> None:
            await self.storage.record_auto_campaign_start(
                1,
                started_at="2026-05-04T08:00:00",
            )
            await self.storage.record_auto_campaign_start(
                2,
                started_at="2026-04-01T08:00:00",
            )
            await self.storage.update_stats(
                1,
                sent=1,
                errors=[],
                delivered_at="2026-05-04T08:00:00",
            )
            await self.storage.update_stats(
                1,
                sent=1,
                errors=[],
                delivered_at="2026-04-01T08:00:00",
            )
            await self.storage.set_auto_message(1, "first")
            await self.storage.set_auto_enabled(1, True)

            since = datetime.fromisoformat("2026-05-01T00:00:00")

            self.assertEqual(await self.storage.count_auto_campaign_starts(), 2)
            self.assertEqual(await self.storage.count_auto_campaign_starts(since=since), 1)
            self.assertEqual(await self.storage.count_auto_deliveries(), 2)
            self.assertEqual(await self.storage.count_auto_deliveries(since=since), 1)
            self.assertEqual(await self.storage.count_active_auto_campaigns(), 1)

        asyncio.run(runner())


if __name__ == "__main__":
    unittest.main()
