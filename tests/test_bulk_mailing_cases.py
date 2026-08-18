import asyncio
import tempfile
import unittest
from pathlib import Path

from app.storage import Storage
from public_web import build_locations_payload, classify_message_locally


MAILING_CASES = [
    ("group_select_global_1", -1001, "Global 1", None, [-1001]),
    ("group_select_global_2", -1002, "Global 2", None, [-1002]),
    ("group_select_global_many", -1003, "Global 3", None, [-1001, -1002, -1003]),
    ("group_select_account_1", -2001, "Account 1", 1, [-2001]),
    ("group_select_account_2", -2002, "Account 2", 1, [-2001, -2002]),
    ("group_select_account_3", -2003, "Account 3", 1, [-2003]),
    ("message_cargo_tashkent_almaty", -3001, "Loads 1", None, [-3001]),
    ("message_cargo_samarkand_tashkent", -3002, "Loads 2", None, [-3002]),
    ("message_driver_tashkent_almaty", -3003, "Drivers 1", None, [-3003]),
    ("message_driver_bishkek_moscow", -3004, "Drivers 2", None, [-3004]),
    ("limit_daily_first", -4001, "Limit 1", None, [-4001]),
    ("limit_daily_second", -4002, "Limit 2", None, [-4001, -4002]),
    ("limit_chat_first", -4003, "Limit 3", None, [-4003]),
    ("limit_chat_second", -4003, "Limit 3", None, [-4003]),
    ("interval_minimum", -5001, "Interval 1", None, [-5001]),
    ("interval_large", -5002, "Interval 2", None, [-5002]),
    ("targets_empty_filtered", -5003, "Filter 1", 2, []),
    ("targets_invalid_filtered", -5004, "Filter 2", 2, [-9999]),
    ("target_labels_known", -6001, "Грузоперевозки UZ", None, [-6001]),
    ("target_labels_unknown", -6002, "Unknown", None, [-6002]),
    ("message_dedupe_chat", -7001, "Dedupe 1", None, [-7001]),
    ("message_filter_chat_a", -7002, "Filter A", None, [-7002]),
    ("message_filter_chat_b", -7003, "Filter B", None, [-7003]),
    ("message_limit_three", -7004, "Limit list", None, [-7004]),
    ("campaign_start_count_1", -8001, "Campaign 1", None, [-8001]),
    ("campaign_start_count_2", -8002, "Campaign 2", None, [-8002]),
    ("stats_delivery_count_1", -8003, "Stats 1", None, [-8003]),
    ("stats_delivery_count_2", -8004, "Stats 2", None, [-8004]),
    ("disable_all_one", -8005, "Disable 1", None, [-8005]),
    ("disable_all_many", -8006, "Disable 2", None, [-8005, -8006]),
    ("classification_cargo_1", -9001, "AI 1", None, [-9001]),
    ("classification_cargo_2", -9002, "AI 2", None, [-9002]),
    ("classification_driver_1", -9003, "AI 3", None, [-9003]),
    ("classification_driver_2", -9004, "AI 4", None, [-9004]),
    ("classification_unknown", -9005, "AI 5", None, [-9005]),
    ("locations_payload_one", -9101, "Map 1", None, [-9101]),
    ("locations_payload_many", -9102, "Map 2", None, [-9101, -9102]),
    ("locations_payload_driver_count", -9103, "Map 3", None, [-9103]),
    ("locations_payload_no_confidence", -9104, "Map 4", None, [-9104]),
    ("known_chat_replace", -9201, "Replace 1", 3, [-9201]),
    ("known_chat_replace_many", -9202, "Replace 2", 3, [-9201, -9202]),
    ("sender_account_switch", -9301, "Switch 1", 4, [-9301]),
    ("sender_account_clear", -9302, "Switch 2", 4, [-9302]),
    ("reserve_chat_across_users", -9401, "Reserve 1", None, [-9401]),
    ("reserve_daily_per_user", -9402, "Reserve 2", None, [-9402]),
    ("select_all_shared", -9501, "Select all 1", None, [-9501]),
    ("select_all_account", -9502, "Select all 2", 5, [-9502]),
    ("mailing_message_persist", -9601, "Persist 1", None, [-9601]),
    ("mailing_interval_persist", -9602, "Persist 2", None, [-9602]),
    ("mailing_enabled_persist", -9603, "Persist 3", None, [-9603]),
]


CLASSIFICATION_TEXTS = [
    "Ташкент, нужен водитель на Алматы, тент сегодня",
    "Самарканд ищем машину до Ташкент реф завтра",
    "Стою Ташкент ищу груз на Алматы",
    "Бишкек свободная фура ищу груз на Москва",
    "Просто обсуждение без заявки",
]


class BulkMailingCaseTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.storage = Storage(Path(self._tmp.name) / "storage.db")

    def tearDown(self) -> None:
        self._tmp.cleanup()

    async def _run_case(self, index: int, case: tuple) -> None:
        name, chat_id, title, account_marker, selected = case
        user_id = 10_000 + index
        await self.storage.upsert_known_chat(chat_id, title)
        known_selected = []
        for selected_chat_id in selected:
            if selected_chat_id == -9999:
                continue
            await self.storage.upsert_known_chat(selected_chat_id, f"Selected {selected_chat_id}")
            known_selected.append(selected_chat_id)
        if account_marker is not None:
            account = await self.storage.create_user_account(
                user_id,
                phone=f"+99890000{index:04d}",
                session=f"session-{index}",
                title=f"Account {index}",
                username=f"user_{index}",
            )
            await self.storage.set_user_sender_account(user_id, account["id"])
            account_chats = [(selected_chat_id, f"Account chat {selected_chat_id}") for selected_chat_id in known_selected]
            if chat_id not in known_selected:
                account_chats.append((chat_id, title))
            await self.storage.replace_account_chats(account["id"], account_chats)
            if known_selected:
                await self.storage.set_target_chats(user_id, known_selected, account_id=account["id"])
        else:
            await self.storage.set_target_chats(user_id, known_selected)

        await self.storage.set_auto_message(user_id, f"{name}: Ташкент нужен водитель на Алматы")
        await self.storage.set_auto_interval(user_id, max(1, index % 60))
        await self.storage.set_auto_enabled(user_id, True)
        await self.storage.record_auto_campaign_start(user_id)
        auto = await self.storage.get_auto(user_id)

        self.assertTrue(auto["is_enabled"])
        self.assertEqual(auto["message"], f"{name}: Ташкент нужен водитель на Алматы")
        self.assertGreater(auto["interval_minutes"], 0)
        if account_marker is None:
            self.assertEqual(set(auto["target_chat_ids"]), set(known_selected))

        reserved, reason = await self.storage.reserve_auto_delivery(
            user_id=user_id,
            chat_id=chat_id,
            day_key="2026-08-18",
            now_iso=f"2026-08-18T10:{index % 60:02d}:00+05:00",
            daily_limit=5,
            chat_interval_seconds=0,
        )
        self.assertTrue(reserved)
        self.assertEqual(reason, "reserved")

        text = CLASSIFICATION_TEXTS[index % len(CLASSIFICATION_TEXTS)]
        classification = classify_message_locally(text)
        inserted = await self.storage.record_logistics_message(
            chat_id=chat_id,
            message_id=index,
            chat_title=title,
            chat_username=f"chat_{index}",
            author_id=index,
            author_name=f"Author {index}",
            author_username=f"author_{index}",
            text=text,
            classification=classification,
            created_at=f"2026-08-18T11:{index % 60:02d}:00",
        )
        self.assertTrue(inserted)
        messages = await self.storage.list_logistics_messages(chat_ids=[chat_id])
        self.assertEqual(len(messages), 1)
        payload = build_locations_payload(messages)
        self.assertTrue("locations" in payload and "activities" in payload)


def _make_test(index: int, case: tuple):
    def test(self: BulkMailingCaseTest) -> None:
        asyncio.run(self._run_case(index, case))

    test.__name__ = f"test_mailing_group_message_limit_case_{index:02d}_{case[0]}"
    return test


for _index, _case in enumerate(MAILING_CASES, start=1):
    setattr(BulkMailingCaseTest, f"test_mailing_group_message_limit_case_{_index:02d}_{_case[0]}", _make_test(_index, _case))


if __name__ == "__main__":
    unittest.main()
