import unittest

from public_web import build_locations_payload, classify_message_locally, public_locations_payload


class PublicWebPayloadTest(unittest.TestCase):
    def test_public_locations_payload_contains_no_demo_data(self) -> None:
        payload = public_locations_payload()

        self.assertEqual(payload["locations"], [])
        self.assertEqual(payload["activities"], [])

    def test_classification_hides_confidence_and_detects_cargo_requests(self) -> None:
        classification = classify_message_locally("Ташкент, нужен водитель на Алматы, тент сегодня")

        self.assertEqual(classification["intent"], "cargo_searching_driver")
        self.assertEqual(classification["current_location"], "Ташкент")
        self.assertEqual(classification["destination"], "Алматы")
        self.assertNotIn("confidence", classification)

    def test_build_locations_payload_uses_real_stored_messages(self) -> None:
        payload = build_locations_payload(
            [
                {
                    "id": 1,
                    "chat_id": -100,
                    "message_id": 10,
                    "chat_title": "Loads",
                    "chat_username": "loads_chat",
                    "author_name": "Dispatcher",
                    "author_username": "dispatcher",
                    "text": "Ташкент, нужен водитель на Алматы, тент сегодня",
                    "intent": "cargo_searching_driver",
                    "current_location": "Ташкент",
                    "destination": "Алматы",
                    "vehicle_type": "тент",
                    "availability": "сегодня",
                    "created_at": "2026-08-13T10:00:00",
                }
            ]
        )

        self.assertEqual(payload["locations"][0]["name"], "Ташкент")
        self.assertEqual(payload["locations"][0]["messages"], 1)
        self.assertEqual(payload["activities"][0]["intent"], "cargo_searching_driver")
        self.assertEqual(payload["activities"][0]["source"], "loads_chat")
