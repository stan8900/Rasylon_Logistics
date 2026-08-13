import unittest

from public_web import classify_message_locally, public_locations_payload


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
