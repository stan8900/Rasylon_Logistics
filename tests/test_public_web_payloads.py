import unittest
from types import SimpleNamespace
from unittest.mock import patch

from public_web import (
    AUTH_COOKIE_NAME,
    AUTH_SESSION_STORE,
    auth_user_from_token,
    build_locations_payload,
    classify_message_locally,
    cors_middleware,
    create_auth_session,
    public_locations_payload,
    resolve_authenticated_user,
)


class PublicWebPayloadTest(unittest.TestCase):
    def test_public_locations_payload_contains_no_demo_data(self) -> None:
        payload = public_locations_payload()

        self.assertEqual(payload["locations"], [])
        self.assertEqual(payload["activities"], [])

    def test_signed_auth_token_survives_memory_store_reset(self) -> None:
        user = {"id": 268248500, "username": "rasylon"}
        with patch.dict("os.environ", {"AUTH_SESSION_SECRET": "unit-test-secret"}, clear=False):
            token = create_auth_session(user)
            AUTH_SESSION_STORE.clear()

            self.assertEqual(auth_user_from_token(token), user)

    def test_signed_auth_token_rejects_tampering(self) -> None:
        user = {"id": 268248500, "username": "rasylon"}
        with patch.dict("os.environ", {"AUTH_SESSION_SECRET": "unit-test-secret"}, clear=False):
            token = create_auth_session(user)
            tampered = token[:-1] + ("a" if token[-1] != "a" else "b")

            self.assertIsNone(auth_user_from_token(tampered))

    def test_resolve_authenticated_user_reads_cookie_token(self) -> None:
        user = {"id": 268248500, "username": "rasylon"}
        with patch.dict("os.environ", {"AUTH_SESSION_SECRET": "unit-test-secret"}, clear=False):
            token = create_auth_session(user)
            AUTH_SESSION_STORE.clear()
            request = SimpleNamespace(cookies={AUTH_COOKIE_NAME: token})

            self.assertEqual(resolve_authenticated_user({}, request), user)

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

    def test_classification_handles_15_logistics_message_cases(self) -> None:
        cases = [
            ("Ташкент, нужен водитель на Алматы, тент сегодня", "cargo_searching_driver", "Ташкент", "Алматы", "тент", "сегодня", True),
            ("Самарканд ищем машину до Ташкент реф завтра", "cargo_searching_driver", "Самарканд", "Ташкент", "рефрижератор", "завтра", True),
            ("Алматы нужна машина в Бишкек фура утром", "cargo_searching_driver", "Алматы", "Бишкек", "фура", "утром", True),
            ("Бишкек нужен транспорт на Ташкент изотерм сейчас", "cargo_searching_driver", "Бишкек", "Ташкент", "изотерм", "сейчас", True),
            ("Москва ищу перевозчика до Самарканд газель вечером", "cargo_searching_driver", "Москва", "Самарканд", "газель", "вечером", True),
            ("Стою Ташкент, фура тент, ищу груз на Алматы сегодня", "driver_searching_cargo", "Ташкент", "Алматы", "тент", "сегодня", True),
            ("Самарканд свободен, реф, ищу загрузку до Ташкент завтра", "driver_searching_cargo", "Самарканд", "Ташкент", "рефрижератор", "завтра", True),
            ("Алматы стою, изотерм, направление в Ташкент сейчас", "driver_searching_cargo", "Алматы", "Ташкент", "изотерм", "сейчас", True),
            ("Бишкек свободная фура ищу груз на Москва утром", "driver_searching_cargo", "Бишкек", "Москва", "фура", "утром", True),
            ("Tashkent тент ищу груз на Almaty", "driver_searching_cargo", "Ташкент", "Алматы", "тент", None, True),
            ("toshkent нужна машина на bishkek тент", "cargo_searching_driver", "Ташкент", "Бишкек", "тент", None, True),
            ("samarqand ищем машину до tashkent реф", "cargo_searching_driver", "Самарканд", "Ташкент", "рефрижератор", None, True),
            ("Привет, как дела", "unknown", None, None, None, None, False),
            ("Нужен водитель, но город не указан", "cargo_searching_driver", None, None, None, None, False),
            ("Ташкент просто обсуждение без заявки", "unknown", "Ташкент", None, None, None, False),
        ]

        self.assertEqual(len(cases), 15)
        for text, intent, location, destination, vehicle, availability, should_map in cases:
            with self.subTest(text=text):
                classification = classify_message_locally(text)
                self.assertEqual(classification["intent"], intent)
                self.assertEqual(classification["current_location"], location)
                self.assertEqual(classification["destination"], destination)
                self.assertEqual(classification["vehicle_type"], vehicle)
                self.assertEqual(classification["availability"], availability)
                self.assertEqual(classification["should_map"], should_map)
                self.assertNotIn("confidence", classification)

    def test_build_locations_payload_handles_15_real_message_cases(self) -> None:
        messages = [
            {
                "id": index,
                "chat_id": -100 - index,
                "message_id": index,
                "chat_title": f"Chat {index}",
                "chat_username": f"chat_{index}",
                "author_name": f"Author {index}",
                "author_username": f"author_{index}",
                "text": text,
                "intent": intent,
                "current_location": location,
                "destination": destination,
                "vehicle_type": vehicle,
                "availability": availability,
                "created_at": f"2026-08-13T10:{index:02d}:00",
            }
            for index, (text, intent, location, destination, vehicle, availability) in enumerate(
                [
                    ("Ташкент, нужен водитель на Алматы, тент сегодня", "cargo_searching_driver", "Ташкент", "Алматы", "тент", "сегодня"),
                    ("Самарканд ищем машину до Ташкент реф завтра", "cargo_searching_driver", "Самарканд", "Ташкент", "рефрижератор", "завтра"),
                    ("Алматы нужна машина в Бишкек фура утром", "cargo_searching_driver", "Алматы", "Бишкек", "фура", "утром"),
                    ("Бишкек нужен транспорт на Ташкент изотерм сейчас", "cargo_searching_driver", "Бишкек", "Ташкент", "изотерм", "сейчас"),
                    ("Москва ищу перевозчика до Самарканд газель вечером", "cargo_searching_driver", "Москва", "Самарканд", "газель", "вечером"),
                    ("Стою Ташкент ищу груз на Алматы", "driver_searching_cargo", "Ташкент", "Алматы", "фура", "сегодня"),
                    ("Самарканд свободен ищу загрузку до Ташкент", "driver_searching_cargo", "Самарканд", "Ташкент", "рефрижератор", "завтра"),
                    ("Алматы стою направление в Ташкент", "driver_searching_cargo", "Алматы", "Ташкент", "изотерм", "сейчас"),
                    ("Бишкек свободная фура ищу груз на Москва", "driver_searching_cargo", "Бишкек", "Москва", "фура", "утром"),
                    ("Tashkent ищу груз на Almaty", "driver_searching_cargo", "Ташкент", "Алматы", "тент", "сегодня"),
                    ("toshkent нужна машина на bishkek", "cargo_searching_driver", "Ташкент", "Бишкек", "тент", "завтра"),
                    ("samarqand ищем машину до tashkent", "cargo_searching_driver", "Самарканд", "Ташкент", "рефрижератор", "утром"),
                    ("Москва нужен водитель на Бишкек", "cargo_searching_driver", "Москва", "Бишкек", "фура", "вечером"),
                    ("Бишкек стою ищу груз на Алматы", "driver_searching_cargo", "Бишкек", "Алматы", "изотерм", "сейчас"),
                    ("Алматы нужен транспорт на Москва", "cargo_searching_driver", "Алматы", "Москва", "газель", "сегодня"),
                ],
                start=1,
            )
        ]

        self.assertEqual(len(messages), 15)
        payload = build_locations_payload(messages)

        self.assertEqual(len(payload["activities"]), 15)
        self.assertEqual(sum(location["messages"] for location in payload["locations"]), 15)
        self.assertEqual(sum(location["drivers"] for location in payload["locations"]), 6)
        self.assertTrue(all("confidence" not in activity for activity in payload["activities"]))
        self.assertEqual(payload["locations"][0]["messages"], 4)


class CorsMiddlewareTest(unittest.IsolatedAsyncioTestCase):
    async def test_cors_allows_custom_domain_from_comma_separated_origins(self) -> None:
        async def handler(_request: SimpleNamespace) -> SimpleNamespace:
            return SimpleNamespace(headers={})

        request = SimpleNamespace(method="POST", headers={"Origin": "https://www.rasylon.uz"})
        with patch.dict(
            "os.environ",
            {
                "CORS_ALLOW_ORIGIN": (
                    "https://rasylonlogisticsfrontend-production.up.railway.app,"
                    "https://rasylon.uz,"
                    "https://www.rasylon.uz"
                )
            },
            clear=False,
        ):
            response = await cors_middleware(request, handler)

        self.assertEqual(response.headers["Access-Control-Allow-Origin"], "https://www.rasylon.uz")
        self.assertEqual(response.headers["Access-Control-Allow-Credentials"], "true")
