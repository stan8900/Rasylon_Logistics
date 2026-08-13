import asyncio
import tempfile
from pathlib import Path

import pytest

from app.storage import Storage
from public_web import build_locations_payload, classify_message_locally


CLASSIFICATION_CASES = [
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


PAYLOAD_CASES = [
    ("Ташкент", "Алматы", "cargo_searching_driver", "тент", "сегодня"),
    ("Самарканд", "Ташкент", "cargo_searching_driver", "рефрижератор", "завтра"),
    ("Алматы", "Бишкек", "cargo_searching_driver", "фура", "утром"),
    ("Бишкек", "Ташкент", "cargo_searching_driver", "изотерм", "сейчас"),
    ("Москва", "Самарканд", "cargo_searching_driver", "газель", "вечером"),
    ("Ташкент", "Алматы", "driver_searching_cargo", "тент", "сегодня"),
    ("Самарканд", "Ташкент", "driver_searching_cargo", "рефрижератор", "завтра"),
    ("Алматы", "Ташкент", "driver_searching_cargo", "изотерм", "сейчас"),
    ("Бишкек", "Москва", "driver_searching_cargo", "фура", "утром"),
    ("Ташкент", "Алматы", "driver_searching_cargo", "тент", "сегодня"),
    ("Ташкент", "Бишкек", "cargo_searching_driver", "тент", "завтра"),
    ("Самарканд", "Алматы", "cargo_searching_driver", "рефрижератор", "утром"),
    ("Москва", "Бишкек", "cargo_searching_driver", "фура", "вечером"),
    ("Бишкек", "Алматы", "driver_searching_cargo", "изотерм", "сейчас"),
    ("Алматы", "Москва", "cargo_searching_driver", "газель", "сегодня"),
]


STORAGE_CASES = [
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


@pytest.mark.parametrize(
    "text,intent,location,destination,vehicle,availability,should_map",
    CLASSIFICATION_CASES,
)
def test_classification_unit_case(text, intent, location, destination, vehicle, availability, should_map):
    classification = classify_message_locally(text)

    assert classification["intent"] == intent
    assert classification["current_location"] == location
    assert classification["destination"] == destination
    assert classification["vehicle_type"] == vehicle
    assert classification["availability"] == availability
    assert classification["should_map"] == should_map
    assert "confidence" not in classification


@pytest.mark.parametrize(
    "location,destination,intent,vehicle,availability",
    PAYLOAD_CASES,
)
def test_payload_unit_case(location, destination, intent, vehicle, availability):
    payload = build_locations_payload(
        [
            {
                "id": 1,
                "chat_id": -100,
                "message_id": 1,
                "chat_title": "Loads",
                "chat_username": "loads_chat",
                "author_name": "Dispatcher",
                "author_username": "dispatcher",
                "text": f"{location} -> {destination}",
                "intent": intent,
                "current_location": location,
                "destination": destination,
                "vehicle_type": vehicle,
                "availability": availability,
                "created_at": "2026-08-13T10:00:00",
            }
        ]
    )

    assert payload["locations"][0]["name"] == location
    assert payload["locations"][0]["messages"] == 1
    assert payload["locations"][0]["drivers"] == (1 if intent == "driver_searching_cargo" else 0)
    assert payload["activities"][0]["intent"] == intent
    assert payload["activities"][0]["destination"] == destination
    assert "confidence" not in payload["activities"][0]


@pytest.mark.parametrize(
    "chat_id,message_id,intent,location,destination,vehicle",
    STORAGE_CASES,
)
def test_storage_unit_case(chat_id, message_id, intent, location, destination, vehicle):
    async def runner():
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "storage.db")
            inserted = await storage.record_logistics_message(
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
            messages = await storage.list_logistics_messages(chat_ids=[chat_id])
            other_messages = await storage.list_logistics_messages(chat_ids=[chat_id - 10_000])

            assert inserted is True
            assert len(messages) == 1
            assert messages[0]["message_id"] == message_id
            assert messages[0]["intent"] == intent
            assert messages[0]["classification"]["current_location"] == location
            assert other_messages == []

    asyncio.run(runner())
