import asyncio
import time
import unittest

from app.user_sender import UserSender


class ConcurrentClient:
    def __init__(self) -> None:
        self.active_sends = 0
        self.max_active_sends = 0
        self.sent = []
        self.connected = True

    def is_connected(self) -> bool:
        return self.connected

    async def connect(self) -> None:
        self.connected = True

    async def is_user_authorized(self) -> bool:
        return True

    async def send_message(self, chat_id: int, message: str) -> None:
        self.active_sends += 1
        self.max_active_sends = max(self.max_active_sends, self.active_sends)
        await asyncio.sleep(0.01)
        self.sent.append((chat_id, message, time.monotonic()))
        self.active_sends -= 1

    async def disconnect(self) -> None:
        self.connected = False


class UserSenderSafeguardsTest(unittest.IsolatedAsyncioTestCase):
    async def test_parallel_send_calls_are_serialized_for_one_session(self) -> None:
        sender = UserSender(12345, "hash", "", min_send_interval_seconds=0)
        client = ConcurrentClient()
        sender._client = client

        await asyncio.gather(*(sender.send_message(-100 - index, f"msg {index}") for index in range(20)))

        self.assertEqual(len(client.sent), 20)
        self.assertEqual(client.max_active_sends, 1)

    async def test_min_send_interval_is_applied_inside_session_queue(self) -> None:
        sender = UserSender(12345, "hash", "", min_send_interval_seconds=0.02)
        client = ConcurrentClient()
        sender._client = client

        await asyncio.gather(sender.send_message(-100, "first"), sender.send_message(-101, "second"))

        self.assertEqual(len(client.sent), 2)
        self.assertGreaterEqual(client.sent[1][2] - client.sent[0][2], 0.018)
