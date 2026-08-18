import asyncio
import logging
import os
import time
from typing import Dict, List, Optional, Tuple, Union

import socks
from telethon import TelegramClient
from telethon.errors import RPCError
from telethon.errors.rpcerrorlist import AuthKeyDuplicatedError, AuthKeyUnregisteredError, SessionRevokedError
from telethon.sessions import StringSession
from telethon.tl.types import Channel, Chat


ChatId = Union[int, str]
ProxyTuple = Tuple[int, str, int, bool, Optional[str], Optional[str]]
AUTHORIZATION_ERRORS = (AuthKeyDuplicatedError, AuthKeyUnregisteredError, SessionRevokedError)


class InvalidUserSessionError(RuntimeError):
    """Raised when Telegram rejects the stored user authorization."""


def build_telethon_proxy(proxy: Optional[Dict[str, Union[str, int]]]) -> Optional[ProxyTuple]:
    if not proxy:
        return None
    host = str(proxy.get("host") or "").strip()
    port_raw = proxy.get("port")
    if not host:
        return None
    try:
        port = int(port_raw)
    except (TypeError, ValueError):
        return None
    proxy_type = str(proxy.get("type") or "socks5").lower()
    type_map = {
        "socks5": socks.SOCKS5,
        "socks4": socks.SOCKS4,
        "http": socks.HTTP,
    }
    resolved_type = type_map.get(proxy_type, socks.SOCKS5)
    username = proxy.get("username") if proxy.get("username") else None
    password = proxy.get("password") if proxy.get("password") else None
    return resolved_type, host, port, True, username, password


class UserSender:
    """Wrapper around a Telethon client that sends messages from a user account."""

    def __init__(
        self,
        api_id: int,
        api_hash: str,
        session_string: str,
        *,
        proxy: Optional[Dict[str, Union[str, int]]] = None,
        receive_updates: bool = False,
        min_send_interval_seconds: Optional[float] = None,
    ) -> None:
        self._proxy = build_telethon_proxy(proxy)
        self._client = TelegramClient(
            StringSession(session_string),
            api_id,
            api_hash,
            proxy=self._proxy,
            connection_retries=2,
            request_retries=2,
            timeout=10,
            receive_updates=receive_updates,
        )
        self._start_lock = asyncio.Lock()
        self._send_lock = asyncio.Lock()
        self._started = False
        self._invalid = False
        if min_send_interval_seconds is None:
            try:
                min_send_interval_seconds = float(os.getenv("TG_USER_SEND_MIN_INTERVAL_SECONDS", "60"))
            except ValueError:
                min_send_interval_seconds = 60.0
        self._min_send_interval_seconds = max(0.0, min_send_interval_seconds)
        self._last_send_monotonic = 0.0
        self._logger = logging.getLogger(__name__)

    async def start(self) -> None:
        async with self._start_lock:
            if self._invalid:
                raise InvalidUserSessionError(
                    "Пользовательская сессия Telegram отозвана. Заново сгенерируйте TG_USER_SESSION."
                )
            if self._started:
                if not self._client.is_connected():
                    try:
                        await self._client.connect()
                    except AUTHORIZATION_ERRORS as exc:
                        await self._mark_invalid()
                        raise InvalidUserSessionError(
                            "Пользовательская сессия Telegram отозвана. Заново сгенерируйте TG_USER_SESSION."
                        ) from exc
                return
            try:
                await self._client.connect()
                if not await self._client.is_user_authorized():
                    await self._mark_invalid()
                    raise InvalidUserSessionError(
                        "Пользовательская сессия Telegram не авторизована. Заново сгенерируйте TG_USER_SESSION."
                    )
            except AUTHORIZATION_ERRORS as exc:
                await self._mark_invalid()
                raise InvalidUserSessionError(
                    "Пользовательская сессия Telegram отозвана. Заново сгенерируйте TG_USER_SESSION."
                ) from exc
            self._started = True

    async def send_message(self, chat_id: ChatId, message: str) -> None:
        async with self._send_lock:
            await self.start()
            await self._wait_for_send_slot()
            try:
                await self._client.send_message(chat_id, message)
                self._last_send_monotonic = time.monotonic()
            except AUTHORIZATION_ERRORS as exc:
                await self._mark_invalid()
                raise InvalidUserSessionError(
                    "Пользовательская сессия Telegram отозвана. Заново авторизуйте аккаунт."
                ) from exc
            except RPCError as exc:
                raise RuntimeError(f"Не удалось отправить сообщение через пользовательский аккаунт: {exc}") from exc

    async def describe_self(self) -> str:
        await self.start()
        try:
            me = await self._client.get_me()
        except AUTHORIZATION_ERRORS as exc:
            await self._mark_invalid()
            raise InvalidUserSessionError(
                "Пользовательская сессия Telegram отозвана. Заново авторизуйте аккаунт."
            ) from exc
        if not me:
            return "неизвестный пользователь"
        username = f"@{me.username}" if getattr(me, "username", None) else None
        full_name = " ".join(filter(None, [me.first_name, me.last_name])) or str(me.id)
        return f"{full_name} {username}" if username else full_name

    async def list_accessible_chats(self) -> List[Tuple[int, str]]:
        await self.start()
        chats: List[Tuple[int, str]] = []
        try:
            async for dialog in self._client.iter_dialogs():
                entity = dialog.entity
                title = dialog.name or getattr(entity, "title", None) or getattr(entity, "username", None)
                if isinstance(entity, Chat):
                    chats.append((entity.id, title or f"Чат {entity.id}"))
                elif isinstance(entity, Channel) and not getattr(entity, "broadcast", False):
                    chats.append((entity.id, title or f"Чат {entity.id}"))
        except AUTHORIZATION_ERRORS as exc:
            await self._mark_invalid()
            raise InvalidUserSessionError(
                "Пользовательская сессия Telegram отозвана. Заново авторизуйте аккаунт."
            ) from exc
        return chats

    async def stop(self) -> None:
        async with self._start_lock:
            if not self._started and not self._client.is_connected():
                return
            await self._client.disconnect()
            self._started = False

    async def _mark_invalid(self) -> None:
        self._invalid = True
        self._started = False
        if self._client.is_connected():
            await self._client.disconnect()

    async def _wait_for_send_slot(self) -> None:
        if self._min_send_interval_seconds <= 0 or self._last_send_monotonic <= 0:
            return
        elapsed = time.monotonic() - self._last_send_monotonic
        remaining = self._min_send_interval_seconds - elapsed
        if remaining > 0:
            await asyncio.sleep(remaining)

    @property
    def client(self) -> TelegramClient:
        return self._client
