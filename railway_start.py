import asyncio
import json
import logging
import os
import signal
from contextlib import suppress

from aiohttp import web

import bot as bot_module
from public_web import create_app


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def normalize_phone_digits(phone: str) -> str:
    return "".join(ch for ch in phone if ch.isdigit())


def user_id_from_otp_phone_map(phone: str) -> int | None:
    raw_map = os.getenv("OTP_PHONE_USER_MAP", "").strip()
    if not raw_map:
        return None
    try:
        mapping = json.loads(raw_map)
    except json.JSONDecodeError:
        logger.warning("OTP_PHONE_USER_MAP is not valid JSON.")
        return None
    target_digits = normalize_phone_digits(phone)
    for mapped_phone, mapped_user_id in dict(mapping).items():
        mapped_digits = normalize_phone_digits(str(mapped_phone))
        if mapped_digits and (mapped_digits == target_digits or mapped_digits.endswith(target_digits) or target_digits.endswith(mapped_digits)):
            try:
                return int(mapped_user_id)
            except (TypeError, ValueError):
                return None
    return None


async def send_otp_to_telegram(phone: str, otp: str, telegram_user_id: int | None = None) -> None:
    digits = normalize_phone_digits(phone)
    if not digits:
        raise ValueError("phone_required")
    user_id = user_id_from_otp_phone_map(phone)
    if user_id is None:
        user_id = await bot_module.storage.find_account_owner_by_phone(phone)
    if user_id is None and telegram_user_id is not None:
        user_id = telegram_user_id
    if user_id is None:
        raise LookupError("telegram_not_linked")
    await bot_module.bot.send_message(
        user_id,
        f"Ваш OTP для входа в Rasylon Logistics: {otp}\nКод действует 5 минут.",
    )


async def run_web(stop_event: asyncio.Event) -> web.AppRunner:
    app = create_app(
        storage=bot_module.storage,
        payment_created_callback=bot_module.notify_admins_about_payment,
        order_created_callback=bot_module.notify_admins_about_mini_order,
        otp_sender_callback=send_otp_to_telegram,
        mailing_start_callback=bot_module.start_mini_mailing,
    )
    runner = web.AppRunner(app)
    await runner.setup()
    host = os.getenv("APP_HOST", "0.0.0.0")
    port = int(os.getenv("PORT", os.getenv("APP_PORT", "8080")))
    site = web.TCPSite(runner, host=host, port=port)
    await site.start()
    logger.info("Web app listening on %s:%s", host, port)
    return runner


async def run_bot() -> None:
    await bot_module.on_startup(bot_module.dp)
    try:
        await bot_module.dp.start_polling()
    finally:
        await bot_module.on_shutdown(bot_module.dp)


async def main() -> None:
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop_event.set)

    runner = await run_web(stop_event)
    bot_task = asyncio.create_task(run_bot(), name="telegram-bot")
    stop_task = asyncio.create_task(stop_event.wait(), name="shutdown-signal")

    done, pending = await asyncio.wait(
        {bot_task, stop_task},
        return_when=asyncio.FIRST_COMPLETED,
    )
    if bot_task in done:
        bot_task.result()

    stop_event.set()
    for task in pending:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
    if not bot_task.done():
        bot_task.cancel()
        with suppress(asyncio.CancelledError):
            await bot_task
    await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
