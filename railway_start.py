import asyncio
import logging
import os
import signal
from contextlib import suppress

from aiohttp import web

import bot as bot_module
from public_web import create_app


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def send_otp_to_telegram(phone: str, otp: str) -> None:
    digits = "".join(ch for ch in phone if ch.isdigit())
    if not digits:
        raise ValueError("phone_required")
    user_id = int(digits[-15:])
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
