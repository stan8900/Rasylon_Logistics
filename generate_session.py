import os
from getpass import getpass

from telethon.errors import (
    AuthRestartError,
    FloodWaitError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    PhoneNumberInvalidError,
    SessionPasswordNeededError,
)
from telethon.sessions import StringSession
from telethon.sync import TelegramClient

api_id = int(os.getenv("TG_USER_API_ID", "38716906"))
api_hash = os.getenv("TG_USER_API_HASH", "792fef6147d502f29e5c8996d1a68a42")


def main() -> None:
    print("Telegram usually sends the login code inside the Telegram app first.")
    print("If Telegram says the code is invalid, restart this script and request a new code.")
    phone = input("Phone number, with country code (example +998901234567): ").strip()

    client = TelegramClient(
        StringSession(),
        api_id,
        api_hash,
        connection_retries=5,
        request_retries=3,
        timeout=20,
    )
    try:
        client.connect()
        if not client.is_user_authorized():
            sent = client.send_code_request(phone)
            print("Code request sent. Check Telegram app notifications/messages first, then SMS.")

            for attempt in range(1, 4):
                code = input("Enter the new code you received: ").strip().replace(" ", "")
                try:
                    client.sign_in(phone=phone, code=code, phone_code_hash=sent.phone_code_hash)
                    break
                except PhoneCodeInvalidError:
                    if attempt == 3:
                        raise
                    print("Invalid code. Do not reuse old codes; wait for the newest Telegram code.")
                except PhoneCodeExpiredError:
                    print("This code expired. Restart the script to request a new code.")
                    return
                except SessionPasswordNeededError:
                    password = getpass("Two-step verification password: ")
                    client.sign_in(password=password)
                    break

        print("TG_USER_SESSION=", client.session.save(), sep="")
    except PhoneNumberInvalidError:
        print("Telegram rejected this phone number. Check the country code and account status.")
    except PhoneCodeInvalidError:
        print("Telegram rejected the code. Restart the script and use the newest code only once.")
    except AuthRestartError:
        print("Telegram asked to restart authorization. Run the script again and request a new code.")
    except FloodWaitError as exc:
        print(f"Telegram rate-limited this login. Wait {exc.seconds} seconds before trying again.")
    except ConnectionError as exc:
        print(f"Network connection failed: {exc}. Try again, or use a reliable VPN/proxy if Telegram is blocked.")
    finally:
        client.disconnect()


if __name__ == "__main__":
    main()
