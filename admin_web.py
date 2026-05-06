from public_web import create_app


if __name__ == "__main__":
    import os

    from aiohttp import web

    port = int(os.getenv("PORT", os.getenv("APP_PORT", "8080")))
    host = os.getenv("APP_HOST", "0.0.0.0")
    web.run_app(create_app(), host=host, port=port)
