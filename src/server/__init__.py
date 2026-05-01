# src/server/__init__.py

from aiohttp import web
from .stream_routes import routes
from src.utils.bot_utils import get_base_url

@web.middleware
async def url_discovery_middleware(request, handler):
    get_base_url(request)
    return await handler(request)

async def web_server():
    web_app = web.Application(
        client_max_size=30000000,
        middlewares=[url_discovery_middleware]
    )
    web_app.add_routes(routes)
    return web_app
