# src/server/stream_routes.py

import re
import secrets
import time
import asyncio
from urllib.parse import quote, unquote

from aiohttp import web
from pyrogram.errors import AuthKeyUnregistered

from src import __version__, StartTime
from src.bot import StreamBot, multi_clients, work_loads
from src.server.exceptions import FileNotFound, InvalidHash
from src.utils.custom_dl import ByteStreamer
from src.utils.bot_utils import get_base_url
from src.utils.logger import logger
from src.vars import Var
from src.utils.render_template import render_page
from src.utils.time_format import get_readable_time

routes = web.RouteTableDef()

SECURE_HASH_LENGTH = 6
CHUNK_SIZE = 1024 * 1024
MAX_CONCURRENT_PER_CLIENT = 8
RANGE_REGEX = re.compile(r"bytes=(?P<start>\d*)-(?P<end>\d*)")
PATTERN_HASH_FIRST = re.compile(
    rf"^([a-zA-Z0-9_-]{{{SECURE_HASH_LENGTH}}})(\d+).*$")
PATTERN_ID_FIRST = re.compile(r"^(\d+).*$")
VALID_HASH_REGEX = re.compile(r'^[a-zA-Z0-9_-]+$')

streamers = {}
dead_clients = set()


def get_streamer(client_id: int) -> ByteStreamer:
    if client_id not in streamers:
        streamers[client_id] = ByteStreamer(multi_clients[client_id])
    return streamers[client_id]


def parse_media_request(path: str, query: dict) -> tuple[int, str]:
    clean_path = unquote(path).strip('/')

    match = PATTERN_HASH_FIRST.match(clean_path)
    if match:
        try:
            message_id = int(match.group(2))
            secure_hash = match.group(1)
            if (len(secure_hash) == SECURE_HASH_LENGTH and
                    VALID_HASH_REGEX.match(secure_hash)):
                return message_id, secure_hash
        except ValueError as e:
            raise InvalidHash(f"Invalid message ID format in path: {e}") from e

    match = PATTERN_ID_FIRST.match(clean_path)
    if match:
        try:
            message_id = int(match.group(1))
            secure_hash = query.get("hash", "").strip()
            if (len(secure_hash) == SECURE_HASH_LENGTH and
                    VALID_HASH_REGEX.match(secure_hash)):
                return message_id, secure_hash
            else:
                raise InvalidHash("Invalid or missing hash in query parameter")
        except ValueError as e:
            raise InvalidHash(f"Invalid message ID format in path: {e}") from e

    raise InvalidHash("Invalid URL format: Could not extract message ID or hash")


def select_optimal_client() -> tuple[int, ByteStreamer]:
    if not work_loads:
        return 0, get_streamer(0)

    # Filter out dead clients
    active_work_loads = {
        cid: load for cid, load in work_loads.items()
        if cid not in dead_clients
    }

    if not active_work_loads:
        # Fallback to primary if all else fails
        return 0, get_streamer(0)

    available_clients = [
        (cid, load) for cid, load in active_work_loads.items()
        if load < MAX_CONCURRENT_PER_CLIENT]

    if available_clients:
        client_id = min(available_clients, key=lambda x: x[1])[0]
    else:
        client_id = min(active_work_loads, key=active_work_loads.get)

    return client_id, get_streamer(client_id)


def parse_range_header(range_header: str, file_size: int) -> tuple[int, int]:
    if not range_header:
        return 0, file_size - 1

    match = RANGE_REGEX.match(range_header)
    if not match:
        raise web.HTTPBadRequest(text=f"Invalid range header: {range_header}")

    start_str = match.group("start")
    end_str = match.group("end")
    if start_str:
        start = int(start_str)
        end = int(end_str) if end_str else file_size - 1
    else:
        if not end_str:
            raise web.HTTPBadRequest(text=f"Invalid range header: {range_header}")
        suffix_len = int(end_str)
        if suffix_len <= 0:
            raise web.HTTPRequestRangeNotSatisfiable(headers={"Content-Range": f"bytes */{file_size}"})
        start = max(file_size - suffix_len, 0)
        end = file_size - 1

    if start < 0 or end >= file_size or start > end:
        raise web.HTTPRequestRangeNotSatisfiable(
            headers={"Content-Range": f"bytes */{file_size}"}
        )

    return start, end


@routes.get("/", allow_head=True)

async def root_redirect(request):
    raise web.HTTPFound("https://t.me/SyntaxRealm")


@routes.get("/status", allow_head=True)
async def status_endpoint(request):
    uptime = time.time() - StartTime
    total_load = sum(work_loads.values())

    workload_distribution = {str(k): v for k, v in sorted(work_loads.items())}

    return web.json_response({
        "server": {
            "status": "operational",
            "version": __version__,
            "uptime": get_readable_time(uptime)
        },
        "telegram_bot": {
            # "username": f"@{StreamBot.username}",
            "active_clients": len(multi_clients)
        },
        "resources": {
            "total_workload": total_load,
            "workload_distribution": workload_distribution

        }
    })


@routes.get(r"/watch/SyntaxRealm-{path:.+}", allow_head=True)
async def media_preview(request: web.Request):
    try:
        path = request.match_info["path"]
        message_id, secure_hash = parse_media_request(path, request.query)

        rendered_page = await render_page(
            message_id, secure_hash, request, requested_action='stream')
        return web.Response(text=rendered_page, content_type='text/html')

    except (InvalidHash, FileNotFound) as e:
        logger.debug(
            f"Client error in preview: {type(e).__name__} - {e}",
            exc_info=True)
        raise web.HTTPNotFound(text="Resource not found") from e
    except Exception as e:

        error_id = secrets.token_hex(6)
        logger.error(f"Preview error {error_id}: {e}", exc_info=True)
        raise web.HTTPInternalServerError(
            text=f"Server error occurred: {error_id}") from e


@routes.get(r"/SyntaxRealm-{path:.+}", allow_head=True)
async def media_delivery(request: web.Request):
    try:
        path = request.match_info["path"]
        message_id, secure_hash = parse_media_request(path, request.query)

        logger.info(f"Request started for path: {path}")
        client_id, streamer = select_optimal_client()
        primary_streamer = get_streamer(0)

        work_loads[client_id] += 1
        logger.debug(f"Selected client {client_id} for path {path}")

        try:
            # Use primary streamer for fast metadata with safety timeout
            # Also keep the message object to reuse it for streaming
            logger.debug(f"Fetching message for ID {message_id}...")
            message = await asyncio.wait_for(
                primary_streamer.get_message(message_id), timeout=30)
            file_info = primary_streamer.get_file_info_sync(message)
            logger.debug(f"Message fetched successfully for ID {message_id}")
            if not file_info.get('unique_id'):
                raise FileNotFound("File unique ID not found in info.")

            if (file_info['unique_id'][:SECURE_HASH_LENGTH] !=
                    secure_hash):
                raise InvalidHash(
                    "Provided hash does not match file's unique ID.")

            file_size = file_info.get('file_size', 0)
            if file_size == 0:
                raise FileNotFound(
                    "File size is reported as zero or unavailable.")

            range_header = request.headers.get("Range", "")
            start, end = parse_range_header(range_header, file_size)
            content_length = end - start + 1

            if start == 0 and end == file_size - 1:
                range_header = ""

            mime_type = (
                file_info.get('mime_type') or 'application/octet-stream')
            filename = (
                file_info.get('file_name') or f"file_{secrets.token_hex(4)}")

            is_download = request.query.get("download") == "1"
            disposition = "attachment" if is_download else "inline"

            headers = {
                "Content-Type": mime_type,
                "Content-Length": str(content_length),
                "Content-Disposition": (
                    f"{disposition}; filename*=UTF-8''{quote(filename)}"),
                "Accept-Ranges": "bytes",
                "Cache-Control": "public, max-age=31536000",
                "Connection": "keep-alive"
            }

            if range_header:
                headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"

            if request.method == 'HEAD':
                work_loads[client_id] -= 1
                logger.info(f"HEAD request completed for {path}")
                return web.Response(
                    status=206 if range_header else 200,
                    headers=headers
                )

            logger.info(f"Starting stream for {path} (Range: {range_header or 'None'})")

            resp = web.StreamResponse(
                status=206 if range_header else 200,
                headers=headers
            )
            await resp.prepare(request)

            try:
                bytes_sent = 0
                PART_SIZE = 512 * 1024
                bytes_to_skip = start % PART_SIZE
                async for chunk in streamer.stream_file(
                        message_id, offset=start, limit=content_length, message=message):
                    if bytes_to_skip > 0:
                        if len(chunk) <= bytes_to_skip:
                            bytes_to_skip -= len(chunk)
                            continue
                        chunk = chunk[bytes_to_skip:]
                        bytes_to_skip = 0

                    remaining = content_length - bytes_sent
                    if len(chunk) > remaining:
                        chunk = chunk[:remaining]
                    
                    if chunk:
                        await resp.write(chunk)
                        bytes_sent += len(chunk)
                    
                    if bytes_sent >= content_length:
                        break
                
                await resp.write_eof()
            except (ConnectionResetError, asyncio.CancelledError):
                logger.debug(f"Stream interrupted for {path}")
            except Exception as e:
                logger.error(f"Stream error during write for {path}: {e}")
            finally:
                work_loads[client_id] -= 1
            
            return resp

        except AuthKeyUnregistered:
            logger.error(f"Client ID {client_id} is unregistered. Removing from pool.")
            dead_clients.add(client_id)
            if client_id in work_loads:
                work_loads[client_id] -= 1
            # We don't retry here to avoid complex state, 
            # but next request will use a different client.
            raise web.HTTPServiceUnavailable(text="Stream client authentication failed. Please try again.")

        except (FileNotFound, InvalidHash):
            work_loads[client_id] -= 1
            raise
        except Exception as e:
            work_loads[client_id] -= 1
            error_id = secrets.token_hex(6)
            logger.error(
                f"Stream error {error_id}: {e}",
                exc_info=True)
            raise web.HTTPInternalServerError(
                text=f"Server error during streaming: {error_id}") from e

    except (InvalidHash, FileNotFound) as e:
        logger.debug(f"Client error: {type(e).__name__} - {e}", exc_info=True)
        raise web.HTTPNotFound(text="Resource not found") from e
    except Exception as e:
        error_id = secrets.token_hex(6)
        logger.error(f"Server error {error_id}: {e}", exc_info=True)
        raise web.HTTPInternalServerError(
            text=f"An unexpected server error occurred: {error_id}") from e
