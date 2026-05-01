# src/utils/custom_dl.py

import asyncio
from typing import Any, AsyncGenerator, Dict

from pyrogram import Client
from pyrogram.errors import FloodWait, AuthKeyUnregistered
from pyrogram.types import Message

from src.server.exceptions import FileNotFound
from src.utils.logger import logger
from src.vars import Var

class ByteStreamer:
    __slots__ = ('client', 'chat_id')

    def __init__(self, client: Client) -> None:
        self.client = client
        self.chat_id = int(Var.BIN_CHANNEL)

    async def get_message(self, message_id: int) -> Message:
        retries = 0
        while retries < 5:
            try:
                # Use a small internal timeout for the API call itself
                logger.debug(f"Fetching message for ID {message_id}...")
                message = await asyncio.wait_for(
                    self.client.get_messages(self.chat_id, message_id), timeout=30)
                break
            except asyncio.TimeoutError:
                logger.debug(f"Timeout fetching message {message_id}, retrying...")
                retries += 1
            except FloodWait as e:
                logger.debug(f"FloodWait: get_message, sleep {e.value}s")
                await asyncio.sleep(e.value)
            except Exception as e:
                logger.debug(f"Error fetching message {message_id}: {e}")
                retries += 1
                await asyncio.sleep(1)
        
        if retries >= 5:
            raise FileNotFound(f"Message {message_id} fetch failed after retries")
        
        if not message or not message.media:
            raise FileNotFound(f"Message {message_id} not found")
        return message

    async def stream_file(self, message_id: int, offset: int = 0, limit: int = 0, message: Message = None) -> AsyncGenerator[bytes, None]:
        if not message:
            message = await self.get_message(message_id)
        
        
        # Pyrogram uses 1MB chunks internally for stream_media
        PART_SIZE = 1024 * 1024
        chunk_offset = offset // PART_SIZE
        chunk_limit = (limit + PART_SIZE - 1) // PART_SIZE if limit > 0 else 0

        while True:
            try:
                logger.debug(f"Starting stream_media for message {message_id} at chunk {chunk_offset}")
                async for chunk in self.client.stream_media(message, offset=chunk_offset, limit=chunk_limit):
                    yield chunk
                logger.debug(f"Finished stream_media for message {message_id}")
                break
            except FloodWait as e:
                logger.debug(f"FloodWait: stream_file, sleep {e.value}s")
                await asyncio.sleep(e.value)
            except AuthKeyUnregistered:
                # Re-raise so stream_routes can mark the client as dead
                raise
            except Exception as e:
                logger.error(f"Error in stream_media for message {message_id}: {e}")
                break

    def get_file_info_sync(self, message: Message) -> Dict[str, Any]:
        media = message.document or message.video or message.audio or message.photo
        if not media:
            return {"message_id": message.id, "error": "No media"}
        return {
            "message_id": message.id,
            "file_size": getattr(media, 'file_size', 0) or 0,
            "file_name": getattr(media, 'file_name', None),
            "mime_type": getattr(media, 'mime_type', None),
            "unique_id": getattr(media, 'file_unique_id', None),
            "media_type": type(media).__name__.lower()
        }

    async def get_file_info(self, message_id: int) -> Dict[str, Any]:
        try:
            message = await self.get_message(message_id)
            return self.get_file_info_sync(message)
        except Exception as e:
            logger.debug(f"Error getting file info for {message_id}: {e}", exc_info=True)
            return {"message_id": message_id, "error": str(e)}
