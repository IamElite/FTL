# src/utils/custom_dl.py

import asyncio
from typing import Any, AsyncGenerator, Dict, Optional, List, Tuple

from pyrogram import Client
from pyrogram.errors import FloodWait, AuthKeyUnregistered
from pyrogram.types import Message

from src.server.exceptions import FileNotFound
from src.utils.database import db
from src.utils.logger import logger
from src.vars import Var

class ByteStreamer:
    __slots__ = ('client', 'chat_id')

    def __init__(self, client: Client) -> None:
        self.client = client
        self.chat_id = int(Var.BIN_CHANNEL)

    async def get_message(self, message_id: int, chat_id: int = None) -> Message:
        target_chat = chat_id if chat_id else self.chat_id
        retries = 0
        while retries < 5:
            try:
                logger.debug(f"Fetching message {message_id} from chat {target_chat}...")
                message = await asyncio.wait_for(
                    self.client.get_messages(target_chat, message_id), timeout=30)
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

    async def check_multi_part_file(self, message: Message) -> Optional[List[Message]]:
        media = message.document
        if not media or not media.file_name:
            return None
        
        file_name = media.file_name
        import re
        match = re.match(r'^(.+?)\.part(\d+)(?:\.mp4)?$', file_name, re.IGNORECASE)
        if not match:
            match = re.match(r'^(.+?)\.(\d+)$', file_name, re.IGNORECASE)
        
        if not match:
            return None
        
        base_name = match.group(1)
        chat_id = message.chat.id
        
        try:
            parts = []
            async for msg in self.client.get_chat_history(chat_id, limit=100):
                if msg.document and msg.document.file_name:
                    fname = msg.document.file_name
                    if (fname.startswith(base_name + '.part') or fname.startswith(base_name + '.')) and 'part' not in fname.lower():
                        if msg.id != message.id:
                            parts.append(msg)
            parts.append(message)
            parts.sort(key=lambda x: x.id)
            logger.info(f"Found {len(parts)} parts for multi-part file")
            return parts if len(parts) > 1 else None
        except Exception as e:
            logger.warning(f"Error checking multi-part: {e}")
            return None

    async def get_origin_info(self, message_id: int) -> Optional[Dict[str, Any]]:
        try:
            origin = await db.get_file_origin(message_id)
            if origin:
                return {
                    "chat_id": origin.get("origin_chat_id"),
                    "message_id": origin.get("origin_message_id")
                }
        except Exception as e:
            logger.debug(f"Error getting origin info: {e}")
        return None

    async def get_message_with_fallback(self, message_id: int) -> Tuple[Message, bool]:
        origin_info = await self.get_origin_info(message_id)
        if origin_info:
            try:
                message = await self.get_message(
                    origin_info["message_id"],
                    origin_info["chat_id"]
                )
                logger.info(f"Using origin message for {message_id}")
                return message, True
            except Exception as e:
                logger.warning(f"Could not get origin message: {e}")
        
        try:
            message = await self.get_message(message_id)
            return message, False
        except Exception as e:
            raise FileNotFound(f"Message {message_id} not found: {e}")
    
    async def stream_file(self, message_id: int, offset: int = 0, limit: int = 0, message: Message = None, use_origin: bool = True) -> AsyncGenerator[bytes, None]:
        if not message:
            if use_origin:
                message, origin_used = await self.get_message_with_fallback(message_id)
                logger.debug(f"Streaming from {'origin' if origin_used else 'bin'} channel")
            else:
                message = await self.get_message(message_id)
        
        
        PART_SIZE = 1024 * 1024
        chunk_offset = offset // PART_SIZE
        chunk_limit = (limit + PART_SIZE - 1) // PART_SIZE if limit > 0 else 0
        retries = 0
        max_retries = 5

        while retries <= max_retries:
            try:
                async for chunk in self.client.stream_media(message, offset=chunk_offset, limit=chunk_limit):
                    yield chunk
                break
            except FloodWait as e:
                if use_origin and retries == 0:
                    logger.warning(f"FloodWait from bin channel, trying origin...")
                    try:
                        origin_info = await self.get_origin_info(message_id)
                        if origin_info:
                            message = await self.get_message(
                                origin_info["message_id"],
                                origin_info["chat_id"]
                            )
                            logger.info(f"Retrying stream from origin for message {message_id}")
                            retries += 1
                            continue
                    except Exception as origin_err:
                        logger.warning(f"Could not switch to origin: {origin_err}")
                
                wait_time = min(e.value, 30)
                logger.warning(f"FloodWait: sleeping {wait_time}s, attempt {retries + 1}/{max_retries + 1}")
                await asyncio.sleep(wait_time)
                retries += 1
            except AuthKeyUnregistered:
                logger.error(f"AuthKeyUnregistered for message {message_id}")
                raise
            except Exception as e:
                logger.warning(f"Stream error for {message_id}: {e}, attempt {retries + 1}/{max_retries + 1}")
                retries += 1
                if retries <= max_retries:
                    await asyncio.sleep(min(retries * 2, 10))
                else:
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
            message, origin_used = await self.get_message_with_fallback(message_id)
            return self.get_file_info_sync(message)
        except Exception as e:
            logger.debug(f"Error getting file info for {message_id}: {e}", exc_info=True)
            return {"message_id": message_id, "error": str(e)}
