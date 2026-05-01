# src/utils/render_template.py

import html as html_module
import urllib.parse
import re
import os

from jinja2 import Environment, FileSystemLoader

from src.bot import StreamBot
from src.server.exceptions import InvalidHash
from src.utils.bot_utils import get_base_url
from src.utils.file_properties import get_fname, get_uniqid
from src.utils.handler import handle_flood_wait
from src.utils.human_readable import humanbytes
from src.utils.logger import logger
from src.vars import Var

template_env = Environment(
    loader=FileSystemLoader('src/template'),
    enable_async=True,
    cache_size=200,
    auto_reload=False,
    optimized=True
)

def get_file_tags(file_name: str, media) -> list[str]:
    tags = ["Stream Ready"]
    
    # Extract extension
    _, ext = os.path.splitext(file_name)
    if ext:
        tags.append(ext[1:].upper())
    
    # Extract resolution from filename
    res_match = re.search(r'(\d{3,4}p)', file_name, re.IGNORECASE)
    if res_match:
        tags.append(res_match.group(1).upper())
    elif hasattr(media, 'height') and media.height:
        h = media.height
        if h >= 2160: tags.append("4K 2160P")
        elif h >= 1440: tags.append("2K 1440P")
        elif h >= 1080: tags.append("HD 1080P")
        elif h >= 720: tags.append("HD 720P")
        elif h >= 480: tags.append("480P")
        elif h >= 360: tags.append("360P")
    
    return tags

async def render_page(id: int, secure_hash: str, request, requested_action: str | None = None) -> str:
    try:
        message = await handle_flood_wait(StreamBot.get_messages, chat_id=int(Var.BIN_CHANNEL), message_ids=id)
        if not message:
            raise InvalidHash("Message not found")
        
        file_unique_id = get_uniqid(message)
        file_name = get_fname(message)
        
        if not file_unique_id or file_unique_id[:6] != secure_hash:
            raise InvalidHash("File unique ID or secure hash mismatch during rendering.")
        
        quoted_filename = urllib.parse.quote(file_name.replace('/', '_'))
        base_url = get_base_url(request)
        src = urllib.parse.urljoin(base_url + "/", f'SyntaxRealm-{secure_hash}{id}/{quoted_filename}')
        
        media = message.document or message.video or message.audio
        mime_type = getattr(media, 'mime_type', 'video/mp4') if media else 'video/mp4'
        
        safe_filename = html_module.escape(file_name)
        if requested_action == 'stream':
            template = template_env.get_template('req.html')
            context = {
                'heading': f"Streaming: {safe_filename}",
                'file_name': safe_filename,
                'src': src,
                'mime_type': mime_type,
                'file_size': humanbytes(getattr(media, 'file_size', 0)),
                'file_tags': get_file_tags(file_name, media),
                'support_link': Var.SUPPORT_LINK
            }
        else:
            template = template_env.get_template('dl.html')
            context = {
                'file_name': safe_filename,
                'src': src,
                'support_link': Var.SUPPORT_LINK
            }
        return await template.render_async(**context)
    except Exception as e:
        logger.error(f"Error in render_page for ID {id} and hash {secure_hash}: {e}", exc_info=True)
        raise
