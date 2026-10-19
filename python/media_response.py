import base64
import hashlib
import json
from pathlib import Path
import re


CACHE_MEDIA = re.compile(
    r"(?<![A-Za-z0-9/.:])/v0/(?:resource|management)/plugins/gflow(?:/api)?/tmp/"
    r"([a-f0-9]{32}(?:_(?:2K|4K))?\.(?:png|jpg|jpeg|webp|mp4|webm|mov))(?=$|[\s\"'<>\)\\])"
)
MEDIA_TYPES = {
    ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
    ".webp": "image/webp", ".mp4": "video/mp4", ".webm": "video/webm",
    ".mov": "video/quicktime",
}
MAX_INLINE_BYTES = 128 * 1024 * 1024
STREAM_INLINE_BYTES = 256 * 1024
STREAM_CHUNK_BYTES = 24 * 1024


def _cached_file(root, filename):
    path = root / filename
    if path.is_symlink() or path.resolve().parent != root or not path.is_file():
        raise ValueError("Generated media is missing from the GFlow cache; generation was not retried")
    return path


def _media_event(filename, mime, chunk="", seq=0, total=0, size=0, digest="", done=False):
    media = {
        "id": filename,
        "mime": mime,
        "encoding": "base64",
        "chunk": chunk,
        "seq": seq,
        "total": total,
        "size": size,
        "sha256": digest,
        "done": done,
    }
    return "data: " + json.dumps({
        "gflow_media": media,
        "choices": [{"index": 0, "delta": {"content": "", "gflow_media": media}, "finish_reason": None}],
    }, ensure_ascii=False) + "\n\n"


def inline_cached_images(value, cache_dir):
    root = Path(cache_dir).resolve()
    memo = {}

    def replace(match):
        filename = match.group(1)
        if filename not in memo:
            path = _cached_file(root, filename)
            if path.stat().st_size > MAX_INLINE_BYTES:
                raise ValueError("Generated media exceeds the 128 MiB inline limit; retrieve the cached file through the authenticated GFlow media management API")
            memo[filename] = "data:" + MEDIA_TYPES[path.suffix] + ";base64," + base64.b64encode(path.read_bytes()).decode("ascii")
        return memo[filename]

    def walk(item):
        if isinstance(item, str):
            return CACHE_MEDIA.sub(replace, item)
        if isinstance(item, dict):
            return {key: walk(child) for key, child in item.items()}
        if isinstance(item, list):
            return [walk(child) for child in item]
        return item

    return walk(value)


def stream_cached_media(value, cache_dir):
    """Keep large cached media out of a single native stream event.

    The final SSE event stays a normal OpenAI chunk while the image bytes travel
    as bounded extension events. The native client reassembles them locally.
    """
    root = Path(cache_dir).resolve()
    memo = {}
    large = {}

    def replace(match):
        filename = match.group(1)
        if filename in memo:
            return memo[filename][0]
        path = _cached_file(root, filename)
        size = path.stat().st_size
        if size > MAX_INLINE_BYTES:
            raise ValueError("Generated media exceeds the 128 MiB inline limit; retrieve the cached file through the authenticated GFlow media management API")
        if size <= STREAM_INLINE_BYTES or MEDIA_TYPES[path.suffix].startswith("video/"):
            data = "data:" + MEDIA_TYPES[path.suffix] + ";base64," + base64.b64encode(path.read_bytes()).decode("ascii")
            memo[filename] = (data, [])
            return data
        raw = path.read_bytes()
        encoded = base64.b64encode(raw).decode("ascii")
        step = STREAM_CHUNK_BYTES * 4 // 3
        chunks = [encoded[i:i + step] for i in range(0, len(encoded), step)]
        digest = hashlib.sha256(raw).hexdigest()
        events = [_media_event(filename, MEDIA_TYPES[path.suffix], chunk, index, len(chunks), len(raw), digest)
                  for index, chunk in enumerate(chunks)]
        events.append(_media_event(filename, MEDIA_TYPES[path.suffix], seq=len(chunks), total=len(chunks), size=len(raw), digest=digest, done=True))
        marker = "gflow-media-ref:" + filename
        memo[filename] = (marker, events)
        large[filename] = events
        return marker

    def walk(item):
        if isinstance(item, str):
            return CACHE_MEDIA.sub(replace, item)
        if isinstance(item, dict):
            return {key: walk(child) for key, child in item.items()}
        if isinstance(item, list):
            return [walk(child) for child in item]
        return item

    rewritten = walk(value)
    events = []
    for filename, media_events in large.items():
        events.extend(media_events)
        rewritten = rewritten.replace("gflow-media-ref:" + filename, "")
    return events + [rewritten]
