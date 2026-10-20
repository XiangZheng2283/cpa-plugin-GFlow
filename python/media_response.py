import base64
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
# Must match CutFlow MAX_INLINE_MEDIA_BYTES. The client separately budgets
# base64 expansion and SSE/JSON overhead instead of limiting the wire to 32 MiB.
MAX_INLINE_BYTES = 128 * 1024 * 1024


def inline_cached_images(value, cache_dir):
    root = Path(cache_dir).resolve()
    memo = {}

    def replace(match):
        filename = match.group(1)
        if filename not in memo:
            path = root / filename
            if path.is_symlink() or path.resolve().parent != root or not path.is_file():
                raise ValueError("Generated media is missing from the GFlow cache; generation was not retried")
            error = "Generated media exceeds the 128 MiB inline limit; the existing cached file is retained and generation was not retried"
            if path.stat().st_size > MAX_INLINE_BYTES:
                raise ValueError(error)
            # Bound the read as well: a cache file may still grow after stat().
            with path.open("rb") as media:
                content = media.read(MAX_INLINE_BYTES + 1)
            if len(content) > MAX_INLINE_BYTES:
                raise ValueError(error)
            memo[filename] = "data:" + MEDIA_TYPES[path.suffix] + ";base64," + base64.b64encode(content).decode("ascii")
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
