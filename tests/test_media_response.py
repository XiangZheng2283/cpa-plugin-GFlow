import base64
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("media_response_under_test", ROOT / "python/media_response.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class MediaResponseRegression(unittest.TestCase):
    def setUp(self):
        staging = ROOT / ".build/media-tests"
        staging.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=staging)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def test_cached_images_and_video_are_delivered_without_management_credentials(self):
        for extension, suffix, mime in [
            ("jpg", "", "image/jpeg"), ("jpg", "_2K", "image/jpeg"),
            ("png", "_4K", "image/png"), ("mp4", "", "video/mp4"),
            ("mp4", "_4K", "video/mp4"),
        ]:
            filename = "a" * 32 + suffix + "." + extension
            body = b"synthetic-media"
            (self.root / filename).write_bytes(body)
            expected = "data:" + mime + ";base64," + base64.b64encode(body).decode()
            for prefix in ["/v0/resource/plugins/gflow", "/v0/management/plugins/gflow/api"]:
                url = prefix + "/tmp/" + filename
                result = module.inline_cached_images({"url": url, "choices": [{"message": {"content": "![media](" + url + ")"}}]}, self.root)
                self.assertEqual(result["url"], expected)
                self.assertIn(expected, result["choices"][0]["message"]["content"])
                self.assertIn(expected, module.inline_cached_images('data: {"url":"' + url + '"}', self.root))

    def test_control_files_traversal_and_external_urls_are_not_read(self):
        for url in [
            "/v0/resource/plugins/gflow/tmp/control-fake.json",
            "/v0/resource/plugins/gflow/tmp/../../secret.jpg",
            "/v0/resource/plugins/gflow/tmp/" + "a" * 32 + "_1K.jpg",
            "/v0/resource/plugins/gflow/tmp/" + "a" * 32 + "_2K_extra.jpg",
            "https://other.example/v0/management/plugins/gflow/api/tmp/" + "a" * 32 + ".mp4",
        ]:
            self.assertEqual(module.inline_cached_images(url, self.root), url)

    def test_missing_cached_media_is_not_reported_as_a_usable_url(self):
        with self.assertRaisesRegex(ValueError, "missing"):
            module.inline_cached_images("/v0/management/plugins/gflow/api/tmp/" + "a" * 32 + ".mp4", self.root)

    def test_oversize_media_preserves_cache_without_resubmission(self):
        filename = "a" * 32 + ".mp4"
        (self.root / filename).write_bytes(b"large")
        with patch.object(module, "MAX_INLINE_BYTES", 1), self.assertRaisesRegex(ValueError, "retained and generation was not retried"):
            module.inline_cached_images("/v0/management/plugins/gflow/api/tmp/" + filename, self.root)


    def test_inline_limit_is_128_mib_and_exact_boundary_is_accepted(self):
        self.assertEqual(module.MAX_INLINE_BYTES, 128 * 1024 * 1024)
        filename = "b" * 32 + ".webm"
        content = b"\x1a\x45\xdf\xa3"
        (self.root / filename).write_bytes(content)
        with patch.object(module, "MAX_INLINE_BYTES", len(content)):
            uri = module.inline_cached_images("/v0/resource/plugins/gflow/tmp/" + filename, self.root)
        self.assertEqual(base64.b64decode(uri.split(",", 1)[1]), content)

    def test_cache_growth_after_stat_is_still_bounded(self):
        filename = "c" * 32 + ".mov"
        path = self.root / filename
        path.write_bytes(b"synthetic")
        # Simulate a concurrent cache append after the initial size check.
        real_stat = Path.stat
        def stale_size(value, *args, **kwargs):
            info = real_stat(value, *args, **kwargs)
            return Mock(st_size=1, st_mode=info.st_mode) if value == path else info
        with patch.object(module, "MAX_INLINE_BYTES", 4), patch.object(Path, "stat", stale_size):
            with self.assertRaisesRegex(ValueError, "retained and generation was not retried"):
                module.inline_cached_images("/v0/resource/plugins/gflow/tmp/" + filename, self.root)
        self.assertEqual(path.read_bytes(), b"synthetic")


if __name__ == "__main__":
    unittest.main()
