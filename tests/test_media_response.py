import base64
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

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

    def test_oversize_media_reports_authenticated_retrieval_without_resubmission(self):
        filename = "a" * 32 + ".mp4"
        (self.root / filename).write_bytes(b"large")
        with patch.object(module, "MAX_INLINE_BYTES", 1), self.assertRaisesRegex(ValueError, "authenticated"):
            module.inline_cached_images("/v0/management/plugins/gflow/api/tmp/" + filename, self.root)

    def test_stream_large_media_uses_bounded_chunks_and_keeps_final_chunk_small(self):
        filename = "b" * 32 + "_2K.jpg"
        body = bytes(range(256)) * 4096
        (self.root / filename).write_bytes(body)
        with patch.object(module, "STREAM_INLINE_BYTES", 1024):
            chunks = module.stream_cached_media(
                'data: {"choices":[{"delta":{"content":"![Generated Image](/v0/resource/plugins/gflow/tmp/' + filename + ')"},"finish_reason":"stop"}]}\n\n',
                self.root,
            )
        self.assertGreater(len(chunks), 2)
        self.assertTrue(all(len(chunk.encode()) < 100_000 for chunk in chunks))
        self.assertTrue(all('gflow_media' in chunk for chunk in chunks[:-1]))
        self.assertEqual([module.json.loads(chunk[6:].strip())['gflow_media']['seq'] for chunk in chunks[:-1]], list(range(len(chunks) - 1)))
        self.assertEqual(module.json.loads(chunks[0][6:].strip())['gflow_media']['size'], len(body))
        self.assertNotIn(filename, chunks[-1])
        self.assertNotIn('data:image', chunks[-1])


if __name__ == "__main__":
    unittest.main()
