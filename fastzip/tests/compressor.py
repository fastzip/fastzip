import unittest
import zlib
from unittest import mock

import zstandard

from ..compressor import DeflateCompressor, StoreCompressor, ZstdCompressor


class CompressorTest(unittest.TestCase):
    def test_store(self):
        comp = StoreCompressor(threads=2)
        self.assertEqual(b"", comp.compress(b""))
        self.assertEqual(b"abc", comp.compress(b"abc"))

    def test_deflate(self):
        comp = DeflateCompressor(threads=2)

        def check(x):
            with self.subTest(f"len={len(x)}"):
                self.assertEqual(x, zlib.decompress(comp.compress(x), -15))

        check(b"")
        check(b"abc")

        # intentionally larger than THREAD_BLOCK_SIZE, and with len(b'abc')
        # relatively prime to it
        check(b"abc" * 1024 * 1024)

    def test_zstandard(self):
        comp = ZstdCompressor(threads=2)

        def check(x):
            with self.subTest(f"len={len(x)}"):
                self.assertEqual(
                    x, zstandard.ZstdDecompressor().decompress(comp.compress(x))
                )

        check(b"")
        check(b"abc")

        # intentionally larger than the hardcoded zstandard block size of
        # 128KiB, and with len(b'abc') relatively prime to it
        check(b"abc" * 1024 * 1024)
