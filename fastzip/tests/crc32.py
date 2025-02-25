import unittest
import zlib

from fastzip._crc32_combine import _crc32_combine_pure, crc32_combine

SAMPLE1 = b"abcdef"
SAMPLE2 = b"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"


class Crc32Test(unittest.TestCase):
    @unittest.skipIf(
        crc32_combine is _crc32_combine_pure, "Using fallback crc32_combine"
    )
    def test_crc32_combine(self) -> None:
        # This is the ctypes one
        self.assertEqual(
            zlib.crc32(SAMPLE1 + SAMPLE2),
            crc32_combine(zlib.crc32(SAMPLE1), zlib.crc32(SAMPLE2), len(SAMPLE2)),
        )

    def test_fallback_crc32_combine(self) -> None:
        self.assertEqual(
            zlib.crc32(SAMPLE1 + SAMPLE2),
            _crc32_combine_pure(zlib.crc32(SAMPLE1), zlib.crc32(SAMPLE2), len(SAMPLE2)),
        )
