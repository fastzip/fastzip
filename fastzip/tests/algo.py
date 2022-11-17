import concurrent.futures
import io
import os
import unittest

from fastzip.algo import find_compressor_cls
from fastzip.algo._wrapfile import WrappedFile

DEMO_DATA = b"Hello World"


class LookupRoundtripTest(unittest.TestCase):
    def _test(self, algo_str: str) -> None:
        cls, params = find_compressor_cls(algo_str)
        inst = cls(threads=2, params=params)
        buf = io.BytesIO()
        with concurrent.futures.ThreadPoolExecutor() as e:
            for f in inst.compress_to_futures(
                pool=e, size=len(DEMO_DATA), mmap_future=memoryview(DEMO_DATA)
            ):
                buf.write(f.result()[0])

        self.assertEqual(DEMO_DATA, inst._decompress_for_testing(buf.getvalue()))

    def test_store(self) -> None:
        self._test("store")

    def test_deflate(self) -> None:
        self._test("deflate")

    def test_zstd(self) -> None:
        self._test("zstd")


class WrappedFileTest(unittest.TestCase):
    def test_stat(self) -> None:
        with open(__file__, "rb") as f:
            w = WrappedFile(f)
            expected_size = os.stat(__file__).st_size
            # Twice on purpose; second uses fast path
            self.assertEqual(expected_size, w.stat().st_size)
            self.assertEqual(expected_size, w.stat().st_size)
            self.assertEqual(expected_size, w.getsize())

    def test_mmap_real_file(self) -> None:
        with open(__file__, "rb") as f:
            w = WrappedFile(f)
            expected_size = os.stat(__file__).st_size
            s, b = w.mmapwrapper()
            self.assertEqual(expected_size, s)
            f.seek(0)
            self.assertEqual(bytes(b), f.read())

    def test_mmap_bytesio(self) -> None:
        f = io.BytesIO(b"abcdef")
        w = WrappedFile(f)

        expected_size = 6
        s, b = w.mmapwrapper()
        self.assertEqual(expected_size, s)
        self.assertEqual(b, f.getvalue())
