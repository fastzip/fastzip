import concurrent.futures
import io
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
                pool=e, file_object=WrappedFile(io.BytesIO(DEMO_DATA))
            ):
                buf.write(f.result()[0])

        self.assertEqual(DEMO_DATA, inst._decompress_for_testing(buf.getvalue()))

    def test_store(self) -> None:
        self._test("store")

    def test_deflate(self) -> None:
        self._test("deflate")

    def test_zstd(self) -> None:
        self._test("zstd")
