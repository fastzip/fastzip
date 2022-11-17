import unittest
from concurrent.futures import ThreadPoolExecutor

from fastzip.algo import find_compressor_cls
from fastzip.algo.zstd import ZstdCompressor
from fastzip.chooser import CompressionChooser, DEFAULT_CHOOSER
from fastzip.types import LocalFileHeader


class ChooserTest(unittest.TestCase):
    def test_defaults_store(self) -> None:
        data = b"Hello!"
        lfh = LocalFileHeader._for_testing(usize=len(data), filename="foo")
        c = DEFAULT_CHOOSER._choose_compressor(lfh)
        self.assertEqual("store", c)
        cls, params = find_compressor_cls(c)
        o = cls(threads=2, params=params)
        tp = ThreadPoolExecutor(max_workers=2)
        self.assertEqual(
            data,
            b"".join(
                x.result()[0]
                for x in o.compress_to_futures(
                    tp, len(data), mmap_future=memoryview(data)
                )
            ),
        )

    def test_defaults_deflate(self) -> None:
        data = b"Hello, World!!!"
        lfh = LocalFileHeader._for_testing(usize=len(data), filename="foo")
        c = DEFAULT_CHOOSER._choose_compressor(lfh)
        self.assertEqual("deflate@compresslevel=-1", c)
        cls, params = find_compressor_cls(c)
        o = cls(threads=2, params=params)
        tp = ThreadPoolExecutor(max_workers=2)
        self.assertEqual(
            data,
            o._decompress_for_testing(
                b"".join(
                    x.result()[0]
                    for x in o.compress_to_futures(
                        tp,
                        len(data),
                        memoryview(data),
                    )
                )
            ),
        )

    def test_zstd(self) -> None:
        data = b"Hello, World!!!"
        lfh = LocalFileHeader._for_testing(usize=len(data), filename="foo")
        c = CompressionChooser(default="zstd@compresslevel=9")._choose_compressor(lfh)
        self.assertEqual("zstd@compresslevel=9", c)
        cls, params = find_compressor_cls(c)
        o = cls(threads=2, params=params)
        tp = ThreadPoolExecutor(max_workers=2)
        self.assertEqual(
            data,
            o._decompress_for_testing(
                b"".join(
                    x.result()[0]
                    for x in o.compress_to_futures(tp, len(data), memoryview(data))
                )
            ),
        )

    def test_zstd_large(self) -> None:
        # This data chosen to be both larger than and relatively prime to the
        # block size of 128KiB
        data = b"abc" * 1024 * 1024
        lfh = LocalFileHeader._for_testing(usize=len(data), filename="foo")
        c = CompressionChooser(
            default="zstd@compresslevel=9,enable_ldm=1"
        )._choose_compressor(lfh)
        self.assertEqual("zstd@compresslevel=9,enable_ldm=1", c)
        cls, params = find_compressor_cls(c)
        o = cls(threads=2, params=params)

        assert isinstance(o, ZstdCompressor)
        self.assertEqual(9, o._compresslevel)
        tp = ThreadPoolExecutor(max_workers=2)
        self.assertEqual(
            data,
            o._decompress_for_testing(
                b"".join(
                    x.result()[0]
                    for x in o.compress_to_futures(tp, len(data), memoryview(data))
                )
            ),
        )
