import io
import os
import tempfile
import unittest
import zipfile
from pathlib import Path

from fastzip.algo._queue import QueueItem
from fastzip.algo._wrapfile import WrappedFile
from fastzip.chooser import CompressionChooser

from fastzip.types import LocalFileHeader
from fastzip.write import WZip


class WZipTest(unittest.TestCase):
    def test_instantiate(self) -> None:
        with tempfile.NamedTemporaryFile() as z:
            m = WZip(filename=Path("foo.zip"), fobj=z.file)
            self.assertEqual(os.cpu_count(), m._threads)
            m._shutdown()  # so _we_ can read the queue
            m._consumer_thread.join()

    def test_compress_default_should_be_deflate(self) -> None:
        with tempfile.NamedTemporaryFile() as z:
            m = WZip(filename=Path("foo.zip"), fobj=z.file)
            m._shutdown()  # so _we_ can read the queue
            m._consumer_thread.join()
            d = b"foo" * 100
            m.enqueue(
                LocalFileHeader._for_testing(usize=len(d), filename="foo/bar.py"),
                WrappedFile(io.BytesIO(d)),
            )

            item = m._queue.get()
            assert isinstance(item, QueueItem)

            self.assertTrue(m._queue.empty())
            self.assertEqual("foo/bar.py", item.partial_lfh.filename)
            self.assertEqual(len(d), item.partial_lfh.usize)

        self.assertEqual(
            # Not proud of hardcoding the deflate stream here, but this is
            # unlikely to change given the age and stability of the algorithm,
            # as well as this being clearly less than any reasonable split size
            # we'd choose.
            b"K\xcb\xcfO\x1bE\xc4!\x00",
            b"".join(x.result()[0] for x in item.compressed_data_futures),
        )

    def test_compress_explicitly_zstd(self) -> None:
        c = CompressionChooser(default="zstd@compression_level=1")
        with tempfile.NamedTemporaryFile() as z:
            m = WZip(filename=Path("foo.zip"), fobj=z.file, chooser=c)
            m._shutdown()  # so _we_ can read the queue
            m._consumer_thread.join()
            d = b"foo" * 100
            m.enqueue(
                LocalFileHeader._for_testing(usize=len(d), filename="foo/bar.py"),
                WrappedFile(io.BytesIO(d)),
            )
            item = m._queue.get()
            assert isinstance(item, QueueItem)

            self.assertTrue(m._queue.empty())

        self.assertEqual("foo/bar.py", item.partial_lfh.filename)
        self.assertEqual(len(d), item.partial_lfh.usize)
        self.assertEqual(
            # Not proud of hardcoding the zstd stream here -- this can and will
            # change over time, but we do get benefit out of noting whether
            # this includes the content size, checksum, or closes the frame
            # properly.
            b"(\xb5/\xfd`,\x00U\x00\x00\x18foo\x01\x00&\xaan\x08",
            b"".join(x.result()[0] for x in item.compressed_data_futures),
        )

    def test_zip64_files(self) -> None:
        b = io.BytesIO()
        with WZip(Path("foo.zip"), fobj=b, force_zip64=True) as z:
            for i in range(20):
                p = Path(f"{i}.txt")
                z.write(p, p, fobj=io.BytesIO(f"{i}\n".encode("ascii")))
        zf = zipfile.ZipFile(b)
        # TODO interrogate the zf to make sure it _was_ zip64
        self.assertEqual(20, len(zf.namelist()))
