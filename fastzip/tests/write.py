import io
import os
import tempfile
import unittest
import zipfile
from pathlib import Path

from fastzip.__main__ import extract, verify
from fastzip.algo._queue import QueueItem
from fastzip.algo._wrapfile import WrappedFile
from fastzip.chooser import CompressionChooser
from fastzip.read import RZipStream

from fastzip.types import LocalFileHeader
from fastzip.write import WZip


class WZipTest(unittest.TestCase):
    def test_instantiate(self) -> None:
        with tempfile.NamedTemporaryFile() as z:
            m = WZip(filename=Path("foo.zip"), fobj=z.file)
            self.assertEqual(os.cpu_count(), m._threads)
            m._shutdown()  # so _we_ can read the queue
            m._consumer_thread.join()

    def test_empty_archive(self) -> None:
        b = io.BytesIO()
        with WZip(filename=Path("foo.zip"), fobj=b):
            pass
        b.seek(0)
        self.assertEqual([], zipfile.ZipFile(b).namelist())

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

    def test_verify_and_extract_zstd(self) -> None:
        d = b"foo" * 100
        tmp = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
        tmp.close()
        try:
            with open(tmp.name, "wb") as fo:
                with WZip(
                    filename=Path(tmp.name),
                    fobj=fo,
                    chooser=CompressionChooser(default="zstd@compression_level=1"),
                ) as z:
                    z.enqueue(
                        LocalFileHeader._for_testing(
                            usize=len(d), filename="foo/bar.py"
                        ),
                        WrappedFile(io.BytesIO(d)),
                    )

            self.assertEqual(0, verify(tmp.name))

            with tempfile.TemporaryDirectory() as td:
                self.assertEqual(0, extract(tmp.name, td))
                self.assertEqual(d, Path(td, "foo/bar.py").read_bytes())
        finally:
            os.unlink(tmp.name)

    def test_merge_preserves_extra_fields(self) -> None:
        extra = b"\xfe\xca\x04\x00ABCD"
        source = io.BytesIO()
        with zipfile.ZipFile(source, "w") as zf:
            zi = zipfile.ZipInfo("a.txt")
            zi.extra = extra
            zf.writestr(zi, b"data")
        source.seek(0)

        out = io.BytesIO()
        with WZip(Path("out.zip"), fobj=out) as wz:
            for lfh, header_data, file_data in RZipStream(
                Path("in.zip"), fobj=source
            ).entries():
                wz.enqueue_precompressed(lfh, header_data, file_data)

        out.seek(0)
        info = zipfile.ZipFile(out).getinfo("a.txt")
        self.assertEqual(extra, info.extra)

    def test_zip64_files(self) -> None:
        b = io.BytesIO()
        with WZip(Path("foo.zip"), fobj=b, force_zip64=True) as z:
            for i in range(20):
                p = Path(f"{i}.txt")
                z.write(p, p, fobj=io.BytesIO(f"{i}\n".encode("ascii")))
        zf = zipfile.ZipFile(b)
        # TODO interrogate the zf to make sure it _was_ zip64
        self.assertEqual(20, len(zf.namelist()))

    def test_zip64_bytes_written_is_accurate(self) -> None:
        # _bytes_written was not updated after writing the Zip64 EOCD record
        # (44 bytes), so it underreported the file size when force_zip64=True.
        b = io.BytesIO()
        with WZip(Path("foo.zip"), fobj=b, force_zip64=True) as z:
            z.write(Path("a.txt"), Path("a.txt"), fobj=io.BytesIO(b"hello"))
        self.assertEqual(len(b.getvalue()), z._bytes_written)
