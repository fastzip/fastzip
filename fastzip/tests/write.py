import io
import os
import stat
import sys
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

    @unittest.skipIf(sys.platform == "win32", "unix permissions not supported on windows")
    def test_unix_permissions_preserved(self) -> None:
        # Write a real file with a distinctive, non-default mode, zip it with
        # fastzip, and verify the unix permission bits survive the round-trip.
        #
        # By convention, the unix mode is stored in the high 16 bits of the
        # central directory's external_attributes field (this is how
        # cpython's zipfile and Info-ZIP encode it).
        mode = 0o750
        with tempfile.TemporaryDirectory() as d:
            src = Path(d) / "script.sh"
            src.write_bytes(b"#!/bin/sh\necho hello\n")
            os.chmod(src, mode)

            b = io.BytesIO()
            with WZip(Path("foo.zip"), fobj=b) as z:
                z.write(src, Path("script.sh"))

            zf = zipfile.ZipFile(b)
            info = zf.getinfo("script.sh")
            stored_mode = (info.external_attr >> 16) & 0o7777
            self.assertEqual(
                mode,
                stored_mode,
                f"expected unix mode {mode:o}, got {stored_mode:o}",
            )

    @unittest.skipIf(sys.platform == "win32", "symlinks not supported on windows")
    def test_write_symlink(self) -> None:
        # write_symlink should store the link target as the (uncompressed)
        # entry content and flag the entry as a symlink via S_IFLNK in the
        # external attributes, so compliant extractors recreate it as a link.
        target = "libsome-1.1.so"
        b = io.BytesIO()
        with WZip(Path("foo.zip"), fobj=b) as z:
            z.write_symlink(
                Path("lib/libsome.so.1"),
                target,
                synthetic_mtime=1577836800,
            )

        zf = zipfile.ZipFile(b)
        self.assertEqual(["lib/libsome.so.1"], zf.namelist())
        info = zf.getinfo("lib/libsome.so.1")
        stored_mode = info.external_attr >> 16
        self.assertTrue(
            stat.S_ISLNK(stored_mode),
            f"expected S_IFLNK, got mode {stored_mode:o}",
        )
        self.assertEqual(0o777, stored_mode & 0o7777)
        self.assertEqual(target, zf.read("lib/libsome.so.1").decode())
        self.assertEqual((2020, 1, 1, 0, 0, 0), info.date_time)

    def test_write_dir(self) -> None:
        # write_dir should store an empty directory entry: zero content, a name
        # ending in '/', and a mode flagged as a directory so extractors
        # recreate it.
        b = io.BytesIO()
        with WZip(Path("foo.zip"), fobj=b) as z:
            z.write_dir(Path("empty"), synthetic_mtime=1577836800)

        zf = zipfile.ZipFile(b)
        self.assertEqual(["empty/"], zf.namelist())
        info = zf.getinfo("empty/")
        self.assertTrue(info.is_dir())
        self.assertEqual(0, info.file_size)
        self.assertEqual(b"", zf.read("empty/"))
        self.assertEqual((2020, 1, 1, 0, 0, 0), info.date_time)

    @unittest.skipIf(sys.platform == "win32", "unix permissions not supported on windows")
    def test_write_dir_mode(self) -> None:
        # A caller-supplied permission mode should be preserved, with the
        # directory type bits (S_IFDIR) added automatically.
        b = io.BytesIO()
        with WZip(Path("foo.zip"), fobj=b) as z:
            z.write_dir(Path("perms"), mode=0o700)

        zf = zipfile.ZipFile(b)
        info = zf.getinfo("perms/")
        stored_mode = info.external_attr >> 16
        self.assertTrue(stat.S_ISDIR(stored_mode))
        self.assertEqual(0o700, stored_mode & 0o7777)

    def test_write_dir_already_trailing_slash(self) -> None:
        # Passing a name that already ends in '/' should not double the slash.
        b = io.BytesIO()
        with WZip(Path("foo.zip"), fobj=b) as z:
            z.write_dir(Path("nested/dir"))

        zf = zipfile.ZipFile(b)
        self.assertEqual(["nested/dir/"], zf.namelist())

    def test_rwrite_preserves_empty_dirs(self) -> None:
        # rwrite should emit an explicit entry for an empty directory while
        # representing populated directories implicitly through their files.
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            (root / "empty").mkdir()
            (root / "full").mkdir()
            (root / "full" / "file.txt").write_bytes(b"hello")

            b = io.BytesIO()
            with WZip(Path("foo.zip"), fobj=b) as z:
                z.rwrite(root / "empty")
                z.rwrite(root / "full")

            zf = zipfile.ZipFile(b)
            names = set(zf.namelist())
            empty_name = (root / "empty").relative_to(root.anchor).as_posix() + "/"
            file_name = (root / "full" / "file.txt").relative_to(root.anchor).as_posix()
            self.assertIn(empty_name, names)
            self.assertIn(file_name, names)
            self.assertTrue(zf.getinfo(empty_name).is_dir())

    def test_empty_zip(self) -> None:
        # Creating a WZip without writing any entries should produce a valid,
        # empty archive (just an EOCD record), matching the behaviour of
        # `zipfile.ZipFile(path, "w")` used as a no-op context manager.
        b = io.BytesIO()
        with WZip(Path("foo.zip"), fobj=b):
            pass

        zf = zipfile.ZipFile(b)
        self.assertEqual([], zf.namelist())
        self.assertIsNone(zf.testzip())

    def test_empty_zip_force_zip64(self) -> None:
        # The same, but exercising the zip64 EOCD path with no entries.
        b = io.BytesIO()
        with WZip(Path("foo.zip"), fobj=b, force_zip64=True):
            pass

        zf = zipfile.ZipFile(b)
        self.assertEqual([], zf.namelist())
        self.assertIsNone(zf.testzip())
