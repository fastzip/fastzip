import io
import unittest
import zipfile
from pathlib import Path

from fastzip.read import RZipStream
from fastzip.write import WZip


class ReadTest(unittest.TestCase):
    def test_zipmerge(self) -> None:
        # This tests read+write but has to exist somewhere, may as well be in
        # this file.
        b1 = io.BytesIO()
        b2 = io.BytesIO()

        with zipfile.ZipFile(b1, mode="w") as zf1:
            zf1.writestr("path1", "Data1")
        b1.seek(0)

        with zipfile.ZipFile(b2, mode="w") as zf2:
            zf2.writestr("path2", "Data2")
        b2.seek(0)

        b3 = io.BytesIO()
        with WZip(Path("foo.zip"), fobj=b3) as z:
            for lfh, header_data, file_data in RZipStream(
                Path("zip1"), fobj=b1
            ).entries():
                z.enqueue_precompressed(lfh, header_data, file_data)
            for lfh, header_data, file_data in RZipStream(
                Path("zip2"), fobj=b2
            ).entries():
                z.enqueue_precompressed(lfh, header_data, file_data)

        zf = zipfile.ZipFile(b3)
        self.assertEqual(["path1", "path2"], zf.namelist())
