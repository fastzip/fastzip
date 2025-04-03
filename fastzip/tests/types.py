import unittest
from dataclasses import asdict
from io import BytesIO

from fastzip.types import CentralDirectoryHeader, LocalFileHeader


class LocalFileHeaderTest(unittest.TestCase):
    def test_roundtrip(self) -> None:
        h = LocalFileHeader._for_testing(123, "foo")
        data, ver = h.dump()
        # Truncated is an error
        with self.assertRaisesRegex(ValueError, "Short read: wanted 3 but got 2"):
            LocalFileHeader.read_from(BytesIO(data[:-1]))
        h2, buf = LocalFileHeader.read_from(BytesIO(data))
        self.assertEqual(asdict(h), asdict(h2))

    def test_zip64(self) -> None:
        h = LocalFileHeader._for_testing(8_000_000_000, "foo")
        self.assertEqual(20, h.version_needed)
        data, ver = h.dump()
        h2, buf = LocalFileHeader.read_from(BytesIO(data))
        # print(h2.parsed_extra)
        self.assertEqual(8_000_000_000, h2.usize)
        self.assertEqual(20, h.version_needed)
        self.assertEqual(45, h2.version_needed)

        cdh = CentralDirectoryHeader.from_lfh_and_relative_offset(h2, 0)

        self.assertEqual(8_000_000_000, cdh.usize)
        self.assertEqual(45, cdh.version_needed)

        data = cdh.dump()
        cdh2, buf = CentralDirectoryHeader.read_from(BytesIO(data))
        self.assertEqual(8_000_000_000, cdh2.usize)
        self.assertEqual(45, cdh2.version_needed)
