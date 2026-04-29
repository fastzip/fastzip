import struct
import unittest
from dataclasses import asdict
from io import BytesIO

from fastzip.types import (
    BadLocalFileSignature,
    CentralDirectoryHeader,
    LOCAL_FILE_HEADER_FORMAT,
    LOCAL_FILE_HEADER_SIGNATURE,
    LocalFileHeader,
)


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

    def test_bad_signature_raises_and_resets_position(self) -> None:
        raw = struct.pack(
            LOCAL_FILE_HEADER_FORMAT,
            0xDEADBEEF,  # wrong signature
            20, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        )
        fo = BytesIO(raw)
        with self.assertRaises(BadLocalFileSignature):
            LocalFileHeader.read_from(fo)
        self.assertEqual(0, fo.tell(), "file position must be reset after bad signature")

    def test_extra_zero_length_record(self) -> None:
        # A single extra record with data_size=0 occupies exactly 4 bytes.
        # The old loop condition (i < len - 4) evaluated to (0 < 0) and skipped
        # it entirely, then raised ValueError("Extra length").
        extra = struct.pack("<HH", 0xCAFE, 0)  # id=0xCAFE, data_size=0
        raw = struct.pack(
            LOCAL_FILE_HEADER_FORMAT,
            LOCAL_FILE_HEADER_SIGNATURE,
            20, 0, 0, 0, 0, 0, 0, 0,
            3,           # filename_length
            len(extra),  # extra_length
        ) + b"foo" + extra
        h, buf = LocalFileHeader.read_from(BytesIO(raw))
        self.assertEqual(1, len(h.parsed_extra))
        self.assertEqual(0xCAFE, h.parsed_extra[0][0])
        self.assertEqual(b"", h.parsed_extra[0][1])

    def test_central_directory_zip64(self) -> None:
        h = LocalFileHeader._for_testing(123, "foo")
        cdh = CentralDirectoryHeader.from_lfh_and_relative_offset(h, 5_000_000_000)
        data, ver = cdh.dump()
        self.assertEqual(45, ver)
        self.assertIn(b"\x01\x00\x08\x00", data)
