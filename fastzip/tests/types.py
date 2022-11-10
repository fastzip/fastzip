import unittest
from dataclasses import asdict
from io import BytesIO

from fastzip.types import LocalFileHeader


class LocalFileHeaderTest(unittest.TestCase):
    def test_roundtrip(self) -> None:
        h = LocalFileHeader._for_testing(123, "foo")
        data = h.dump()
        # Truncated is an error
        with self.assertRaisesRegex(ValueError, "Short read: wanted 3 but got 2"):
            LocalFileHeader.read_from(BytesIO(data[:-1]))
        h2, buf = LocalFileHeader.read_from(BytesIO(data))
        self.assertEqual(asdict(h), asdict(h2))
