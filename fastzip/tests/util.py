import io
import unittest

from ..util import _readn, _slicen


class UtilTest(unittest.TestCase):
    def test_readn(self) -> None:
        b = io.BytesIO(b"abcd")
        self.assertEqual(b"ab", _readn(b, 2))
        self.assertEqual(2, b.tell())

        with self.assertRaisesRegex(ValueError, "Short read: wanted 4 but got 2"):
            _readn(b, 4)

        # Note: currently does not reset position on failure!
        self.assertEqual(4, b.tell())

    def test_slicecn(self) -> None:
        b = b"abcd"
        self.assertEqual(b"ab", _slicen(b, 0, 2))
        self.assertEqual(b"cd", _slicen(b, 2, 2))
        with self.assertRaisesRegex(ValueError, "Short read: wanted 4 but got 2"):
            _slicen(b, 2, 4)
