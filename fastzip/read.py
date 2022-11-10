import sys
from pathlib import Path
from typing import Callable, IO, Iterator, Optional, Tuple

from .types import EndOfLocalFiles, LocalFileHeader
from .util import _readn


class RZipBase:
    def __init__(self, filename: Path) -> None:
        self._filename = filename


class RZipStream(RZipBase):
    """
    Reads a zip file from the beginning.

    Doing so ignores the central directory, but is also much more suitable for
    streaming.  This class only reads a subset of zips that have no gaps
    between files, and don't have data descriptors.
    """

    _fobj: IO[bytes]

    def __init__(
        self,
        filename: Path,
        fobj: Optional[IO[bytes]] = None,
        max_search: int = 0,
    ) -> None:
        self._filename = filename
        if fobj:
            # Presumed to be at the correct position
            self._fobj = fobj
        else:
            self._fobj = open(filename, "rb")
        # TODO support max_search, which requires a tee or seeking

    def entries(
        self, callback: Optional[Callable[[LocalFileHeader], bool]] = None
    ) -> Iterator[Tuple[LocalFileHeader, bytes, bytes]]:
        """
        Yields `(local_file_header, header_data, file_data)` for each entry that
        `callback(local_file_header)` returns True.

        `header_data` includes the local file header and extra data, while
        `file_data` includes just the compressed data exactly as it was in the
        original zip so it can be copied to a new one.  To use this to write a
        correct zip, you will need to keep track of where you write this opaque
        data so you can also output the central directory afterwards.

        Currently ignores the central directory when reading and simply reads
        local files starting at offset 0.  This is expected to change in a
        future version for compatibility, but retain the fast path (and
        validation, due to some spec ambiguity).

        Only call this function once.
        """

        while True:
            # The position really only matters for debugging
            # pos = self._fobj.tell()
            try:
                lfh, buf = LocalFileHeader.read_from(self._fobj)
                assert lfh.crc32 is not None
            except EndOfLocalFiles:
                break

            buf2 = _readn(self._fobj, lfh.csize)
            if callback is None or callback(lfh):
                yield (lfh, buf, buf2)


if __name__ == "__main__":
    z = RZipStream(Path(sys.argv[1]))
    for lfh, buf, buf2 in z.entries():
        print(lfh)
