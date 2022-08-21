import os
import sys
from pathlib import Path
from typing import BinaryIO, Callable, Iterator, Optional, Tuple

from .types import _readn, EndOfLocalFiles, LocalFileHeader


class FastzipInput:
    _filename: os.PathLike[str]
    _fobj: BinaryIO

    def __init__(self, filename: os.PathLike[str]) -> None:
        self._filename = filename
        self._fobj = open(filename, "rb")

    def entries(
        self, callback: Optional[Callable[[LocalFileHeader], bool]] = None
    ) -> Iterator[Tuple[LocalFileHeader, bytes]]:
        """
        Yields `(local_file_header, file_data)` for each entry that
        `callback(local_file_header)` returns True.

        `file_data` includes the local file header, extra data, and compressed
        data exactly as it was in the original zip so it can be copied to a new
        one.  To use this to write a correct zip, you will need to keep track of
        where you write this opaque data so you can also output the central
        directory afterwards.

        Currently ignores the central directory when reading and simply reads
        local files starting at offset 0.  This is expected to change in a
        future version for compatibility, but retain the fast path (and
        validation, due to some spec ambiguity).
        """

        # TODO support an initial offset with a helper that reads the central
        # directory offsets.

        while True:
            # pos = self._fobj.tell()
            try:
                lfh, buf = LocalFileHeader.read_from(self._fobj)
            except EndOfLocalFiles:
                break

            buf += _readn(self._fobj, lfh.csize)
            if callback is None or callback(lfh):
                yield (lfh, buf)


if __name__ == "__main__":
    f = FastzipInput(Path(sys.argv[1]))
    for e, b in f.entries():
        print(e, len(b))
