import io
import mmap
import os
import time
from typing import Any, IO, Optional, Tuple


class WrappedFile:
    def __init__(self, fo: IO[bytes]) -> None:
        self.fo = fo
        self._mmap: Optional[mmap.mmap] = None

    def stat(self) -> os.stat_result:
        try:
            return os.fstat(self.fo.fileno())
        except (TypeError, AttributeError, io.UnsupportedOperation):
            assert isinstance(self.fo, io.BytesIO)
            return os.stat_result(
                (
                    0o644,  # mode
                    0,  # ino
                    0,  # dev
                    1,  # nlink
                    0,  # uid
                    0,  # gid
                    len(self.fo.getbuffer()),  # size
                    0,  # atime
                    time.time(),  # mtime
                    0,  # ctime
                )
            )

    def read(self, size: int = -1) -> bytes:
        return self.fo.read(size)

    def getsize(self) -> int:
        # TODO consider specialcasing BytesIO here as well and calling fstat
        # otherwise
        self.fo.seek(0, os.SEEK_END)
        v = self.fo.tell()
        self.fo.seek(0, os.SEEK_SET)
        return v

    def mmapwrapper(self) -> Tuple[int, memoryview]:
        try:
            fileno = self.fo.fileno()
        except io.UnsupportedOperation:
            # To make testing easier,
            assert isinstance(self.fo, io.BytesIO)
            buf = self.fo.getbuffer()
            return len(buf), buf
        else:
            length = self.getsize()

            if length == 0:
                # We can't make a zero-length mapping on Windows, but it's pretty
                # useless to make one anywhere.
                return length, memoryview(b"")
            self._mmap = mmap.mmap(fileno, length, mmap.MAP_PRIVATE)
            return length, memoryview(self._mmap)

    def __enter__(self) -> "WrappedFile":
        return self

    def __exit__(self, *args: Any) -> None:
        if self._mmap:
            self._mmap.close()
        self.fo.close()
