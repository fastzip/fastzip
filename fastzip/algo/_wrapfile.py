import io
import mmap
import os
import time
from typing import Any, IO, Optional, Tuple


class WrappedFile:
    def __init__(self, fo: IO[bytes]) -> None:
        self.fo = fo
        self._mmap: Optional[mmap.mmap] = None
        self._stat: Optional[os.stat_result] = None

    def stat(self) -> os.stat_result:
        if self._stat is not None:
            return self._stat

        try:
            st = os.fstat(self.fo.fileno())
        except (TypeError, AttributeError, io.UnsupportedOperation):
            assert isinstance(self.fo, io.BytesIO)
            st = os.stat_result(
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
        self._stat = st
        return st

    def read(self, size: int = -1) -> bytes:
        return self.fo.read(size)

    def getsize(self) -> int:
        return self.stat().st_size

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
