from __future__ import annotations

import io
import mmap
import os
import time
from concurrent.futures import Future
from threading import Lock
from typing import Any, IO, Optional, Tuple, Union

from keke import kev


class WrappedFile:
    def __init__(self, fo: Union[IO[bytes], Future[IO[bytes]]]) -> None:
        self.fo = fo
        self._mmap: Optional[mmap.mmap] = None
        self._stat: Optional[os.stat_result] = None
        self._cached_mmap: Optional[Tuple[int, memoryview]] = None
        self._lock = Lock()

    def _ensure_future_result(self) -> None:
        with self._lock:
            if isinstance(self.fo, Future):
                self.fo = self.fo.result()

    def stat(self) -> os.stat_result:
        self._ensure_future_result()
        assert not isinstance(self.fo, Future)

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
        self._ensure_future_result()
        assert not isinstance(self.fo, Future)
        return self.fo.read(size)

    def getsize(self) -> int:
        return self.stat().st_size

    def mmapwrapper(self) -> Tuple[int, memoryview]:
        self._ensure_future_result()
        assert not isinstance(self.fo, Future)

        if self._cached_mmap is None:
            with kev("mmapwrapper", __name__):
                self._cached_mmap = self._mmapwrapper()
        return self._cached_mmap

    def _mmapwrapper(self) -> Tuple[int, memoryview]:
        self._ensure_future_result()
        assert not isinstance(self.fo, Future)

        try:
            fileno = self.fo.fileno()
        except io.UnsupportedOperation:
            # To make testing easier,
            assert isinstance(self.fo, io.BytesIO)
            buf = self.fo.getbuffer()
            return len(buf), buf
        else:
            with kev("getsize", __name__):
                length = self.getsize()

            if length == 0:
                # We can't make a zero-length mapping on Windows, but it's pretty
                # useless to make one anywhere.
                return length, memoryview(b"")
            elif length <= 32768:
                with kev("read", __name__, size=length):
                    return length, memoryview(self.fo.read(length))
            with kev("mmap", __name__, size=length):
                self._mmap = mmap.mmap(
                    fileno, length, mmap.MAP_SHARED, prot=mmap.PROT_READ
                )
                return length, memoryview(self._mmap)

    def __enter__(self) -> "WrappedFile":
        return self

    def __exit__(self, *args: Any) -> None:
        if self._mmap:
            self._cached_mmap = None
            self._mmap.close()
        # XXX this exit being be called in the same executor that the future is
        # from... hopefully the result is already ready, we just need to fetch
        # it.
        if isinstance(self.fo, Future):
            print("DEADLOCK?")
        else:
            self.fo.close()
