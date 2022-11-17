from __future__ import annotations

import zlib
from concurrent.futures import Executor, Future
from typing import Optional, Sequence, Tuple, Union

from keke import kev

from ._base import BaseCompressor, parse_params

THREAD_BLOCK_SIZE = 1024 * 1024  # 1MiB


class DeflateCompressor(BaseCompressor):
    number = 8
    version_needed = 20

    _compresslevel: int

    def __init__(self, threads: int, params: str = "") -> None:
        super().__init__(threads=threads)

        self._compresslevel = zlib.Z_DEFAULT_COMPRESSION
        for k, v in parse_params(params).items():
            if k == "compresslevel":
                assert -1 <= v <= 9
                self._compresslevel = v
            else:
                raise ValueError(f"Unknown param {k!r} for {self.__class__.__name__}")

    def compress_to_futures(
        self,
        pool: Executor,
        size: int,
        mmap_future: Union[memoryview, Future[memoryview]],
    ) -> Sequence[Future[Tuple[bytes, int, Optional[int]]]]:
        with kev("block_starts", __name__):
            if size == 0:
                block_starts = [0]
            else:
                block_starts = list(range(0, size, THREAD_BLOCK_SIZE))

        # DEFLATE streams can be concatenated, as long as a Z_FINISH block is
        # not issued too early.

        with kev("pool.submit", __name__):
            return [
                pool.submit(
                    self._compress_block,
                    # TODO test with .mmapwrapper()
                    mmap_future,
                    start,
                    min(size, start + THREAD_BLOCK_SIZE),
                    start == block_starts[-1],
                )
                for start in block_starts
            ]

    def _compress_block(
        self,
        data: Union[bytes, memoryview, Future[memoryview]],
        a: int,
        b: int,
        final: bool,
    ) -> Tuple[bytes, int, int]:
        if isinstance(data, Future):
            with kev(".result"):
                data = data.result()

        data = data[a:b]

        with kev("compressobj", __name__):
            # zlib compressobj are incredibly cheap, we'll just create a new one
            # each time and let it go out of scope.
            obj = zlib.compressobj(self._compresslevel, zlib.DEFLATED, -15)
        with kev("compress", __name__, size=len(data)):
            # TODO benchmark to ensure the + isn't too expensive
            buf: bytes = obj.compress(data)

        with kev("flush", __name__, final=final):
            # ref https://www.bolet.org/~pornin/deflate-flush-fr.html
            if final:
                # Passing the correct arg here saves 6 bytes vs compressing the
                # block with a full flush and appending a final flush!
                buf += obj.flush(zlib.Z_FINISH)
            else:
                buf += obj.flush(zlib.Z_FULL_FLUSH)

        with kev("crc32", __name__):
            crc = zlib.crc32(data)

        return (buf, len(data), crc)

    def _decompress_for_testing(self, data: bytes) -> bytes:
        obj = zlib.decompressobj(-15)
        return obj.decompress(data)
