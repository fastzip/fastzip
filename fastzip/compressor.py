import concurrent.futures
import io
import zlib
from typing import Tuple

import zstandard

THREAD_BLOCK_SIZE = 1024 * 1024  # 1MiB

# Note: this API does not share threads well between different compressors, or
# different concurrent processes, or get the threading benefit for many small
# files.  For fastzip 2.0 expect a rewrite to express everything in terms of
# compress_block with a jobserver, and calculating crc-32 as blocks come in.
#
# The blocker (so to speak) is that zstandard currently does not expose any API
# that would let us write a single frame containing multiple blocks where we
# manage the block-compression threads.


class BaseCompressor:
    number: int
    short_name: str
    min_version: Tuple[int, int]

    _threads: int

    def __init__(self, threads: int) -> None:
        self._threads = threads

    def compress(self, data: bytes) -> bytes:  # pragma: no cover
        """
        Compress the given data in parallel.

        This function MUST NOT be called concurrently on the same instance.
        """
        raise NotImplementedError


class StoreCompressor(BaseCompressor):
    number = 0
    short_name = "store"
    min_version = (1, 0)  # folder support

    def compress(self, data: bytes) -> bytes:
        return data


class DeflateCompressor(BaseCompressor):
    number = 8
    min_version = (2, 0)

    _compresslevel: int
    _threadpool: concurrent.futures.ThreadPoolExecutor

    def __init__(
        self, threads: int, compresslevel: int = zlib.Z_DEFAULT_COMPRESSION
    ) -> None:
        super().__init__(threads=threads)
        self._compresslevel = compresslevel
        self._threadpool = concurrent.futures.ThreadPoolExecutor(self._threads)

    def compress(self, data: bytes) -> bytes:
        # TODO: Consider eventually exposing a file-like-object API, although
        # that comes with the complexity of needing to seek in the zip while
        # writing.

        buf = io.BytesIO()

        if len(data) == 0:
            block_starts = [0]
        else:
            block_starts = list(range(0, len(data), THREAD_BLOCK_SIZE))

        # DEFLATE streams can be concatenated, as long as a Z_FINISH block is
        # not issued too early.
        for start in block_starts:
            buf.write(
                self._compress_block(
                    data[start : start + THREAD_BLOCK_SIZE],
                    start == block_starts[-1],
                )
            )

        return buf.getvalue()

    def _compress_block(self, data: bytes, final: bool) -> bytes:
        obj = zlib.compressobj(self._compresslevel, zlib.DEFLATED, -15)
        # TODO benchmark to ensure the + isn't too expensive
        buf: bytes = obj.compress(data)

        # ref https://www.bolet.org/~pornin/deflate-flush-fr.html
        if final:
            # Passing the correct arg here saves 6 bytes vs compressing the
            # block with a full flush and appending a final flush!
            buf += obj.flush(zlib.Z_FINISH)
        else:
            buf += obj.flush(zlib.Z_FULL_FLUSH)

        return buf


class ZstdCompressor(BaseCompressor):
    number = 93
    min_version = (6, 5)

    _compresslevel: int
    _compressor: zstandard.ZstdCompressor
    _seqnum: int

    def __init__(self, threads: int, compresslevel: int = 10) -> None:
        super().__init__(threads=threads)
        self._compresslevel = compresslevel
        # TODO: Check the defaults this uses; we want the original length
        # encoded (as zipfile-zstd currently requires it), but don't really need
        # the content hash.
        self._compressor = zstandard.ZstdCompressor(
            self._compresslevel, threads=self._threads
        )
        self._seqnum = 0

    def compress(self, data: bytes) -> bytes:
        # This is certainly not atomic, but catches the silliest uses of calling
        # this concurrently.
        t = (self._seqnum + 1) % 1 << 31
        self._seqnum = t

        buf = self._compressor.compress(data)

        if self._seqnum != t:
            raise RuntimeError(f"compress called concurrently on {self!r}")

        return buf
