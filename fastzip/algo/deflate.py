import zlib
from concurrent.futures import Executor, Future
from typing import Iterable, Optional, Tuple, Union

from ._base import BaseCompressor, parse_params
from ._wrapfile import WrappedFile

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
        self, pool: Executor, file_object: WrappedFile
    ) -> Iterable[Future[Tuple[bytes, int, Optional[int]]]]:
        # TODO: Size could be passed in instead
        size = file_object.getsize()

        if size == 0:
            block_starts = [0]
        else:
            block_starts = list(range(0, size, THREAD_BLOCK_SIZE))

        # DEFLATE streams can be concatenated, as long as a Z_FINISH block is
        # not issued too early.
        _, m = file_object.mmapwrapper()
        for start in block_starts:
            yield pool.submit(
                self._compress_block,
                # TODO test with .mmapwrapper()
                m[start : min(size, start + THREAD_BLOCK_SIZE)],
                start == block_starts[-1],
            )

    def _compress_block(
        self, data: Union[bytes, memoryview], final: bool
    ) -> Tuple[bytes, int, int]:
        # zlib compressobj are incredibly cheap, we'll just create a new one
        # each time and let it go out of scope.
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

        return (buf, len(data), zlib.crc32(data))

    def _decompress_for_testing(self, data: bytes) -> bytes:
        obj = zlib.decompressobj(-15)
        return obj.decompress(data)
