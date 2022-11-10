from concurrent.futures import Executor, Future
from typing import Iterable, Optional, Tuple
from zlib import crc32

from ._base import BaseCompressor
from ._wrapfile import WrappedFile


class StoreCompressor(BaseCompressor):
    number = 0
    short_name = "store"
    version_needed = 10  # folder support

    def __init__(self, threads: int, params: str = "") -> None:
        super().__init__(threads=threads)
        assert params == ""

    def compress_to_futures(
        self, pool: Executor, file_object: WrappedFile
    ) -> Iterable[Future[Tuple[bytes, int, Optional[int]]]]:
        def func() -> Tuple[bytes, int, int]:
            raw_data = file_object.read()
            return (raw_data, len(raw_data), crc32(raw_data))

        return [pool.submit(func)]

    def _decompress_for_testing(self, data: bytes) -> bytes:
        return data