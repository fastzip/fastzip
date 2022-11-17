from __future__ import annotations

from concurrent.futures import Executor, Future
from typing import Optional, Sequence, Tuple, Union
from zlib import crc32

from keke import kev

from ._base import BaseCompressor


class StoreCompressor(BaseCompressor):
    number = 0
    short_name = "store"
    version_needed = 10  # folder support

    def __init__(self, threads: int, params: str = "") -> None:
        super().__init__(threads=threads)
        assert params == ""

    def compress_to_futures(
        self,
        pool: Executor,
        size: int,
        mmap_future: Union[memoryview, Future[memoryview]],
    ) -> Sequence[Future[Tuple[bytes, int, Optional[int]]]]:
        # TODO Tuple[Union[bytes, memoryview], int, int]
        def func() -> Tuple[bytes, int, int]:
            with kev("read", __name__):
                if isinstance(mmap_future, Future):
                    raw_data = mmap_future.result()
                else:
                    raw_data = mmap_future
            with kev("crc", __name__):
                crc = crc32(raw_data)
            return (raw_data, len(raw_data), crc)

        return [pool.submit(func)]

    def _decompress_for_testing(self, data: bytes) -> bytes:
        return data
