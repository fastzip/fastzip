from __future__ import annotations

import io
import logging
from concurrent.futures import Executor, Future
from threading import Condition
from typing import Optional, Sequence, Tuple, Union
from zlib import crc32

import zstandard

from keke import kev

from ._base import BaseCompressor, parse_params
from ._freelist import FactoryFreelist

ZSTD_SINGLE_THRESHOLD = 16 * 1024 * 1024

LOG = logging.getLogger(__name__)


class ZstdCompressor(BaseCompressor):
    number = 93
    version_needed = 65

    _compresslevel: int

    def __init__(self, threads: int, params: str = "") -> None:
        super().__init__(threads=threads)
        paramdict = parse_params(params)
        self._compresslevel = paramdict.pop("compresslevel", 10)
        self._single_params = zstandard.ZstdCompressionParameters.from_level(
            self._compresslevel, **paramdict, threads=0
        )
        self._multi_params = zstandard.ZstdCompressionParameters.from_level(
            self._compresslevel, **paramdict, threads=self._threads
        )

        self._single_freelist: FactoryFreelist[
            zstandard.ZstdCompressor
        ] = FactoryFreelist(self._single_chunk_factory)
        self._multi_freelist: FactoryFreelist[
            zstandard.ZstdCompressor
        ] = FactoryFreelist(self._multi_chunk_factory)

    # N.b. the defaults write_checksum=True and write_content_size=True are
    # kept intact; we probably want these for compatibility with
    # https://pypi.org/project/zipfile-zstd/ decompression.
    def _single_chunk_factory(self) -> zstandard.ZstdCompressor:
        LOG.debug("single_chunk_factory")
        with kev("factory s"):
            return zstandard.ZstdCompressor(compression_params=self._single_params)

    def _multi_chunk_factory(self) -> zstandard.ZstdCompressor:
        LOG.debug("multi_chunk_factory")
        with kev("factory m"):
            return zstandard.ZstdCompressor(compression_params=self._multi_params)

    def compress_to_futures(
        self,
        pool: Executor,
        size: int,
        mmap_future: Union[memoryview, Future[memoryview]],
    ) -> Sequence[Future[Tuple[bytes, int, Optional[int]]]]:
        if size < ZSTD_SINGLE_THRESHOLD:

            def func() -> Tuple[bytes, int, Optional[int]]:
                # print("single")
                with kev("zstd s"):
                    with kev("enter"):
                        obj = self._single_freelist.enter()
                    if isinstance(mmap_future, Future):
                        with kev("read"):
                            raw_data = mmap_future.result()
                    else:
                        raw_data = mmap_future
                    with kev("compress"):
                        data = obj.compress(raw_data)
                    with kev("leave"):
                        self._single_freelist.leave(obj)
                    with kev("crc"):
                        crc = crc32(raw_data)
                return (data, len(raw_data), crc)

            # This only consumes one slot (single-threaded)
            return [pool.submit(func)]
        else:
            cond = Condition()
            done: bool = False

            def func() -> Tuple[bytes, int, Optional[int]]:
                # print("multi")
                with kev("enter"):
                    obj = self._multi_freelist.enter()
                nonlocal done
                with cond:
                    buf = io.BytesIO()
                    with kev("compressobj"):
                        compobj = obj.compressobj(size)
                    running_crc = crc32(b"")

                    if isinstance(mmap_future, Future):
                        with kev("read"):
                            raw_data = mmap_future.result()
                    else:
                        raw_data = mmap_future

                    start = 0
                    while chunk := raw_data[
                        start : start + zstandard.COMPRESSION_RECOMMENDED_INPUT_SIZE
                    ]:
                        with kev("compress/write"):
                            buf.write(compobj.compress(chunk))
                        with kev("crc"):
                            running_crc = crc32(chunk, running_crc)
                        start += len(chunk)

                    with kev("flush/write"):
                        buf.write(compobj.flush())
                    del compobj  # to make sure we don't accidentally reuse

                    with kev("leave"):
                        self._multi_freelist.leave(obj)
                    done = True
                    # this is done by exit_stack now
                    # file_object.fo.close()
                    cond.notify_all()
                return (buf.getvalue(), size, running_crc)

            def spacer() -> Tuple[bytes, int, Optional[int]]:
                with kev("spacer"):
                    with cond:
                        while not done:
                            cond.wait()
                return (b"", 0, None)

            # This consumes all the slots, the one that does the multithreaded
            # work is first.  This tends to schedule _more_ work than we have
            # cores; if it came last we would schedule _less_
            return [pool.submit(func)] + [
                pool.submit(spacer) for _ in range(self._threads - 1)
            ]

    def _decompress_for_testing(self, data: bytes) -> bytes:
        return zstandard.ZstdDecompressor().decompress(data)
