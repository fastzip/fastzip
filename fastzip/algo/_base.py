from __future__ import annotations

from concurrent.futures import Executor, Future

from typing import Dict, Optional, Sequence, Tuple, Union


class BaseCompressor:
    number: int
    short_name: str
    version_needed: int  # docs give x.y but this is x * 10 + y

    _threads: int

    def __init__(self, threads: int, params: str = "") -> None:
        """
        Subclasses are expected to parse the params string for level, window size, etc.
        """
        self._threads = threads

    def compress_to_futures(
        self,
        pool: Executor,
        size: int,
        mmap_future: Union[memoryview, Future[memoryview]],
    ) -> Sequence[Future[Tuple[bytes, int, Optional[int]]]]:
        """
        Compress the given data, presumably in parallel.

        This method MUST be threadsafe, and the futures MUST be assumed to run
        concurrently as well.  Use a mutex here or in whatever the returned
        future runs to ensure this.

        The returned futures are (compressed_chunk, raw_length,
        [raw_chunk_crc32]).  The consumer of these needs to merge crc32s but can
        just concatenate compressed_chunk.
        """
        raise NotImplementedError

    def _decompress_for_testing(self, data: bytes) -> bytes:
        """
        Only intended for testing, this buffers the entire input and output.
        """
        raise NotImplementedError


def parse_params(params: str) -> Dict[str, int]:
    """
    Parses a dict of `,` and `=` separated parameters.
    """
    d: Dict[str, int] = {}
    if not params:
        return d

    for p in params.split(","):
        k, _, v = p.partition("=")
        if v:
            vi = int(v)
        else:
            vi = 1
        d[k] = vi
    return d
