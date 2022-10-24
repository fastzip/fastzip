from concurrent.futures import Future
from contextlib import ExitStack

from dataclasses import dataclass
from typing import Iterable, Optional, Tuple

from ..types import LocalFileHeader


@dataclass
class QueueItem:
    partial_lfh: LocalFileHeader  # will set compression, crc, csize on it
    compressed_data_futures: Iterable[Future[Tuple[bytes, int, Optional[int]]]] = ()
    exit_stack: Optional[ExitStack] = None
