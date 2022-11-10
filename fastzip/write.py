import io
import logging
import os
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import replace
from pathlib import Path
from queue import Queue
from typing import Any, Dict, IO, List, Optional, Tuple, Union

from ._crc32_combine import crc32_combine

from .algo import find_compressor_cls
from .algo._base import BaseCompressor
from .algo._queue import QueueItem
from .algo._wrapfile import WrappedFile
from .chooser import CompressionChooser, DEFAULT_CHOOSER

from .types import CentralDirectoryHeader, EOCD, EOCD_SIGNATURE, LocalFileHeader

LOG = logging.getLogger(__name__)


class _Sentinel:
    pass


SHUTDOWN_SENTINEL = _Sentinel()


class WZip:
    _central_directory: List[Tuple[int, LocalFileHeader]]

    def __init__(
        self,
        filename: os.PathLike[str],
        fobj: Optional[IO[bytes]] = None,
        threads: Optional[int] = None,
        chooser: CompressionChooser = DEFAULT_CHOOSER,
        executor: Optional[ThreadPoolExecutor] = None,
        prefix_data: Optional[bytes] = None,
        comment: Optional[str] = None,
    ):
        self._filename = filename
        if fobj is not None:
            self._fobj = fobj
        else:
            self._fobj = open(filename, "xb")  # Do not allow overwriting
        self._threads: int = threads if threads is not None else os.cpu_count()  # type: ignore
        self._queue: Queue[Union[QueueItem, _Sentinel]] = Queue(self._threads)
        self._executor = (
            executor
            if executor is not None
            else ThreadPoolExecutor(max_workers=self._threads)
        )
        self._chooser = chooser
        self._cache: Dict[str, BaseCompressor] = {}

        if prefix_data:
            self._fobj.write(prefix_data)

        self.comment = comment

        self._central_directory = []
        self._central_directory_min_ver = 0
        self._consumer_thread = threading.Thread(target=self._consumer)
        self._consumer_thread.start()

    def __enter__(self) -> "WZip":
        return self

    def __exit__(self, *args: Any) -> None:
        # TODO check for exception in consumer
        self._shutdown()
        self._consumer_thread.join()
        self._write_central_dir()

    def _shutdown(self) -> None:
        self._queue.put(SHUTDOWN_SENTINEL)

    def write(
        self,
        local_path: Path,
        archive_path: Optional[Path] = None,
        synthetic_mtime: Optional[int] = None,
        fobj: Optional[IO[bytes]] = None,
    ) -> None:

        if fobj is None:
            fobj = open(local_path, "rb")
        wf = WrappedFile(fobj)

        partial_lfh = LocalFileHeader.from_wrapped_file(
            (archive_path if archive_path is not None else local_path), wf
        )
        self.enqueue(partial_lfh, wf)

    def enqueue_precompressed(
        self,
        lfh: LocalFileHeader,
        extra_bytes: Union[memoryview, bytes],
        compressed_bytes: Union[memoryview, bytes],
    ) -> None:
        # TODO: note the extra_bytes are not currently used; we make the lfh
        # reconstruct them (unnecessarily)
        self._queue.put(
            QueueItem(lfh, [self._executor.submit(identity, compressed_bytes)])
        )

    def enqueue(self, partial_lfh: LocalFileHeader, file_object: WrappedFile) -> None:
        assert binary_io_check(file_object), f"{file_object} is not binary"
        LOG.debug("Enqueue %s w/ %s", partial_lfh.filename, repr(file_object))

        compressor_name = self._chooser._choose_compressor(partial_lfh)
        if compressor_name in self._cache:
            obj = self._cache[compressor_name]
        else:
            cls, params = find_compressor_cls(compressor_name)
            obj = cls(self._threads, params)
            self._cache[compressor_name] = obj

        partial_lfh.method = obj.number
        partial_lfh.version_needed = max(obj.version_needed, partial_lfh.version_needed)
        exit_stack = ExitStack()
        exit_stack.enter_context(file_object)

        data_futures = obj.compress_to_futures(self._executor, file_object)
        self._queue.put(QueueItem(partial_lfh, data_futures, exit_stack))

    def _write_central_dir(self) -> None:
        LOG.info("Writing central directory")
        # Shouldn't be creating empty zips, this is a sanity check
        assert self._central_directory

        first_pos = self._fobj.tell()
        pos = first_pos
        for abs_offset, lfh in self._central_directory:
            # print("POS", pos, "ABS_OFFSET", abs_offset)
            cdh = CentralDirectoryHeader.from_lfh_and_relative_offset(
                lfh,
                abs_offset,
            )
            data = cdh.dump()
            self._fobj.write(data)
            pos += len(data)

        central_directory_size = pos - first_pos

        # TODO this will raise for zip64, I think
        e = EOCD(
            EOCD_SIGNATURE,
            0,
            0,
            len(self._central_directory),
            len(self._central_directory),
            central_directory_size,
            first_pos,
            0,  # Will get replaced
            self.comment or "",
        )
        self._fobj.write(e.dump())

    def _consumer(self) -> None:
        while True:
            item = self._queue.get()
            if item is SHUTDOWN_SENTINEL:
                LOG.debug("Shutdown consumer")
                return

            assert isinstance(item, QueueItem)

            t0 = time.time()
            pos = self._fobj.tell()
            running_crc = None
            running_size = 0
            written_lfh, min_ver = item.partial_lfh.dump()
            self._central_directory_min_ver = max(
                self._central_directory_min_ver, min_ver
            )
            self._fobj.write(written_lfh)

            for f in item.compressed_data_futures:
                # TODO this exception-setting thing doesn't appear to work, and
                # also loses the most relevant stack
                try:
                    (future_data, future_size, future_crc) = f.result()
                except Exception as e:
                    self._exc = e
                    traceback.print_exc()
                    return

                if future_data:
                    self._fobj.write(future_data)
                    running_size += len(future_data)
                if future_crc is not None:
                    if running_crc is None:
                        running_crc = future_crc
                    else:
                        running_crc = crc32_combine(
                            running_crc, future_crc, future_size
                        )

            if item.exit_stack:
                item.exit_stack.close()

            # assert buf
            lfh = replace(item.partial_lfh, csize=running_size)
            if running_crc is not None:
                lfh = replace(lfh, crc32=running_crc)
            t1 = time.time()

            # This doesn't know the _relative_ offset until we actually start
            # outputting the central directory, so just defer all choices
            # until then.
            self._central_directory.append((pos, lfh))
            new_lfh, min_ver = lfh.dump()
            self._central_directory_min_ver = max(
                self._central_directory_min_ver, min_ver
            )
            if len(new_lfh) != len(written_lfh):
                raise ValueError("lfh changed size")
            t = self._fobj.tell()
            self._fobj.seek(pos)
            self._fobj.write(new_lfh)
            self._fobj.seek(t)
            t2 = time.time()
            LOG.info(
                "Done writing %s ratio=%.1f%% compwait=%.1fs write=%.1fs",
                lfh.filename,
                lfh.csize / lfh.usize * 100 if lfh.usize != 0 else 100 * lfh.csize,
                t1 - t0,
                t2 - t1,
            )


def identity(
    x: Union[memoryview, bytes]
) -> Tuple[Union[memoryview, bytes], int, Optional[int]]:
    return (x, 0, None)


def binary_io_check(f: object) -> bool:
    if isinstance(f, WrappedFile):
        f = f.fo

    if hasattr(f, "mode"):
        return "b" in f.mode  # type: ignore
    elif isinstance(f, io.BytesIO):
        return True
    else:
        return False
