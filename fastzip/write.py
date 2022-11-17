from __future__ import annotations

import io
import logging
import os
import threading
import time
import traceback
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import replace
from pathlib import Path
from queue import SimpleQueue
from typing import Any, BinaryIO, Dict, IO, List, Optional, Tuple, Union

from keke import get_tracer, kcount, kev

from ._crc32_combine import crc32_combine

from .algo import find_compressor_cls
from .algo._base import BaseCompressor
from .algo._queue import QueueItem
from .algo._wrapfile import WrappedFile
from .chooser import CompressionChooser, DEFAULT_CHOOSER

from .types import (
    CentralDirectoryHeader,
    EOCD,
    EOCD_SIGNATURE,
    LocalFileHeader,
    ZIP64_EOCD_LOCATOR_SIGNATURE,
    ZIP64_EOCD_SIGNATURE,
    Zip64EOCD,
    Zip64EOCDLocator,
)

MAX_UINT16 = 0xFFFF
MAX_UINT32 = 0xFFFF_FFFF

DEFAULT_IO_THREADS = 4
DEFAULT_FILE_BUDGET = 200

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
        io_threads: Optional[int] = None,
        file_budget: Optional[int] = None,
        force_zip64: bool = False,
    ):
        self._filename = filename
        if fobj is not None:
            self._fobj = fobj
            self._fobj_provided = True
        else:
            self._fobj = open(
                filename,
                "xb",
                buffering=1024 * 1024,  # XXX Default is ~8KiB
            )  # Do not allow overwriting
            self._fobj_provided = False
        self._threads: int = threads if threads is not None else os.cpu_count()  # type: ignore
        _file_budget: int = (
            file_budget if file_budget is not None else DEFAULT_FILE_BUDGET
        )

        self._file_budget = threading.BoundedSemaphore(_file_budget)

        self._open_queue: SimpleQueue[
            Union[QueueItem, Future[Tuple[LocalFileHeader, WrappedFile]], _Sentinel]
        ] = SimpleQueue()
        self._queue: SimpleQueue[Union[QueueItem, _Sentinel]] = SimpleQueue()
        self._executor = (
            executor
            if executor is not None
            else ThreadPoolExecutor(
                max_workers=self._threads, thread_name_prefix="Compress"
            )
        )
        _io_threads = io_threads if io_threads is not None else DEFAULT_IO_THREADS
        self._io_executor = ThreadPoolExecutor(
            max_workers=_io_threads, thread_name_prefix="IO"
        )

        self._chooser = chooser
        self._cache: Dict[str, BaseCompressor] = {}

        self._bytes_written = 0
        if prefix_data:
            self._fobj.write(prefix_data)
            self._bytes_written += len(prefix_data)

        self.comment = comment

        self._central_directory = []
        self._central_directory_min_ver = 0
        self._open_consumer_thread = threading.Thread(target=self._open_consumer)
        self._open_consumer_thread.start()
        self._consumer_thread = threading.Thread(target=self._consumer)
        self._consumer_thread.start()
        self._done = False
        self._stats_thread: Optional[threading.Thread] = None
        self._force_zip64 = force_zip64
        if get_tracer():
            # This uses about 10% of a core, so don't enable unless we're
            # actually tracing.
            self._stats_thread = threading.Thread(target=self._stats)
            self._stats_thread.start()

    def __enter__(self) -> "WZip":
        return self

    def __exit__(self, *args: Any) -> None:
        # TODO check for exception in consumer
        with kev("_shutdown", __name__):
            self._shutdown()
        with kev("_open_consumer_thread.join", __name__):
            self._open_consumer_thread.join()
        with kev("_consumer_thread.join", __name__):
            self._consumer_thread.join()
        with kev("flush"):
            self._fobj.flush()
        with kev("io_executor.shutdown"):
            self._io_executor.shutdown()
        with kev("_write_central_dir"):
            self._write_central_dir()
        if not self._fobj_provided:
            with kev("close"):
                self._fobj.close()
        self._done = True
        if self._stats_thread:
            self._stats_thread.join()

    def _stats(self) -> None:
        prev_ts = None
        prev_process_time = None
        prev_bytes_written = None
        while not self._done:
            kcount("open_queue", self._open_queue.qsize())
            kcount("queue", self._queue.qsize())
            kcount("futures", self._executor._work_queue.qsize())
            kcount("io_futures", self._io_executor._work_queue.qsize())
            kcount("file_budget", self._file_budget._value)

            ts = time.time()
            process_time = time.process_time()
            bytes_written = self._bytes_written
            if prev_ts is not None:
                kcount(
                    "proc_cpu_pct",
                    100 * (process_time - prev_process_time) / (ts - prev_ts),
                )
                kcount(
                    "kB_written_per_sec",
                    (bytes_written - prev_bytes_written) / (ts - prev_ts) / 1000,
                )
            prev_ts = ts
            prev_process_time = process_time
            prev_bytes_written = bytes_written

            # TODO linux-only
            kcount("fds", len(os.listdir("/proc/self/fd")))

            time.sleep(0.01)

    def _shutdown(self) -> None:
        self._open_queue.put(SHUTDOWN_SENTINEL)

    def rwrite(self, local_path: Path) -> None:
        """
        Whereas write() only takes files, this can take directories.

        Slightly slower because it has to stat up front, but you also get
        reasonable exceptions in the main thread.
        """
        if local_path.is_dir():
            for p in local_path.iterdir():
                self.rwrite(p)
        else:
            assert os.access(local_path, os.R_OK)
            self.write(local_path)

    def _write_open(
        self,
        fobj: Optional[BinaryIO],
        local_path: Path,
        archive_path: Path,
    ) -> Tuple[LocalFileHeader, WrappedFile]:
        f: BinaryIO
        with kev("open", __name__, path=local_path.as_posix()):
            if fobj is None:
                f = open(local_path, "rb")
            else:
                f = fobj

            wf = WrappedFile(f)
        with kev("lfh"):
            lfh = LocalFileHeader.from_wrapped_file(
                (archive_path if archive_path is not None else local_path),
                wf,
            )
        with kev("mmapwrapper"):
            wf.mmapwrapper()

        # Every compression method will need to call mmapwrapper, do this
        # eagerly so it can be handled in parallel while this QueueItem
        # waits in the queue.
        # self._io_executor.submit(wf.mmapwrapper)
        return (lfh, wf)

    def write(
        self,
        local_path: Path,
        archive_path: Optional[Path] = None,
        synthetic_mtime: Optional[int] = None,
        fobj: Optional[BinaryIO] = None,
    ) -> None:
        with kev("acquire", "file_budget"):
            # Acquisition needs to be in order
            self._file_budget.acquire()

        with kev("fut", "open_queue"):
            # TODO figure out how to display this as an idle stall
            fut = self._io_executor.submit(
                self._write_open,
                fobj,
                local_path,
                # TODO this line is duplicated twice, and is a little questionable to begin with.
                (archive_path if archive_path is not None else local_path),
            )
        with kev("put", "open_queue"):
            self._open_queue.put(fut)

    def enqueue_precompressed(
        self,
        lfh: LocalFileHeader,
        extra_bytes: Union[memoryview, bytes],
        compressed_bytes: Union[memoryview, bytes],
    ) -> None:
        # TODO: note the extra_bytes are not currently used; we make the lfh
        # reconstruct them (unnecessarily), as well as funnel this through the
        # open_queue (to ensure it remains properly ordered).
        self._open_queue.put(
            QueueItem(
                lfh,
                [self._executor.submit(identity, compressed_bytes)],
                None,
            )
        )

    def enqueue(self, partial_lfh: LocalFileHeader, file_object: WrappedFile) -> None:
        assert binary_io_check(file_object), f"{file_object} is not binary"
        LOG.debug("Enqueue %s w/ %s", partial_lfh.filename, repr(file_object))

        with kev("get compressor", __name__):
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

        with kev("compress_to_futures", __name__, algo=repr(obj)):
            data_futures = obj.compress_to_futures(
                self._executor,
                file_object.stat().st_size,
                file_object.mmapwrapper()[1],
            )

        # TODO figure out how to display this as idle
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
            self._bytes_written += len(data)
            pos += len(data)

        central_directory_size = pos - first_pos
        num_entries = len(self._central_directory)

        if (
            num_entries > MAX_UINT16
            or central_directory_size > MAX_UINT32
            or first_pos >= MAX_UINT32
            or self._force_zip64
        ):
            e64_pos = pos
            e64 = Zip64EOCD(
                ZIP64_EOCD_SIGNATURE,
                0,  # Will get replaced
                65,  # arbitrarily, the version that supports zstd
                self._central_directory_min_ver,
                0,
                0,
                len(self._central_directory),
                len(self._central_directory),
                central_directory_size,
                first_pos,
            )
            self._fobj.write(e64.dump())
            l64 = Zip64EOCDLocator(
                ZIP64_EOCD_LOCATOR_SIGNATURE,
                0,
                e64_pos,
                1,
            )
            data = l64.dump()
            self._fobj.write(data)
            self._bytes_written += len(data)

            if num_entries > MAX_UINT16:
                num_entries = MAX_UINT16
            if central_directory_size > MAX_UINT32:
                central_directory_size = MAX_UINT32
            if first_pos > MAX_UINT32:
                first_pos = MAX_UINT32

        e = EOCD(
            EOCD_SIGNATURE,
            0,
            0,
            num_entries,
            num_entries,
            central_directory_size,
            first_pos,
            0,  # Will get replaced
            self.comment or "",
        )
        data = e.dump()
        self._fobj.write(data)
        self._bytes_written += len(data)

    def _consumer(self) -> None:
        while True:
            item = self._queue.get()
            if item is SHUTDOWN_SENTINEL:
                LOG.debug("Shutdown consumer")
                return

            assert isinstance(item, QueueItem)
            assert len(item.compressed_data_futures) >= 1  # no generators

            if len(item.compressed_data_futures) == 1:
                with kev("_consumer_single"):
                    self._consumer_single(item)
            else:
                with kev("_consumer_many"):
                    self._consumer_many(item)
            # self._queue.task_done()

    def _exit_stack(self, exit_stack: Optional[ExitStack]) -> None:
        with kev("exit_stack"):
            if exit_stack:
                exit_stack.close()
            self._file_budget.release()

    def _consumer_single(self, item: QueueItem) -> None:
        try:
            (future_data, future_size, future_crc) = item.compressed_data_futures[
                0
            ].result()
        except Exception as e:
            self._exit_stack(item.exit_stack)
            self._exc = e
            traceback.print_exc()
            return

        with kev("lfh.replace"):
            lfh = replace(item.partial_lfh, csize=len(future_data))
            if future_crc is not None:
                lfh = replace(lfh, crc32=future_crc)

        with kev("tell"):
            pos = self._fobj.tell()

        with kev("lfh.dump"):
            self._central_directory.append((pos, lfh))
            new_lfh, min_ver = lfh.dump()
            self._central_directory_min_ver = max(
                self._central_directory_min_ver, min_ver
            )
        with kev("write", size=len(new_lfh) + len(future_data)):
            self._fobj.write(new_lfh)
            self._bytes_written += len(new_lfh)
            self._fobj.write(future_data)
            self._bytes_written += len(future_data)

        # XXX If there are any refereces lying around, the mmap.close() will
        # raise, and right now nothing will ever see that exception.
        item.compressed_data_futures = ()
        del future_data
        self._io_executor.submit(self._exit_stack, item.exit_stack)

    def _consumer_many(self, item: QueueItem) -> None:
        t0 = time.time()
        pos = self._fobj.tell()
        running_crc = None
        running_size = 0
        written_lfh, min_ver = item.partial_lfh.dump()
        self._central_directory_min_ver = max(self._central_directory_min_ver, min_ver)
        self._fobj.write(written_lfh)

        for f in item.compressed_data_futures:
            # TODO this exception-setting thing doesn't appear to work, and
            # also loses the most relevant stack
            try:
                (future_data, future_size, future_crc) = f.result()
            except Exception as e:
                self._exit_stack(item.exit_stack)
                self._exc = e
                traceback.print_exc()
                return

            with kev("write", size=len(future_data) if future_data is not None else 0):
                if future_data:
                    self._fobj.write(future_data)
                    self._bytes_written += len(future_data)
                    running_size += len(future_data)
            with kev("crc32_combine"):
                if future_crc is not None:
                    if running_crc is None:
                        running_crc = future_crc
                    else:
                        running_crc = crc32_combine(
                            running_crc, future_crc, future_size
                        )

        # XXX If there are any refereces lying around, the mmap.close() will
        # raise, and right now nothing will ever see that exception.
        item.compressed_data_futures = ()
        del future_data
        self._io_executor.submit(self._exit_stack, item.exit_stack)

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
        self._central_directory_min_ver = max(self._central_directory_min_ver, min_ver)
        if len(new_lfh) != len(written_lfh):
            raise ValueError("lfh changed size")
        t = self._fobj.tell()
        self._fobj.seek(pos)
        self._fobj.write(new_lfh)
        # TODO the seek is probably more costly than the write here :/
        self._bytes_written += len(new_lfh)
        self._fobj.seek(t)
        t2 = time.time()
        LOG.info(
            "Done writing %s ratio=%.1f%% compwait=%.1fs write=%.1fs",
            lfh.filename,
            lfh.csize / lfh.usize * 100 if lfh.usize != 0 else 100 * lfh.csize,
            t1 - t0,
            t2 - t1,
        )

    def _open_consumer(self) -> None:
        while True:
            item = self._open_queue.get()
            if isinstance(item, _Sentinel):
                self._queue.put(SHUTDOWN_SENTINEL)
                break
            elif isinstance(item, QueueItem):
                # precompressed data
                self._queue.put(item)
            else:
                with kev(".result", "open_consumer"):
                    try:
                        partial_lfh, wf = item.result()
                    except Exception as e:
                        self._exc = e
                        traceback.print_exc()
                        continue

                self.enqueue(partial_lfh, wf)


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
