import logging
import os
import zlib
from pathlib import Path
from typing import Any, IO, List, Optional

import click
from keke import kev, TraceOutput

from fastzip.algo import find_compressor_cls
from fastzip.algo._base import BaseCompressor
from fastzip.chooser import CompressionChooser
from fastzip.read import RZipStream
from fastzip.write import DEFAULT_FILE_BUDGET, DEFAULT_IO_THREADS, WZip

log = logging.getLogger(__name__)

# TODO this should go in some central place, but also be able to register other
# (de)compressors like you can with a CompressionChooser rules.
def compressor_from_method(method: int) -> BaseCompressor:
    if method == 0:
        return find_compressor_cls("store")[0](1)
    elif method == 8:
        return find_compressor_cls("deflate")[0](1)
    else:
        raise ValueError(method)


def verify(filename: str) -> int:
    z = RZipStream(Path(filename))

    rc = 0
    for lfh, header_data, data in z.entries():
        obj = compressor_from_method(lfh.method)
        crc = zlib.crc32(obj._decompress_for_testing(data))
        if lfh.crc32 != crc:
            print("  %s: %08x != %08x (%d)" % (lfh.filename, crc, lfh.crc32, len(data)))
            rc |= 1
        else:
            print("  %s: ok" % (lfh.filename,))

    return rc


def extract(filename: str, target_dir: str) -> int:
    z = RZipStream(Path(filename))
    target_path = Path(target_dir)

    rc = 0
    for lfh, header_data, data in z.entries():
        obj = compressor_from_method(lfh.method)
        decompressed = obj._decompress_for_testing(data)
        crc = zlib.crc32(decompressed)
        if lfh.crc32 != crc:
            print("  %s: %08x != %08x (%d)" % (lfh.filename, crc, lfh.crc32, len(data)))
            rc |= 1

        assert lfh.filename is not None
        if lfh.filename.endswith("/"):
            (target_path / lfh.filename).mkdir(parents=True, exist_ok=True)
        else:
            (target_path / lfh.filename).parent.mkdir(parents=True, exist_ok=True)
        (target_path / lfh.filename).write_bytes(decompressed)

    return rc


def compress(
    filename: str, members: List[str], force_method: Optional[str] = None, **kwargs: Any
) -> int:
    rc = 0
    with WZip(Path(filename), **kwargs) as z:
        if force_method:
            z._chooser = CompressionChooser(default=force_method)

        for m in members:
            try:
                if m.startswith("+"):
                    # Merge in another zip
                    with kev("+merge"):
                        zi = RZipStream(Path(m[1:]))
                        for lfh, header_data, data in zi.entries():
                            z.enqueue_precompressed(lfh, b"", data)
                else:
                    z.rwrite(Path(m))

            except Exception as e:
                log.warning("Skipping %s (hopefully) because of %s", m, repr(e))

    return rc


@click.command()
@click.option(
    "--algo",
    help="Compression algorithm to use, e.g. `store` or `deflate@compresslevel=9`",
)
@click.option("--verbose", "-v", help="Verbose log level", count=True)
@click.option("--output", "-o", help="Output zip name", metavar="ZIP")
@click.option("--force", "-f", help="Overwrite output", is_flag=True)
@click.option("--dest", "-d", help="Dest dir", metavar="DIR")
@click.option(
    "--trace", help="Write trace-events to", metavar="FILE", type=click.File(mode="w")
)
@click.option(
    "--threads",
    help="Number of compression threads",
    default=os.cpu_count(),
    show_default=True,
)
@click.option(
    "--io-threads",
    help="Number of IO threads",
    default=DEFAULT_IO_THREADS,
    show_default=True,
)
@click.option(
    "--file-budget",
    help="Max number of files kept open, approximately",
    default=DEFAULT_FILE_BUDGET,
    show_default=True,
)
@click.option("-t", "verb", flag_value="test", help="Test archive 1st positional arg")
@click.option(
    "-e", "verb", flag_value="extract", help="Extract archive 1st positional arg"
)
@click.option("-c", "verb", flag_value="compress", help="Create archive named by -o")
@click.argument("files", nargs=-1)
def main(
    algo: Optional[str],
    verbose: int,
    output: Optional[str],
    dest: Optional[str],
    trace: "Optional[IO[str]]",
    threads: Optional[int],
    io_threads: Optional[int],
    file_budget: Optional[int],
    force: bool,
    verb: Optional[str],
    files: List[str],
) -> int:
    # TODO better format ala glog
    if verbose == 0:
        logging.basicConfig(level=logging.WARNING)
    elif verbose == 1:
        logging.basicConfig(level=logging.INFO)
    elif verbose >= 2:
        logging.basicConfig(level=logging.DEBUG)

    # Note that thread_sortkeys is kind of best-effort; trace viewers may not support it.
    with TraceOutput(
        file=trace, thread_sortkeys={"MainThread": -1, "Compress": 2, "IO": 3}
    ):
        if verb == "test":
            assert len(files) == 1
            with kev("test", __name__):
                return verify(files[0])
        elif verb == "extract":
            assert len(files) == 1
            assert dest is not None
            with kev("extract", __name__):
                return extract(files[0], dest)
        elif verb == "compress":
            assert output is not None

            if force:
                try:
                    Path(output).unlink()
                except Exception as e:
                    print(e)

            with kev("compress", __name__, algo=algo):
                return compress(
                    output,
                    files,
                    algo,
                    threads=threads,
                    io_threads=io_threads,
                    file_budget=file_budget,
                )
        else:
            raise NotImplementedError(f"Verb {verb}")


if __name__ == "__main__":
    main()
