from typing import IO


def _readn(fo: IO[bytes], n: int) -> bytes:
    data = fo.read(n)
    if len(data) != n:
        raise ValueError(f"Short read: wanted {n} but got {len(data)}")
    return data


def _slicen(buf: bytes, off: int, n: int) -> bytes:
    data = buf[off : off + n]
    if len(data) != n:
        raise ValueError(f"Short read: wanted {n} but got {len(data)}")
    return data
