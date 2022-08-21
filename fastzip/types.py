import struct
from dataclasses import dataclass
from typing import BinaryIO, List, Optional, Sequence, Tuple


def _readn(fo: BinaryIO, n: int) -> bytes:
    data = fo.read(n)
    if len(data) != n:
        raise ValueError(f"Short read: wanted {n} but got {len(data)}")
    return data


def _slicen(buf: bytes, off: int, n: int) -> bytes:
    data = buf[off : off + n]
    if len(data) != n:
        raise ValueError(f"Short read: wanted {n} but got {len(data)}")
    return data


class EndOfLocalFiles(Exception):
    pass


FLAG_DATA_DESCRIPTOR = 1 << 3
FLAG_FILENAME_UTF8 = 1 << 11

LOCAL_FILE_HEADER_SIGNATURE = 0x04034B50
LOCAL_FILE_HEADER_FORMAT = "<LHHHHHLLLHH"


@dataclass
class LocalFileHeader:
    signature: int
    version_needed: int
    flags: int
    method: int
    mtime: int
    mdate: int
    crc32: int
    csize: int
    usize: int
    filename_length: int
    extra_length: int

    filename: Optional[str] = None
    parsed_extra: Sequence[Tuple[int, bytes]] = ()

    @classmethod
    def read_from(cls, fo: BinaryIO) -> Tuple["LocalFileHeader", bytes]:
        """
        Assuming fo is ready to read a valid local file header, reads the object
        and returns `(object, buffer)` while leaving the position ready to read
        (or seek past) the file data of length `self.csize`.
        """
        buf = _readn(fo, struct.calcsize(LOCAL_FILE_HEADER_FORMAT))
        args = struct.unpack(
            LOCAL_FILE_HEADER_FORMAT,
            buf,
        )
        inst = cls(*args)

        if inst.signature == CENTRAL_DIRECTORY_SIGNATURE:
            raise EndOfLocalFiles()
        if inst.signature != LOCAL_FILE_HEADER_SIGNATURE:
            raise ValueError("Invalid signature %0x" % (inst.signature,))

        filename_data = _readn(fo, inst.filename_length)
        buf += filename_data

        if inst.flags & FLAG_FILENAME_UTF8:
            inst.filename = filename_data.decode("utf-8")  # can raise
        else:
            inst.filename = filename_data.decode("cp437")

        # print("Filename", inst.filename)

        if inst.flags & FLAG_DATA_DESCRIPTOR:
            raise NotImplementedError("Data descriptor")

        if inst.extra_length:
            extra: List[Tuple[int, bytes]] = []
            extra_data = _readn(fo, inst.extra_length)
            # print(" ".join("%02x" % c for c in extra_data))

            i = 0
            while i < len(extra_data) - 4:
                extra_id, data_size = struct.unpack(
                    "<HH",
                    _slicen(extra_data, i, 4),
                )
                # print("Extra", i, extra_id, data_size)
                i += 4
                data = _slicen(extra_data, i, data_size)
                i += data_size
                extra.append((extra_id, data))

                if extra_id == 1:  # zip64 entry
                    sizes = [
                        int.from_bytes(data[n : n + 8], "little")
                        for n in range(0, len(data), 8)
                    ]
                    if inst.usize == 0xFFFFFFFF:
                        inst.usize = sizes.pop(0)
                    if inst.csize == 0xFFFFFFFF:
                        inst.csize = sizes.pop(0)
                    # "header offset" and "disk" should not exist in the local
                    # header copy
                    if len(sizes) != 0:
                        raise ValueError("Extra zip64 extra in LFH")
            if i != len(extra_data):
                raise ValueError("Extra length")
            inst.parsed_extra = tuple(extra)
            buf += extra_data

        return inst, buf


CENTRAL_DIRECTORY_SIGNATURE = 0x02014B50
