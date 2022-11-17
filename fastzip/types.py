import os
import struct
import time
from dataclasses import dataclass
from pathlib import Path
from typing import IO, List, Optional, Sequence, Tuple, TYPE_CHECKING

from keke import kev

from .util import _readn, _slicen

if TYPE_CHECKING:
    from .algo._wrapfile import WrappedFile


class EndOfLocalFiles(Exception):
    pass


class BadLocalFileSignature(Exception):
    pass


UINT32_MAX = 0xFFFFFFFF
ZIP64_VERSION = 45

FLAG_DATA_DESCRIPTOR = 1 << 3
FLAG_FILENAME_UTF8 = 1 << 11

LOCAL_FILE_HEADER_SIGNATURE = 0x04034B50
LOCAL_FILE_HEADER_FORMAT = "<LHHHHHLLLHH"

WILL_BE_REPLACED_VALUE = 0xFF112233


@dataclass
class LocalFileHeader:
    # Section 4.3.7 of APPNOTE.TXT
    signature: int
    version_needed: int
    flags: int
    method: int
    mtime: int
    mdate: int
    crc32: int
    csize: int
    usize: int
    filename_length: int  # TODO this is unicode length!
    extra_length: int

    filename: Optional[str] = None
    parsed_extra: Sequence[Tuple[int, bytes]] = ()

    @classmethod
    def from_wrapped_file(
        cls,
        filename: Path,
        fo: "WrappedFile",
        synthetic_mtime: Optional[int] = None,
        synthetic_mode: Optional[int] = None,
    ) -> "LocalFileHeader":

        with kev("stat", __name__):
            stat = fo.stat()
        # mode = synthetic_mode if synthetic_mode is not None else stat.st_mode
        mtime = synthetic_mtime if synthetic_mtime is not None else stat.st_mtime
        # TODO this loses a little bit of precision, and someday we probably
        # want to store the higher-resolution unix extra as well.
        dt = time.localtime(mtime)
        # These two lines come verbatim from cpython's zipfile.py but are
        # likely the only way to do this numeric conversion.
        dosdate = (dt[0] - 1980) << 9 | dt[1] << 5 | dt[2]
        dostime = dt[3] << 11 | dt[4] << 5 | (dt[5] // 2)

        if filename.anchor:
            # N.b. platform-dependent behavior; this will strip drive letters
            # and UNC paths but only on Windows
            with kev("relative_to", __name__):
                filename = filename.relative_to(filename.anchor)

        with kev("as_posix", __name__):
            filename_str = filename.as_posix()  # '/' normalized value

        with kev("ret", __name__):
            return cls(
                signature=LOCAL_FILE_HEADER_SIGNATURE,
                version_needed=2,
                flags=0,  # (mode & 0o777) << 8,  # TODO assumes not a dir
                method=0,
                mtime=dostime,
                mdate=dosdate,
                crc32=WILL_BE_REPLACED_VALUE,
                csize=WILL_BE_REPLACED_VALUE,
                usize=stat.st_size,
                filename_length=len(filename_str),
                extra_length=0,  # TODO unix extra, zip64 extra
                filename=filename_str,
            )

    @classmethod
    def _for_testing(cls, usize: int, filename: str) -> "LocalFileHeader":
        return cls(
            signature=LOCAL_FILE_HEADER_SIGNATURE,
            version_needed=20,
            flags=0,
            method=0,
            mtime=0,
            mdate=0,
            crc32=0,
            csize=0,
            usize=usize,
            filename_length=len(filename),
            extra_length=0,
            filename=filename,
        )

    @classmethod
    def read_from(cls, fo: IO[bytes]) -> Tuple["LocalFileHeader", bytes]:
        """
        Assuming fo is ready to read a valid local file header, reads the object
        and returns `(object, buffer)` while leaving the position ready to read
        (or seek past) the file data of length `self.csize`.

        The `buffer` returned contains the raw bytes of the LFH, including the
        extra data if present.  This means that a tool intending to simply copy
        this file to another zip can do so without roundtripping this class's
        fields.

        If the signature appears to belong to a Central Directory (which would
        typically be immediately following the last Local File), the file
        position will be reset to where it was before the `read_from` call, and
        EndOfLocalFiles will be raised.

        If the signature mismatches for any other reason, the file position will
        be reset and BadLocalFileSignature will be raised.  This is an abnormal
        case and one that does not conform to zip-the-good-parts.
        """
        buf = _readn(fo, struct.calcsize(LOCAL_FILE_HEADER_FORMAT))
        args = struct.unpack(LOCAL_FILE_HEADER_FORMAT, buf)
        inst = cls(*args)
        assert inst.crc32 is not None

        if inst.signature == CENTRAL_DIRECTORY_SIGNATURE:
            fo.seek(-len(buf), os.SEEK_CUR)
            raise EndOfLocalFiles()
        if inst.signature != LOCAL_FILE_HEADER_SIGNATURE:
            raise ValueError("Invalid signature %0x" % (inst.signature,))

        filename_data = _readn(fo, inst.filename_length)
        buf += filename_data

        if inst.flags & FLAG_FILENAME_UTF8:
            inst.filename = filename_data.decode("utf-8")  # can raise
        else:
            inst.filename = filename_data.decode("cp437")

        if inst.flags & FLAG_DATA_DESCRIPTOR:
            # I am not a fan of the complexity and additional validation
            # required to support this flag; although Python's zipfile.py can
            # generate such files, I don't see the usefulness and would like to
            # guarantee that files output by this library will not contain them.
            raise NotImplementedError("Data descriptor")

        if inst.extra_length:
            extra: List[Tuple[int, bytes]] = []
            extra_data = _readn(fo, inst.extra_length)
            # print(" ".join("%02x" % c for c in extra_data))

            i = 0
            # The len() - 4 is to avoid `_slicen` needing to raise an exception
            # if there are 1-3 bytes left.  We raise that exception ourselves
            # directly below the loop to make it more clear that it's leftover
            # data at the _end_ rather than one that is completely malformed.
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
                    if inst.usize == UINT32_MAX:
                        inst.usize = sizes.pop(0)
                    if inst.csize == UINT32_MAX:
                        inst.csize = sizes.pop(0)
                    # We can validate here because section 4.5.3 is one of the
                    # few places that APPNOTE.TXT uses the modern word "MUST" --
                    # and both "disk" and "header offset" can't exist in the
                    # LFH.
                    if len(sizes) != 0:
                        raise ValueError("Extra zip64 extra in LFH")
            if i != len(extra_data):
                raise ValueError("Extra length")
            inst.parsed_extra = tuple(extra)
            buf += extra_data

        return inst, buf

    def replace_extra(self, num: int, value: bytes) -> None:
        n: List[Tuple[int, bytes]] = []
        for i, v in self.parsed_extra:
            if i != num:
                n.append((i, v))
        n.append((num, value))
        self.parsed_extra = n

    # TODO not happy with the name
    def dump(self) -> Tuple[bytes, int]:
        flags = self.flags
        assert self.filename is not None
        try:
            fn = self.filename.encode("ascii")
            # If utf-8 is already set, leave it?
        except UnicodeEncodeError:
            fn = self.filename.encode("utf-8")
            flags |= FLAG_FILENAME_UTF8

        usize = self.usize
        csize = self.csize
        min_ver = self.version_needed
        if self.usize >= UINT32_MAX or self.csize >= UINT32_MAX:
            zip64_extra = struct.pack("<QQ", self.usize, self.csize)
            usize = UINT32_MAX
            csize = UINT32_MAX
            self.replace_extra(1, zip64_extra)
            min_ver = max(self.version_needed, ZIP64_VERSION)
        extra = b"".join(
            struct.pack("<HH", i[0], len(i[1])) + i[1] for i in self.parsed_extra
        )
        extra_length = len(extra)
        return (
            struct.pack(
                LOCAL_FILE_HEADER_FORMAT,
                self.signature,
                min_ver,
                flags,
                self.method,
                self.mtime,
                self.mdate,
                self.crc32,
                csize,
                usize,
                len(fn),
                extra_length,
            )
            + fn
            + extra
        ), min_ver


CENTRAL_DIRECTORY_FORMAT = "<LHHHHHHLLLHHHHHLL"
CENTRAL_DIRECTORY_SIGNATURE = 0x02014B50


@dataclass
class CentralDirectoryHeader:
    # Section 4.3.12 of APPNOTE.TXT
    signature: int
    version_made_by: int  # Not in LFH
    version_needed: int
    flags: int
    method: int
    mtime: int
    mdate: int
    crc32: int
    csize: int
    usize: int
    filename_length: int  # TODO this is unicode length!
    extra_length: int
    comment_length: int  # Not in LFH

    disk_start: int  # Not in LFH
    internal_attributes: int
    external_attributes: int
    relative_offset_of_lfh: int

    filename: Optional[str] = None
    parsed_extra: Sequence[Tuple[int, bytes]] = ()
    file_comment: Optional[str] = None  # Not in LFH

    @classmethod
    def from_lfh_and_relative_offset(
        cls, lfh: LocalFileHeader, offset: int
    ) -> "CentralDirectoryHeader":
        return cls(
            signature=CENTRAL_DIRECTORY_SIGNATURE,
            version_made_by=0,  # TODO
            version_needed=lfh.version_needed,
            flags=lfh.flags,
            method=lfh.method,
            mtime=lfh.mtime,
            mdate=lfh.mdate,
            crc32=lfh.crc32,
            csize=lfh.csize,
            usize=lfh.usize,
            filename_length=lfh.filename_length,  # TODO verify?
            extra_length=lfh.extra_length,  # TODO
            comment_length=0,  # TODO be able to set?
            disk_start=0,  # We only want to support single-disk archives
            internal_attributes=0,  # TODO WUT
            external_attributes=0,
            relative_offset_of_lfh=offset,
            filename=lfh.filename,  # TODO ordering
        )

    # TODO not happy with the name
    def dump(self) -> bytes:
        flags = self.flags

        assert self.filename is not None
        try:
            fn = self.filename.encode("ascii")
            # If utf-8 is already set, leave it?
        except UnicodeEncodeError:
            fn = self.filename.encode("utf-8")
            flags |= FLAG_FILENAME_UTF8
        # TODO dump these too, they're important
        extra = b""
        comment = b""
        return (
            struct.pack(
                CENTRAL_DIRECTORY_FORMAT,
                self.signature,
                self.version_made_by,
                self.version_needed,
                flags,
                self.method,
                self.mtime,
                self.mdate,
                self.crc32,
                self.csize,
                self.usize,
                # TODO always recalculates filename length, I guess?
                len(fn),
                0,  # TODO extra_length
                0,  # TODO comment_length
                self.disk_start,
                self.internal_attributes,
                self.external_attributes,
                self.relative_offset_of_lfh,
            )
            + fn
            + extra
            + comment
        )


ZIP64_EOCD_FORMAT = "<LQHHLLQQQQ"
ZIP64_EOCD_SIGNATURE = 0x06064B50


@dataclass
class Zip64EOCD:
    signature: int
    size: int
    version_made_by: int
    version_needed: int
    disk_num: int
    disk_with_start: int
    num_entries_this_disk: int
    num_entries_total: int
    size_of_cd: int
    offset_start: int
    extensible_data: bytes = b""

    def dump(self) -> bytes:
        # Spec says to do this, whatev
        size = struct.calcsize(ZIP64_EOCD_FORMAT) + len(self.extensible_data) - 12
        return (
            struct.pack(
                ZIP64_EOCD_FORMAT,
                self.signature,
                size,
                self.version_made_by,
                self.version_needed,
                self.disk_num,
                self.disk_with_start,
                self.num_entries_this_disk,
                self.num_entries_total,
                self.size_of_cd,
                self.offset_start,
            )
            + self.extensible_data
        )


ZIP64_EOCD_LOCATOR_FORMAT = "<LLQL"
ZIP64_EOCD_LOCATOR_SIGNATURE = 0x07064B50


@dataclass
class Zip64EOCDLocator:
    signature: int
    disk_with_start: int
    relative_offset: int
    total_disks: int

    def dump(self) -> bytes:
        return struct.pack(
            ZIP64_EOCD_LOCATOR_FORMAT,
            self.signature,
            self.disk_with_start,
            self.relative_offset,
            self.total_disks,
        )


EOCD_FORMAT = "<LHHHHLLH"
EOCD_SIGNATURE = 0x06054B50


@dataclass
class EOCD:
    signature: int
    disk_num: int
    disk_with_start: int
    num_entries_this_disk: int
    num_entries_total: int
    size: int
    offset_start: int
    comment_length: int
    comment: str

    def dump(self) -> bytes:
        # TODO it's not clear how to have a utf-8 comment, and cp437 is not
        # super useful, so restrict to ascii for now.
        comment_bytes = self.comment.encode("ascii")
        return (
            struct.pack(
                EOCD_FORMAT,
                self.signature,
                self.disk_num,
                self.disk_with_start,
                self.num_entries_this_disk,
                self.num_entries_total,
                self.size,
                self.offset_start,
                len(comment_bytes),
            )
            + comment_bytes
        )
