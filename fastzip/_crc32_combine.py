from ctypes import c_ulong, CDLL
from ctypes.util import find_library

# TODO I suspect this needs to be different on Windows
_zlib_ctypes = CDLL(find_library("z"))
_zlib_ctypes.crc32_combine.argtypes = [c_ulong, c_ulong, c_ulong]
_zlib_ctypes.crc32_combine.restype = c_ulong


def crc32_combine(crc1: int, crc2: int, len2: int) -> int:
    """
    This function is a trivial wrapper around ctypes for the benefit of typing.

    More explanation at https://groups.google.com/g/comp.compression/c/SHyr5bp5rtc/m/PP5-pmv9-9sJ
    """
    return _zlib_ctypes.crc32_combine(crc1, crc2, len2)  # type: ignore
