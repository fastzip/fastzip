import importlib
from typing import Tuple, Type

from ._base import BaseCompressor


def find_compressor_cls(d: str) -> Tuple[Type[BaseCompressor], str]:
    """
    Returns (compressor_class, param_str)

    You can refer to the provided algorithms ("store", "deflate", "zstd") by
    those strings, but any other must be provided as "qual.name:ClsName"
    format.

    Can raise ImportError, AttributeError, etc.
    """
    compname, _, params = d.partition("@")
    if ":" in compname:
        modname, _, compname = compname.partition(":")
    else:
        modname = f".{compname}"
        compname = f"{compname.capitalize()}Compressor"
    mod = importlib.import_module(modname, __package__)
    return getattr(mod, compname), params
