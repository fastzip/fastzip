import operator
import re
from dataclasses import dataclass
from typing import Any, Callable, Optional, Sequence, Union

from .algo import find_compressor_cls
from .types import LocalFileHeader


def op_regex_match(pat: str, flags: int = 0) -> Callable[[str], bool]:
    """
    Operator function that returns a closure that matches strings.

    >>> op_regex_match(r"a{3,}").match("a")
    False
    >>> op_regex_match(r"a{3,}").match("aaa")
    True
    >>> op_regex_match(r"a{3,}").match("aaaaaaaa")
    True

    Note that fullmatch is used.

    >>> op_regex_match(r"a{3,}").match("aaab")
    False

    Flags are accepted, but not particularly useful except for `IGNORECASE`.
    """
    r = re.compile(pat, flags)

    def inner(subj: str) -> bool:
        return r.fullmatch(subj) is not None

    return inner


def op_fnmatch(pat: str) -> Callable[[str], bool]:
    """
    Operator function that returns a closure that matches filenames.

    >>> op_fnmatch("*.txt")("a.txt")
    True
    >>> op_fnmatch("*.txt")("a.bin")
    False

    Note that this uses the more modern glob syntax where `**` can refer to any
    directories.

    >>> op_fnmatch("*.txt")("a/b/c.txt")
    False
    >>> op_fnmatch("**/*.txt")("a/b/c.txt")
    True
    """
    import pywildcard

    return op_regex_match(pywildcard.translate(pat))


@dataclass
class Rule:
    lfh_attr: str
    # The first callable is intended for filenames, the second for everything
    # else (numeric)
    operator: Union[Callable[[str], bool], Callable[[Any, Any], bool]]
    rhs: Optional[int]
    algo_str: str


class CompressionChooser:
    """
    A way to choose the compression (type/params) based on file info.

    This class is only allowed to make decisions based on what is in the
    preliminary local file header, such as filename-as-stored or size.  It is
    explicitly not currently allowed to trial-compress data, as various
    compression experts have cautioned against that (e.g. debug sections of an
    executable probably still compress well, even if the first part doesn't).

    N.b. If the future we want to fall back to "store" after noticing a size
    increase for some complex compression method, it would be trivial to throw
    away the compressed data and use store, but that hack doesn't belong here,
    it belongs in whatever consumes the compression futures.
    """

    rules: Sequence[Rule]

    def __init__(self, default: str = "store", rules: Sequence[Rule] = ()) -> None:
        self.rules = list(rules)
        # TODO: property set that validates rules
        self.default_compressor = default
        self._validate_compressor_names()

    def _validate_compressor_names(self) -> None:
        # TODO: somewhere should validate the field names
        for rule in self.rules:
            find_compressor_cls(rule.algo_str)
        find_compressor_cls(self.default_compressor)

    def _choose_compressor(self, partial_lfh: LocalFileHeader) -> str:
        for rule in self.rules:
            if rule.rhs is None:
                if rule.operator(getattr(partial_lfh, rule.lfh_attr)):  # type: ignore
                    return rule.algo_str
            else:
                if rule.operator(getattr(partial_lfh, rule.lfh_attr), rule.rhs):  # type: ignore
                    return rule.algo_str
        return self.default_compressor


# This is a slightly-optimized demonstrator;
DEFAULT_CHOOSER = CompressionChooser(
    default="deflate@compresslevel=-1",
    rules=[
        # DEFLATE will grow small files -- the smallest stream for a single
        # repeating character is at least 11 bytes regardless of length.
        Rule("usize", operator.lt, 12, "store"),
        Rule("filename", op_regex_match(r"\.zip$"), None, "store"),
    ],
)
