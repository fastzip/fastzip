# Invalid .ipa analysis

TL;DR `ditto` produces invalid zip files (it should use zip64, but the values
just get truncated to a 32-bit int).

## The Setup

I was provided a sample of a `.ipa` file (iOS app build, but fundamentally a
zip file) produced by xcode by a trusted third-party that both Info-Zip's
`unzip -t` and Python's `python -m zipfile -t` said was corrupt.

## Repro

```
dd if=/dev/random of=big.bin bs=1m count=4097
ditto -k -c big.bin big.zip
unzip -t big.zip
```

Here's the structures as-read:

```
EOCD(..., size=65, offset_start=2359105, comment_length=0, comment='')
Calculated actual CD offset_start 4297326401
0: CentralDirectoryHeader(
    ...,
    version_made_by=789, version_needed=20, flags=8, method=8, ...,
    csize=2359036, usize=1048576, filename_length=7, extra_length=12,
    comment_length=0, disk_start=0, internal_attributes=0,
    external_attributes=2175025152, relative_offset_of_lfh=0, filename=b'big.bin',
    parsed_extra=(
        (22613, b'\xf0\x9d\x06h\xef\x9d\x06h'),
    ), file_comment=None)
```

1. Notably, the EOCD's `offset_start` is incorrect (it's the right value mod
   2^32), and the single file's `csize` and `usize` (the compressed and
   uncompressed sizes) are similarly mod-2^32.

2. For larger zip files the `relative_offset_of_lfh` can also be incorrect, and
   due to some compatibility code in Python's `zipfile`, *is* incorrect once
   parsed.

3. The same issue exists in the `csize` and `usize` of the local file header, but
   some tools (Archive Utility, and WinRAR at least) read until the end of the
   deflate stream and don't validate the resulting size :/

4. Not demonstrated above, more than 65535 files causes the truncated count to
   be written to the EOCD.  All of the directory entries get written, but with
   the wrong count, most decompressors won't see them.

## Storytime

What would a correctly-zipped version of this look like?

Where's what Info-Zip `zip -o big2.zip big.bin` gives:

```
Zip64EOCD(..., size=44, version_made_by=798, ..., offset_start=4296712046)
0: CentralDirectoryHeader(
    ...,
    version_made_by=798, version_needed=45, flags=0, method=8, ...,
    csize=4296711961, usize=4296015872, filename_length=7, extra_length=44,
    comment_length=0, disk_start=0, internal_attributes=0,
    external_attributes=2175008768, relative_offset_of_lfh=0, filename=b'big.bin',
    parsed_extra=(
        (21589, b'\x03\xef\x9d\x06h'),
        (30837, b'\x01\x04\xf5\x01\x00\x00\x04\x14\x00\x00\x00'),
        (1, b'\x00\x00\x10\x00\x01\x00\x00\x00\x19\x9f\x1a\x00\x01\x00\x00\x00')
    ), file_comment=None)
```

Notably, there's a zip64 EOCD as well as the zip64 extra on the central
directory entry itself (the extra number 1).  That extra is the only way to
store sizes >= 4GB (and if you're extremely paranoid about decompressors mixing
up signed and unsigned, >= 2GB).

This is also a more typical zip in that it doesn't use "data descriptors"
(flags=8).  I haven't seen those in a long time (but I guess I don't see a lot
of Mac-created zips, because they're unnecessarily used by all the Apple
tools I tried when writing this up).

## But what about "compatibility code"

First, for the sake of clarity, "relative offsets" are actually relative to the
start of that particular zip, and the terminology dates from when zips might be
spanned across multiple floppies (a feature rarely used today).  It is *not* a
signed number, and you can pretty much read it as *absolute* in modern
implementations.

Because a zip's directory is at the end of a file, it can start with anything.
This is how self-extracting archives as well as `.par` files work.

It is possible to construct a fully-valid zip with arbitrary starting data by
adding its length to all the "relative offsets" contained within.  However,
it's also possible to just concatenate a `.sh` with a `.zip` and people expect
that to work.

If we assume no gaps between the files, central directory, and EOCD there is
actually a duplicate piece of information -- the `size` of the central
directory can be treated as the number of bytes before the EOCD where the
central directory can be found.  Using this method already works around issue #1.

The difference between `offset_start` and `(eocd start) - size (of cd)` is
considered `concat` -- the number of bytes that were inserted at the beginning,
and from that point on all offsets get modified by that amount during reading.

Source ref: [zipfile line 1486](https://github.com/python/cpython/blob/3b4b56f46dbfc0c336a1f70704f127593ec1f4ce/Lib/zipfile/__init__.py#L1486-L1490)


We can write code to correct the file offsets after the central directory is
read, by simply reversing that logic.

```py
def fix_file_header_offsets(zf: zipfile.ZipFile) -> None:
    inverse_concat = None
    offset = 0
    infos = zf.infolist()  # We will modify these objects in place
    for info in infos:
        if inverse_concat is None:
            inverse_concat = info.header_offset

        info.header_offset -= inverse_concat
        while info.header_offset < offset:
            info.header_offset += 0x1_0000_0000
        offset = info.header_offset
        info._end_offset = info.header_offset + info.compress_size + 1024
```

The `_end_offset` is needed for making zip-bomb detection not raise, and 1024 is
longer than most headers are ever going to be.  That piece is a little hacky,
but far simpler than the multiple iterations (and special-casing the last file)
necessary to set the precisely correct value (see below).

We also do this operation in the natural list order (rather than sorting by
`header_offset` as `zipfile` does when writing `_end_offset` in the first
place), under the assumption that local files are in the same order as their
headers, because we don't have anything better to rely on to find the order.

For members less than 4GB in size, this is probably sufficient to extract them
by working around issue #2 very efficiently.

## But what about big archive members

Working around issue #3 is a bit trickier, slower, and more inexact but works
in practice.  Here's the version that handles that as well as properly
adjusting `_end_offset` exactly.

```py
def fix_file_header_offsets(zf: zipfile.ZipFile, path: os.PathLike[str]) -> None:
    with open(path) as fh:
        inverse_concat = None
        offset = 0
        infos = zf.infolist()
        for info in infos:
            if inverse_concat is None:
                inverse_concat = info.header_offset

            info.header_offset -= inverse_concat
            while info.header_offset < offset:
                info.header_offset += 0x1_0000_0000
            for addtl in range(3): # 0, 4GB, 8GB
                try:
                    zf.open(info)
                    break
                except zipfile.BadZipFile: # Bad magic number for file header
                    info.header_offset += 0x1_0000_0000
            else:
                raise zipfile.BadZipFile("Could not correct for bad offset")

        # At this point, `zf.start_dir` is correct and the various
        # `info.header_offset` are correct, but we need to fix `_end_offset` as
        # well as two sizes using different methods.
        end = zf.start_dir
        for info in reversed(infos):
            info._end_offset = end
            while info.header_offset + info.compress_size + 0x1_0000_0000 <= end:
                info.compress_size += 0x1_0000_0000

            if info.compress_type == zipfile.ZIP_STORED:
                info.file_size = info.compress_size
            else:
                # Because uncompressed size is likely the first to overflow (with
                # deflate), we have to figure out its length regardless of whether
                # the other conditions matched.
                size = 0
                with zf.open(info) as f:
                    # Set this to a silly number, otherwise it stops reading early;
                    # we throw away this ZipExtFile object and don't need to fix
                    # this copy to the real value here.
                    f._left = 0x1_0000_0000_0000_0000

                    # This loop also validates CRC, and terminates when the deflate
                    # end-of-stream is set.
                    while t := f.read(1024):
                        size += len(t)

                info.file_size = size

            end = info.header_offset
```

## But what about #4

```
mkdir x
(cd x; seq 65537 | xargs touch)
ditto -k -c x x.zip
```

This one is also readily reproducible, but requires more invasive workarounds
in `zipfile` because this is part of the initial info reading, and the EOCD
items are not kept around.

The easiest workaround is to duplicate stdlib's `zipfile` as `zipfile2` or
something, and modify the following loop:

```
while total < size_cd:
  centdir = fp.read(sizeCentralDir)
  if len(centdir) != sizeCentralDir:
  ...
```

to be more like:

```
while True:
  centdir = fp.read(sizeCentralDir)
  if not centdir.startswith(b"PK\x01\x02"):
    break
  if len(centdir) != sizeCentralDir:
  ...
```
Incidentally, zip64 due to file count is
the first time I ran up against decompressors not supporting zip64, which was
in Python's *other* implementation, `zipimport`.  That's now fixed in 3.13 with
my [PR 94146](https://github.com/python/cpython/pull/94146) was merged.  It
uses the same method and raises a good error when the detected count doesn't
match the header count.

## Conclusion

Now that I know the root cause and have better words to search for, basically
this same bug has been known in the Archive Utility and `ditto` since 
"quite some time" before Dec 2009.

It appears to be fixed in Archive Utility, and Finder's "right-click, Compress"
in Sequoia 15.3.2, but still exists in `ditto` with the current version of
developer tools.

* https://sourceforge.net/p/sevenzip/bugs/2038/ (2017)
  * https://sourceforge.net/p/sevenzip/bugs/1474/ (2015)
  * https://sourceforge.net/p/sevenzip/bugs/1170/ (2010)
    * https://web.archive.org/web/20140331005235/http://www.springyarchiver.com/blog/topic/topic/203 (2009)
