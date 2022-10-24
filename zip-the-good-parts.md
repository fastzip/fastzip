# Zip: the good parts

Fastzip reads and writes a subset of the features in the zip "spec."  There is a
lot of ambiguity and this tries to pin down an interpretation that is generally
compatible.

## Archive Offset

The central directory contains some byte-offsets from the start of the file, but
most tools can read zips that have been appended to some other file despite this
not being in the spec.  The code to do this is not standardized, but we have a
helper that uses logic similar to CPython's zipfile.py to determine a plausible
start.

## Duplicated file headers

File headers (including names, sizes, etc) are written in both "local" and
"central" versions and the spec does not say what happens when they differ.  The
"central" version is generally considered authoritative by CPython but contains
less data.

## Multi-disk archives

These are not supported by CPython's zipfile.py or zipimport.py anyway.  The
same helper for 

Input: will be rejected

Output: will never happen

## Slashes

When reading existing zips, the intent is to pass them through unmodified, but
when creating new, only write forward slashes.  How to encode a filename that
contains a forward slash?  That's a very good question.

## Filenames

Do not contain a BOM, ever.  Do not contain trailing or leading spaces.  Do not
contain nulls, thus "modified UTF-8" is not necessary.

TODO: Normalization (especially as it pertains to the following section), and
whether UTF-8-MAC is still a thing.

## Duplicated names

An error will be printed and the first one kept, regardless of type (directory
vs file).

## Gaps

Don't.  (A future version may support alignment, but that will be done with a
padding extra.)

## Data descriptor

Don't.

## System made by

Always unix.

## Illegal names

Names such as `CON:` will be allowed.

## Timestamps

Always include higher-precision timestamps in one of the unix extras.
