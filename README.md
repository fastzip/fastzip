# fastzip

This project lets you efficiently create and combine zip files.


## What is "fast" in "faszip"?

Mainly two things:

1. Multithreaded compression (DEFLATE), similar to `pigz`
2. The ability to copy/merge zips without recompressing, similar to `zipmerge`

It's also pure python, able to stream the input (without needing the central
directory to be present), and features a rules-based compression method chooser,
to avoid _increasing_ the size of tiny files.


## Demo

```py
from pathlib import Path
from fastzip.write import WZip
with WZip(Path("out.zip")) as z:
    z.write(Path("a"))
```

If you'd like to customize the number of threads, choice of compression,
filename within the archive, etc those are all possible.

```py
from io import BytesIO
from pathlib import Path
from fastzip.write import WZip
from fastzip.chooser import CompressionChooser

force_store = CompressionChooser(default="store")
with WZip(Path("out.zip"), threads=2, chooser=force_store) as z:
    z.write(Path("a"), archive_path=Path("inzipname"), synthetic_mtime=0, fobj=BytesIO(b"foo"))
```

## Benchmark

On a 12-core machine, it's able to use almost 11 cores. The remainder appear to
be waiting for IO in the consumer thread.

```sh
$ dd if=/dev/urandom of=a bs=1M count=512
512+0 records in
512+0 records out
536870912 bytes (537 MB, 512 MiB) copied, 2.17395 s, 247 MB/s


$ /usr/bin/time zip -o a.zip a  # info-zip
  adding: a (deflated 0%)
12.21user 0.47system 0:12.71elapsed 99%CPU (0avgtext+0avgdata 2696maxresident)k
448inputs+1048752outputs (5major+209minor)pagefaults 0swaps

$ /usr/bin/time python -m zipfile -c a1.zip a  # zipfile
15.01user 0.63system 0:15.67elapsed 99%CPU (0avgtext+0avgdata 11516maxresident)k
1048inputs+1048904outputs (6major+1617minor)pagefaults 0swaps

$ /usr/bin/time python -m fastzip -c a2.zip a  # fastzip
23.43user 0.92system 0:02.24elapsed 1083%CPU (0avgtext+0avgdata 1089432maxresident)k
0inputs+1048912outputs (0major+150323minor)pagefaults 0swaps


$ ls -l a{,1,2}.zip
-rw-r--r-- 1 tim tim 537034782 Nov  6 07:54 a1.zip
-rw-r--r-- 1 tim tim 537037385 Nov  6 08:20 a2.zip
-rw-r--r-- 1 tim tim 536957698 Nov  6 07:50 a.zip
```


## Future plans (from drawbacks)

Right now, there are some potential drawbacks for using this instead of existing
libraries:

1. The API is a little more verbose than it needs to be, and some compression
   method require MMAP-able files.
2. There is no memory budget, and it can buffer about 2x the size of the largest
   file, in memory.  If you have a 1GB file, it can use 2GB of RAM.
3. Errors reported in threads are not always obvious

NOTE: The API is expected to change several times before version 1.0; if adding
a dep edge to this project (e.g. if you use a hypothetical version `0.4` make
sure you specify `>=0.4, <0.5`.


# License

fastzip is copyright [Tim Hatch](https://timhatch.com/), and licensed under
the MIT license.  I am providing code in this repository to you under an open
source license.  This is my personal repository; the license you receive to
my code is from me and not from my employer. See the `LICENSE` file for details.
