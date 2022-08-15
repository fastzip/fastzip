import queue
from types import LocalFileHeader


class FastzipOutput:
    _central_directory: Sequence[Tuple[int, LocalFileHeader]] = ()

    def __init__(
        self, filename: os.PathLike[str], prefix_data: Optional[bytes]
    ) -> None:
        self._filename = filename
        # TODO: don't overwrite, or at least warn
        self._fobj = open(filename, "wb")
        self._central_directory = []
        self._queue = queue.Queue(10)  # TODO less magic number

        self._consumer_thread = threading.thread(target=self._consumer)

    def _consumer(self):
        pass
