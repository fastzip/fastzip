from threading import Lock

from typing import Callable, Generic, List, TypeVar

T = TypeVar("T")


class FactoryFreelist(Generic[T]):
    def __init__(self, factory_func: Callable[[], T]):
        self.factory_func = factory_func
        self.freelist: List[T] = []
        self.lock = Lock()

    def enter(self) -> T:
        with self.lock:
            if self.freelist:
                return self.freelist.pop(-1)
            else:
                return self.factory_func()

    def leave(self, obj: T) -> None:
        with self.lock:
            self.freelist.append(obj)
