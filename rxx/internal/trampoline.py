from collections import deque
from threading import Lock


class Trampoline(object):

    def __init__(self):
        self._idle = True
        self._queue = deque()
        self._lock = Lock()

    def idle(self) -> bool:
        with self._lock:
            return self._idle

    def run(self, item) -> None:
        with self._lock:
            self._queue.append(item)
            if self._idle:
                self._idle = False
            else:
                return
        try:
            self._run()
        finally:
            with self._lock:
                self._idle = True
                self._queue.clear()

    def _run(self) -> None:
        ready = None
        while True:
            with self._lock:
                ready = self._queue.copy()
                self._queue.clear()

            while len(ready) > 0:
                item = ready.popleft()
                item()

            with self._lock:
                if len(self._queue) == 0:
                    break
