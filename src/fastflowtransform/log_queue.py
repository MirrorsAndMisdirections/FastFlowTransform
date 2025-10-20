# fastflowtransform/log_queue.py
from __future__ import annotations

import threading
from dataclasses import dataclass
from queue import SimpleQueue


@dataclass(frozen=True)
class LogLine:
    """Single immutable log line with a monotonic sequence index."""

    idx: int
    text: str


class LogQueue:
    """Thread-safe line logger that preserves emission order across threads.

    Workers enqueue complete lines; the main thread drains and prints them
    after a level or at the end of the run to avoid interleaving output.
    """

    def __init__(self) -> None:
        self._q: SimpleQueue[LogLine] = SimpleQueue()
        self._seq = 0
        self._lock = threading.Lock()

    def put(self, text: str) -> None:
        """Enqueue a single log line in a thread-safe way."""
        with self._lock:
            idx = self._seq
            self._seq += 1
        self._q.put(LogLine(idx, text))

    def drain(self) -> list[str]:
        """Drain all pending lines (in stable order) and return them."""
        items: list[LogLine] = []
        while True:
            try:
                items.append(self._q.get_nowait())
            except Exception:
                break
        # SimpleQueue preserves FIFO, but we sort by idx for belt-and-suspenders stability
        items.sort(key=lambda x: x.idx)
        return [i.text for i in items]
