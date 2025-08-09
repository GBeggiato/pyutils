"""
basic scheduler for full tasks (void -> void functions)
"""

# TODO: add persistent check for what has already been executed

from datetime import datetime
import functools
from pathlib import Path
import time
from typing import Callable, NamedTuple


SLEEP_SECONDS = 1
DATE_FMT = "%Y-%m-%d %H:%M:%S"

type SchedulableFunc = Callable[[None], None]


class _Island(NamedTuple):
    """
    basic representation of a scheduled task

    uniquely identified by its attributes
    """
    task: SchedulableFunc
    when: datetime

    def __str__(self) -> str:
        return f"task ({self.task.__name__}) scheduled for {self.when}"

    @functools.cached_property
    def msg(self) -> str:
        return f"{self.when.strftime(DATE_FMT)} | {self.task.__name__}"


class Oasis:

    def __init__(self):
        self._todos: list[_Island] = list()
        self._done = Path(__file__).with_name("oasis_done.txt")
        self._done.touch()

    def schedule(self, task: SchedulableFunc, when: datetime):
        self._todos.append(_Island(task, when))

    def write_done(self, island: _Island):
        with self._done.open("a") as fp:
            fp.writelines(island.msg)

    def already_done(self, island: _Island) -> bool:
        for line in self._done.open("r"):
            if line == island.msg:
                return True
        return False

    def mainloop(self):
        if not self._todos:
            return
        self._todos.sort(key=lambda x: x.when, reverse=True)
        while self._todos:
            time.sleep(SLEEP_SECONDS)
            ts = datetime.now()
            idxs_to_clear = sum(1 for e in self._todos if e.when <= ts)
            for _ in range(idxs_to_clear):
                island = self._todos.pop()
                if self.already_done(island):
                    print(f"{island}")
                else:
                    island.task()
                    self.write_done(island)
                
