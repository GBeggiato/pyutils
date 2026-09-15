# many thanks to: https://www.youtube.com/watch?v=Rp9Ha0rVM1w&t=529s

import functools
from typing import Callable


type Composable[T] = Callable[[T], T]


def compose[T](*functions: Composable) -> Composable:
    def apply(value: T, fn: Composable[T]) -> T:
        return fn(value)
    return lambda x: functools.reduce(apply, functions, x)
    
