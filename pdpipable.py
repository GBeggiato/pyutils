from typing import Any, Callable

from pandas import DataFrame as DF


def pipable(f: Callable[[DF], DF | None]) -> Callable[[DF], DF]:
    """decorator for functions fed to pandas.pipe"""
    def inner(*args: Any, **kwargs: Any) -> DF:
        assert isinstance(args[0], DF)
        r = f(*args, **kwargs)
        return r if isinstance(r, DF) else args[0]
    return inner
