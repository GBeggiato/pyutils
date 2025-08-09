from typing import Callable

from pandas import DataFrame as DF


def pipable(f: Callable):
    """decorator for functions fed to pandas.pipe"""
    def inner(*args, **kwargs) -> DF:
        assert isinstance(args[0], DF)
        r = f(*args, **kwargs)
        return r if isinstance(r, DF) else args[0]
    return inner
