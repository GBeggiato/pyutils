
import cProfile
import pstats
from typing import Callable


def profile(f: Callable):
    """
    basic profiler

    Example

    from profiling import profile

    @profile
    def your_func():
        ...

    """
    def profiled(*args, **kwargs):
        with cProfile.Profile() as profile:
            f(*args, **kwargs)
        results = pstats.Stats(profile)
        results.sort_stats(pstats.SortKey.TIME)
        results.print_stats()
    return profiled
