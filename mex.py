import itertools
import typing as ty


T = ty.TypeVar("T")
Gen = ty.Generator[T, ty.Any, None]
Predicate = ty.Callable[[T], bool]


def sigma_algebra(it: list[T]) -> Gen[list[T]]:
    """
    the set of all the subsets of it

    if t has n elements, then sigma_algebra(t) has 2^n elements
    """
    for i in range(len(it)+1):
        yield from map(list, itertools.combinations(it, i))


def estimate_models(
    y            : str,
    xs           : list[str],
    data         : ty.Any,
    modelf       : ty.Callable[[ty.Any, ty.Any], T],
    filterxs     : ty.Optional[Predicate[list[str]]] = None,
    filtermodels : ty.Optional[Predicate] = None
) -> Gen[T]:
    """
    * y            : data[y] is the y data points
    * xs           : data[xs] is the xs data points
    * data         : the actual data container, usually a dataframe or similar
    * modelf       : a function that takes data[y] and data[xs] data points and returns an estimation
    * filterxs     : optional predicate to filter out unwanted regressors combinations
    * filtermodels : optional predicate to filter out models after estimation
    """
    pool = sigma_algebra(xs)
    if filterxs is not None:
        pool = filter(filterxs, pool)
    y_data = data[y]
    pool = (modelf(y_data, data[x]) for x in pool)
    if filtermodels is not None:
        pool = filter(filtermodels, pool)
    yield from pool

