from graphlib import TopologicalSorter
import itertools
import string
from typing import Any, Callable, Iterable, Literal, Self

from pyspark.sql import DataFrame, SparkSession

# ==============================================================================
# string manip
# ==============================================================================

_DOT = "."
_ALLOWED_CHARS = frozenset(string.ascii_letters + string.digits + "_")


def dot_split(s: str) -> list[str]:
    "s.split('.')"
    return s.split(_DOT)


def dot_join(*strings: str) -> str:
    "`'.'.join(strings)`, available as `dot_join` and `dj`"
    return _DOT.join(strings)


dj = dot_join


def is_valid_tablepath(value: Any) -> bool:
    "can this represent a table location in databricks ?"
    if not isinstance(value, str):
        return False
    if not value.count(_DOT) == 2:
        return False
    return all(_ALLOWED_CHARS.issuperset(p) for p in dot_split(value))

# ==============================================================================
# spark connection
# ==============================================================================

def sparksession() -> SparkSession:
    "SparkSession.active()"
    return SparkSession.active()


def table_exists(table: str, db: str | None = None) -> bool:
    "spark.catalog.tableExists(tableName, dbName)"
    return sparksession().catalog.tableExists(table, db)


def table_drop(name: TablePath):
    sql_execute(sql_drop(name))


def table_overwrite(df: DataFrame, name: TablePath):
    df.write.option("overwriteSchema", "true").saveAsTable(name, mode="overwrite")


def table_append(df: DataFrame, name: TablePath):
    "append to existing table, tolerating for new cols"
    df.write.option("mergeSchema", "true").saveAsTable(name, mode="append")

# ==============================================================================
# types
# ==============================================================================

class TablePath(str):
    "path of the form `catalog.schema.name`"

    def __new__(cls, value: Any):
        if not is_valid_tablepath(value):
            raise ValueError(f"'{value}' is not a valid {cls.__name__}")
        return super().__new__(cls, value)

    @classmethod
    def from_parts(cls, catalog: str, schema: str, name: str) -> Self:
        return cls(dot_join(catalog, schema, name))

    @property
    def parts(self) -> list[str]:
        "catalog, schema, name"
        return dot_split(self)

    @property
    def catalog(self) -> str:
        return self.parts[0]

    @property
    def schema(self) -> str:
        return self.parts[1]

    @property
    def name(self) -> str:
        return self.parts[2]

    def _with_new_part(self, new: str, pos: Literal[0, 1, 2]) -> Self:
        parts = self.parts
        parts[pos] = new
        return self.from_parts(*parts)

    def with_catalog(self, new: str) -> Self:
        return self._with_new_part(new, 0)

    def with_schema(self, new: str) -> Self:
        return self._with_new_part(new, 1)

    def with_name(self, new: str) -> Self:
        return self._with_new_part(new, 2)

    def exists(self) -> bool:
        "calls into spark.catalog"
        return table_exists(self)

# ==============================================================================
# sql
# ==============================================================================

def sql_select_all(table: TablePath) -> str:
    "select * from {table}"
    return f"select * from {table}"


def sql_create_view(destination: TablePath, definition: str) -> str:
    "create or replace view {destination} as ({definition})"
    return f"create or replace view {destination} as ({definition})"


def sql_expose_with_view(destination: TablePath, source: TablePath) -> str:
    "create or replace view {destination} as (select * from {source})"
    return sql_create_view(destination, sql_select_all(source))


def sql_drop(table: TablePath) -> str:
    "drop table if exists {table}"
    return f"drop table if exists {table}"


def sql_execute(sqlQuery: str, args: dict[str, Any] | list | None = None):
    "spark.sql(sqlQuery, args)"
    sparksession().sql(sqlQuery=sqlQuery, args=args)

# ==============================================================================
# graphlib wrapper
# ==============================================================================

def topo_sort[T](
    items: Iterable[T],
    get_predecessor: Callable[[T], list[T]]
) -> list[list[T]]:
    """
    wrapper around graphlib.TopologicalSorter() logic

    - items: list of elements
    - get_predecessor: callable that gets the predecessors for an item

    ## Note
    works only on hashable elements

    ## Example
    >>> nums = {
    >>>     1: [2, 3],
    >>>     2: [4]
    >>> }
    >>> xs = topo_sort(nums.keys(), lambda k: nums[k])
    """
    sorter = TopologicalSorter()
    for item in items:
        sorter.add(item, *get_predecessor(item))
    ordered = list()
    sorter.prepare()
    while sorter.is_active():
        nodes = list(sorter.get_ready())
        ordered.append(nodes)
        sorter.done(*nodes)
    return ordered


def topo_sort_obj[T, K](
    items: Iterable[T], *,
    get_key: Callable[[T], K],
    get_pred: Callable[[T], list[K]]
) -> list[list[T]]:
    """
    allows to topologically sort complex objects

    ## Example
    >>> @dataclass
    >>> class P:
    >>>     name: int
    >>>     preds: list[int]
    >>>
    >>> points = [
    >>>     P(1, [2, 3]),
    >>>     P(2, [4])
    >>> ]
    >>> xs = topo_sort_obj(
    >>>     points,
    >>>     get_key=lambda p: p.name,
    >>>     get_pred=lambda p: p.preds
    >>> ) # [[P(name=2, preds=[4])], [P(name=1, preds=[2, 3])]]
    """
    sortable = {get_key(p): p for p in items}
    xs = topo_sort(sortable.keys(), lambda p: get_pred(sortable[p]))
    xs = [list(filter(None, (sortable.get(k) for k in x))) for x in xs]
    return [x for x in xs if len(x) > 0]


def flatten[T](xs: list[list[T]]) -> list[T]:
    return list(itertools.chain.from_iterable(xs))

# ==============================================================================

