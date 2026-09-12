import string
from typing import Any, Literal, Self

from pyspark.sql import DataFrame, SparkSession

# ==============================================================================
# table name string manip routines
# ==============================================================================

_DOT = "."
_ALLOWED_CHARS = frozenset(string.ascii_letters + string.digits + "_")


def dot_split(s: str) -> list[str]:
    "`s.split('.')`"
    return s.split(_DOT)


def dot_join(*strings: str) -> str:
    "`'.'.join(strings)`, available as `dot_join` and `dj`"
    return _DOT.join(strings)


dj = dot_join


def is_valid_tablepath(value: Any) -> bool:
    """
    from a string perspective, can this represent a table location in
    databricks ?
    """
    if not isinstance(value, str):
        return False
    if not value.count(_DOT) == 2:
        return False
    return all(_ALLOWED_CHARS.issuperset(p) for p in dot_split(value))

# ==============================================================================
# sql string manip routines
# ==============================================================================

def sql_select_all(table: TablePath) -> str:
    return f"select * from {table}"


def sql_create_view(definition: str, destination: TablePath) -> str:
    return f"create or replace view {destination} as ({definition})"

# ==============================================================================
# spark connection routines
# ==============================================================================

def sparksession() -> SparkSession:
    "`SparkSession.active()`"
    return SparkSession.active()


def table_exists(tableName: str, dbName: str | None = None) -> bool:
    "`sparksession().catalog.tableExists(tableName, dbName)`"
    return sparksession().catalog.tableExists(tableName, dbName)

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
        return table_exists(self)

# ==============================================================================

def main():
    tp = TablePath("data_prep.industry.mytable")
    print(tp)


if __name__ == "__main__":
    main()

