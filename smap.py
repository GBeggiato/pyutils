
import csv
import dataclasses
import itertools
from pathlib import Path
import shutil
import sqlite3
from sqlite3 import Connection
import tempfile
from typing import Any, Generator, Iterable


type ColToType = list[tuple[str, str | str]]


REAL = "REAL"
INTEGER = "INTEGER"
TEXT = "TEXT"

SQLITE_QUERY_TABLES = """
    SELECT name
    FROM sqlite_schema
    WHERE type ='table' AND name NOT LIKE 'sqlite_%';
"""


def sqlite3_table_names(f: Path) -> list[str]:
    with sqlite3.connect(f) as conn:
        return list(itertools.chain.from_iterable(conn.execute(SQLITE_QUERY_TABLES).fetchall()))


def _is_float(s: str) -> bool:
    try:
        _ = float(s)
        return True
    except ValueError:
        return False


def _thing_to_sqlite_type(t: Any) -> str:
    # TODO: properly
    if isinstance(t, int):
        return INTEGER
    elif isinstance(t, float):
        return REAL
    else:
        return TEXT


def _str_to_sqlite_type(s: str) -> str | None:
    # TODO: properly
    assert isinstance(s, str)
    if s == "":
        return None
    if s.isdecimal():
        return INTEGER
    if _is_float(s):
        return REAL
    return TEXT


@dataclasses.dataclass(slots=True)
class _QueryResult:
    _columns: tuple[str, ...]
    _rows: Iterable[tuple]

    @classmethod
    def _from_cursor(cls, c: sqlite3.Cursor):
        cols = tuple(x[0] for x in c.description)
        return cls(cols, iter(c))

    def columns(self) -> tuple[str, ...]:
        return self._columns

    def rows(self) -> Generator[tuple, Any, None]:
        yield from self._rows

    def to_csv(self, f: Path):
        with f.open("w") as fp:
            writer = csv.writer(fp)
            writer.writerow(self._columns)
            writer.writerows(self._rows)

    def to_sqlite(self, f: Path, table_name: str):
        rows = iter(self._rows)
        collected_rows = []
        d: None | ColToType = None
        while 1:
            row = next(rows)
            collected_rows.append(row)
            if any(r is None for r in row):
                continue
            else:
                # why does zip convert str to str ?
                d = list(dict(zip(
                    self._columns, 
                    map(_thing_to_sqlite_type, row),
                    strict=True
                )).items()) 
                break
        assert d is not None, "could not infer types from query result (each row has at least 1 NULL value)"
        q_create = _build_create_statement(d, table_name)
        q_insert = _build_insert_statement(d, table_name)

        with sqlite3.connect(f) as conn:
            conn.execute(q_create)
            conn.executemany(q_insert, collected_rows)
            conn.executemany(q_insert, rows)

    def head(self, *, show_all:bool=False):
        """
        helper method to check the result

        note this consumes the rows iterator
        """
        print(self._columns)
        print("-"*len(str(self._columns)))
        for i, e in enumerate(self._rows):
            if (not show_all) and i > 5:
                break
            print(e)


class _FileTypesParser:

    def __init__(self, col_to_type: ColToType) -> None:
        self.colname_to_sqlitetype = col_to_type

    @classmethod
    def from_sqlite(cls, f: Path, table_name: str):
        # NOTE: the 'table_name' is both the old table name and the new table name
        with sqlite3.connect(f) as conn:
            q = f"select * from {table_name} limit 2"
            cur = conn.execute(q)
            cols = (c[0] for c in cur.description)
            row = next(cur)
            row_type = map(_thing_to_sqlite_type, row)
            d = dict(zip(cols, row_type, strict=True))
        assert all(isinstance(v, str) for v in d.values()), "could not detect dtypes"
        assert all(isinstance(k, str) for k in d.keys())
        return cls(list(d.items()))

    @classmethod
    def from_csv(cls, f: Path):
        typedict = {}
        with f.open() as fp:
            reader = csv.DictReader(fp)
            for row in reader:
                for k, v in row.items():
                    if typedict.get(k) is None:
                        typedict[k] = _str_to_sqlite_type(v)
                if all(v is not None for v in typedict.values()):
                    break
        assert all(isinstance(v, str) for v in typedict.values()), "could not detect dtypes"
        return cls(list(typedict.items()))

    def queries(self, table_name: str) -> tuple[str, str]:
        return (
            _build_create_statement(self.colname_to_sqlitetype, table_name),
            _build_insert_statement(self.colname_to_sqlitetype, table_name)
        )


def _build_create_statement(colname_to_sqlitetype: ColToType, table_name: str) -> str:
    insert = ", ".join((f"{k} {v}" for k, v in colname_to_sqlitetype))
    return f"CREATE TABLE IF NOT EXISTS {table_name} ({insert})"


def _build_insert_statement(colname_to_sqlitetype: ColToType, table_name: str) -> str:
    values = ", ".join((f":{k}" for k, _ in colname_to_sqlitetype))
    insert = values.replace(":", "")
    return f"INSERT INTO {table_name} ({insert}) VALUES ({values})"


class DataSource:
    """local temporary db"""

    def __init__(self) -> None:
        self._db: Path | None = None
        self._conn: Connection | None = None

    @property
    def path(self):
        """path to the db file"""
        self.assert_with_ctx()
        assert self._db.exists()
        return self._db

    def __enter__(self):
        self._db = Path(tempfile.mkstemp(suffix=".db")[1])
        self._conn = sqlite3.connect(self._db)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._conn is not None:
            self._conn.close()
        if self._db is not None and self._db.exists():
            self._db.unlink()

    def assert_with_ctx(self):
        assert self._db is not None and self._conn is not None, "can be called only from 'with' context"

    def tables(self) -> list[str]:
        self.assert_with_ctx()
        return sqlite3_table_names(self._db)

    def add_csv(self, table_name: str, inpt: Path):
        self.assert_with_ctx()
        _create_table_from_csv(inpt, self._conn, table_name)

    def add_sqlite(self, inpt: Path, inpt_table: str, table_name: str):
        """loads a single sqlite table"""
        self.assert_with_ctx()
        _create_table_from_sqlite(inpt, inpt_table, self._conn, table_name)

    def add_sqlite_db(self, f: Path):
        """loads a full sqlite file"""
        self.assert_with_ctx()
        tables = sqlite3_table_names(f)
        for table in tables:
            _create_table_from_sqlite(f, table, self._conn, table)

    def query(self, sql: str, args = None) -> _QueryResult:
        """ execute a sqlite-style query against the available data sources """
        self.assert_with_ctx()
        params = (sql, ) if args is None else (sql, args)
        res = self._conn.execute(*params)
        cols = tuple(x[0] for x in res.description)
        return _QueryResult(cols, iter(res))

    def save(self, f: Path):
        """ saves as copy of the current db """
        assert f.suffix == ".db", "not a sqlite3 file"
        self.assert_with_ctx()
        shutil.copy(self._db, f)


def _create_table_from_csv(inpt: Path, conn: Connection, table_name: str):
    q_create, q_insert = _FileTypesParser.from_csv(inpt).queries(table_name)
    conn.execute(q_create)
    with inpt.open() as fp:
        reader = csv.DictReader(fp)
        conn.executemany(q_insert, reader)
    conn.commit()


def _create_table_from_sqlite(inpt: Path, inpt_name: str, conn: Connection, table_name: str):
    q_create, q_insert = _FileTypesParser.from_sqlite(inpt, inpt_name).queries(table_name)
    conn.execute(q_create)
    with sqlite3.connect(inpt) as old_conn:
        cur = old_conn.execute(f"select * from {inpt_name}")
        cols = [c[0] for c in cur.description]
        rows = map(lambda r: dict(zip(cols, r, strict=True)), cur)
        conn.executemany(q_insert, rows)
    conn.commit()

