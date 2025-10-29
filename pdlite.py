from contextlib import closing
import datetime
from pathlib import Path
import sqlite3
import tempfile
from typing import Generator, Literal, Optional

import pandas as pd
from pandas import DataFrame as DF


# datetime inherits from date
def _isoformat(d: datetime.date) -> str:
    return d.isoformat()


def _fromisoformat(s: bytes) -> datetime.datetime:
    return datetime.datetime.fromisoformat(s.decode())


sqlite3.register_adapter(datetime.date, _isoformat)
sqlite3.register_converter("date", lambda s: _fromisoformat(s).date())

# pandas.Timestamp relies on the standard datetime and this conversion is
# enough for pandas.read_sql to parse the dates as datetime64[ns]
sqlite3.register_adapter(datetime.datetime, _isoformat)
sqlite3.register_converter("datetime", _fromisoformat)


def _pdlite_connect(db: Path) -> closing[sqlite3.Connection]:
    return closing(sqlite3.connect(
        database     = db,
        detect_types = sqlite3.PARSE_DECLTYPES
    ))


def tables(db: Path) -> Generator[str, None, None]:
    """lazily list table names in a sqlite db file"""
    with _pdlite_connect(db) as conn:
        yield from conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")


def to_sqlite(df: DF, table: str, db: Path, if_exists: Literal["fail", "replace", "append"]="fail"):
    """dump a pandas df to a sqlite3 db"""
    with _pdlite_connect(db) as conn:
        return df.to_sql(name=table, con=conn, if_exists=if_exists, index=False)


def from_sqlite(table: str, db: Path, sql: Optional[str]=None) -> DF:
    """read a sqlite3 query into a pandas df"""
    _sql = sql or f"SELECT * FROM {table}"
    with _pdlite_connect(db) as conn:
        return pd.read_sql(sql=_sql, con=conn)


def query_df(df: DF, sql: str, table = "temp_table") -> DF:
    """run a sqlite3 query on a pandas df"""
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "temp.db"
        to_sqlite(df, table, db, if_exists="fail")
        return from_sqlite(table, db, sql)

