from functools import cached_property
import itertools
from typing import NamedTuple, Union

from pyspark.sql import DataFrame as SparkDF, functions as sf, Column
from pyspark.sql.session import SparkSession


STR_OR_LIST = Union[str, list[str]]


class SparkMergeError(Exception):
    """custom error for non trivial cases"""
    pass


class ColumnNames:
    """
    convenience class to manage the renaming of
    the columns due to the merge implementation
    """

    def __init__(self, names:list[str], suffix:str):
        self._original_names = names
        self.suffix = suffix

    @property
    def old_names(self) -> list[str]:
        return self._original_names
    
    @cached_property
    def new_names(self) -> list[str]:
        return [f"{c}{self.suffix}" for c in self._original_names]

    @cached_property
    def old_to_new(self) -> dict[str, str]:
        return {old:new for old, new in zip(self._original_names, self.new_names)}
    
    @cached_property
    def new_to_old(self) -> dict[str, str]:
        return {v:k for k,v in self.old_to_new.items()}

    
class ColumnManager(NamedTuple):
    left: ColumnNames
    right: ColumnNames

    @property
    def common_cols_old(self) -> frozenset:
        return frozenset(self.left.old_names).intersection(self.right.old_names)
    
    def new_names_zip(self):
        return zip(self.left.new_names, self.right.new_names)


def _validate_inputs(left, right, on, left_on, right_on, sort, suffixes, indicator, validate):
    if not isinstance(left, SparkDF):
        raise TypeError(f"'left' must be a spark dataframe, got {type(left)}")
    if not isinstance(right, SparkDF):
        raise TypeError(f"'right' must be a spark dataframe, got {type(right)}")
    if (on and left_on) or (on and right_on):
        raise SparkMergeError("the options 'on' and 'left_on, right_on' are mutually exclusive. please choose only one")
    if on is not None:
        if not isinstance(on, (str, list)):
            raise TypeError(f"'on' must be string or list of strings, got {type(on)}")
    if left_on is not None:
        if not frozenset(left_on).issubset(left.columns):
            raise ValueError(f"not all column names in 'left_on' are present in the 'left' dataframe")
        if right_on is None:
            raise ValueError("'left_on' and 'right_on' mut both be provided")
        else:
            if not frozenset(right_on).issubset(right.columns):
                raise ValueError(f"not all column names in 'right_on' are present in the 'right' dataframe")
    else:
        if right_on is not None:
            raise ValueError("'left_on' and 'right_on' mut both be provided")
    if all(i is None for i in (on, left_on, right_on)):
        raise ValueError("please provide either 'on' or both 'left_on' and 'right_on'")
    if not isinstance(sort, bool):
        raise TypeError(f"'sort' must be boolean, got {type(sort)}")
    if not isinstance(suffixes, (tuple, list)):
        raise TypeError(f"'suffixes' must be list, got {type(suffixes)}")
    if not len(suffixes) == 2:
        raise ValueError(f"'suffixes' must have lenght 2, got {len(suffixes)}")
    if suffixes[0] == suffixes[1]:
        raise ValueError("Elements of suffixes can't be equal")
    if not all(isinstance(i, str) for i in suffixes):
        raise TypeError("Elements of suffixes must be strings")
    if not isinstance(indicator, bool):
        raise TypeError(f"'indicator' must be boolean, got {type(indicator)}")
    _validators = (
        None,
        "one_to_one", "1:1",
        "one_to_many", "1:m",
        "many_to_one", "m:1",
        "many_to_many", "m:m",
    )
    if validate not in _validators:
        raise ValueError(f"'validate must be one of the following strings: {' '.join(_validators)}'")
    

def add_suffix_to_all_cols(df:SparkDF, name_map:dict[str, str]) -> SparkDF:
    """
    returns a copy of the input dataframe with all columns 
    presenting the provided suffix
    """
    renamed_df = df
    for (old, new) in name_map.items():
        renamed_df = renamed_df.withColumnRenamed(old, new)
    return renamed_df


def _listify(item:STR_OR_LIST) -> list[str]:
    if isinstance(item, str):
        return [item]
    assert isinstance(item, list)
    assert all(isinstance(i, str) for i in item)
    return item


def restore_old_names(joined:SparkDF, column_manager:ColumnManager, join_column_manager:ColumnManager) -> SparkDF:
    """
    general name cleanup function
    """
    # first the non-join columns
    left, right = column_manager
    jleft, jright = join_column_manager
    _to_skip = frozenset(itertools.chain(
        jleft.new_names, 
        jright.new_names,
        frozenset(left.old_to_new.get(i) for i in column_manager.common_cols_old),
        frozenset(right.old_to_new.get(i) for i in column_manager.common_cols_old)
    ))
    for new_name in joined.columns:   
        if new_name in _to_skip:
            continue
        old_name = left.new_to_old.get(new_name) or right.new_to_old.get(new_name)
        if (old_name in join_column_manager.common_cols_old) or (old_name is None):
            continue
        joined = joined.withColumnRenamed(new_name, old_name)
    # then the join-related columns
    join_cols = sorted(itertools.chain(jleft.new_names, jright.new_names))
    for i in range(0, len(join_cols), 2):
        left_name = join_cols[i]
        right_name = join_cols[i+1]
        old_name = left.new_to_old.get(left_name)
        joined = (
            joined
            .withColumnRenamed(left_name, old_name)
            .drop(right_name)
        )
    # remove the indicator columns
    for col in joined.columns:
        if col.endswith("_indicator_merge"):
            new_indicator_name = f"{left.new_to_old.get(col[:-16])}_merge"
            assert new_indicator_name is not None
            joined = joined.withColumnRenamed(col, new_indicator_name)
    return joined


def listify_join_syntax(on:STR_OR_LIST | None, left_on:STR_OR_LIST, right_on:STR_OR_LIST) -> tuple[list[str], list[str]]:
    if on is not None:
        on = _listify(on)
        left_on, right_on = on, on
    else:
        if not len(left_on) == len(right_on):
            raise ValueError("len(left_on) != len(right_on)")
        left_on = _listify(left_on)
        right_on = _listify(right_on)
    return left_on, right_on


def build_join_expr(left:SparkDF, right:SparkDF, join_column_manager:ColumnManager) -> list[Column]:
    """build join expression"""
    return [left[left_col] == right[right_col] for left_col, right_col in join_column_manager.new_names_zip()]


def rename_dfs(left:SparkDF, right:SparkDF, column_manager:ColumnManager) -> tuple[SparkDF, SparkDF]:
    left = add_suffix_to_all_cols(left, column_manager.left.old_to_new)
    right = add_suffix_to_all_cols(right, column_manager.right.old_to_new)
    return left, right


def get_indicator_cols_names(join_column_manager:ColumnManager) -> tuple:
    indicator_left = (f"{col}_indicator" for col in join_column_manager.left.new_names)
    indicator_right = (f"{col}_indicator" for col in join_column_manager.right.new_names)
    return tuple(zip(indicator_left, indicator_right))


def compute_indicator_cols(left:SparkDF, right:SparkDF, joined:SparkDF, join_column_manager:ColumnManager) -> SparkDF:
    for i, (left_name, right_name) in enumerate(join_column_manager.new_names_zip()):
        in_left = joined[left_name].isin(left[left_name])
        in_right = joined[right_name].isin(right[right_name])
        merge_col = (
            sf
            .when((in_left == in_right), "both")
            .when((in_left.isNotNull()) & (in_right.isNull()), "left_only")
            .when((in_left.isNull()) & (in_right.isNotNull()), "right_only")
        )
        if not i:
            joined = joined.withColumn(f"_merge", merge_col)
        else:
            joined = joined.withColumn(
                f"_merge",
                sf.when(sf.col("_merge").isNotNull(), sf.col("_merge")).otherwise(merge_col)
            )
    return joined


def join_with_indicators(left:SparkDF, right:SparkDF, column_manager:ColumnManager, join_column_manager:ColumnManager, how:str):
    left, right = rename_dfs(left, right, column_manager)
    join_expr = build_join_expr(left, right, join_column_manager)
    # =================================
    # SPARK JOIN HERE
    joined = (
        left
        .join(
            other = right,
            on = join_expr,
            how = how
        )
    )
    # =================================

    return joined


def _get_key_uniqueness(df:SparkDF, key_cols:list[str]) -> bool:
    pool = df.select(*key_cols).cache()
    rows = pool.count()
    unique_rows = pool.dropDuplicates().count()
    return rows == unique_rows


def _validate(left:SparkDF, right:SparkDF, validate:str, join_column_manager:ColumnManager) -> None:
    # Check uniqueness of each
    left_unique = _get_key_uniqueness(left, join_column_manager.left.old_names)
    right_unique = _get_key_uniqueness(right, join_column_manager.right.old_names)
    # Check data integrity
    if validate in ["one_to_one", "1:1"]:
        if not left_unique and not right_unique:
            raise SparkMergeError(
                "Merge keys are not unique in either left "
                "or right dataset; not a one-to-one merge"
            )
        if not left_unique:
            raise SparkMergeError(
                "Merge keys are not unique in left dataset; not a one-to-one merge"
            )
        if not right_unique:
            raise SparkMergeError(
                "Merge keys are not unique in right dataset; not a one-to-one merge"
            )

    elif validate in ["one_to_many", "1:m"]:
        if not left_unique:
            raise SparkMergeError(
                "Merge keys are not unique in left dataset; not a one-to-many merge"
            )

    elif validate in ["many_to_one", "m:1"]:
        if not right_unique:
            raise SparkMergeError(
                "Merge keys are not unique in right dataset; "
                "not a many-to-one merge"
            )
    elif validate in ["many_to_many", "m:m"]:
        pass
    else:
        raise ValueError(
            f'"{validate}" is not a valid argument. '
            "Valid arguments are:\n"
            '- "1:1"\n'
            '- "1:m"\n'
            '- "m:1"\n'
            '- "m:m"\n'
            '- "one_to_one"\n'
            '- "one_to_many"\n'
            '- "many_to_one"\n'
            '- "many_to_many"'
        )


def spark_merge(
    left:SparkDF, 
    right:SparkDF, 
    *, 
    how:str='inner', 
    on:STR_OR_LIST=None, 
    left_on:STR_OR_LIST=None, 
    right_on:STR_OR_LIST=None,  
    sort:bool=False,
    suffixes:tuple[str, str]=('_left', '_right'),  
    indicator:bool=False, 
    validate:str=None):
    """
    spark implementation of the pandas merge algorithm
    [more or less...]

    ### References
    https://pandas.pydata.org/docs/reference/api/pandas.merge.html

    https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html

    ## Parameters
    - left: pyspark.sql.DataFrame; the left table of the join
    - right: pyspark.sql.DataFrame; the right table of the join
    - how:str; type of merge, defaults to inner
    - on: str or list of str; the columns to use on the join. must be present in both columns
    - left_on str or list of str; the columns of the left table to use on the join
    - right_on str or list of str; the columns of the right table to use on the join
    - sort:bool; sort the join keys lexicographically in the result DataFrame.
    - suffixes: list[str, str] A length-2 sequence where each element is optionally a string indicating the suffix to add to overlapping column names in left and right respectively.
    - idicator: If True, adds a column to the output DataFrame called “_merge” with information on the source of each row.
    - validate: If specified, checks if merge is of specified type.
      - “one_to_one” or “1:1”: check if merge keys are unique in both left and right datasets.
      - “one_to_many” or “1:m”: check if merge keys are unique in left dataset.
      - “many_to_one” or “m:1”: check if merge keys are unique in right dataset.
      - “many_to_many” or “m:m”: allowed, but does not result in checks.
    """
    _validate_inputs(left, right, on, left_on, right_on, sort, suffixes, indicator, validate)
    # setup for columns management
    left_suffix, right_suffix = suffixes
    column_manager = ColumnManager(
        left = ColumnNames(left.columns, left_suffix), 
        right = ColumnNames(right.columns, right_suffix)
    )
    left_on, right_on = listify_join_syntax(on, left_on, right_on)
    join_column_manager = ColumnManager(
        left = ColumnNames(left_on, left_suffix), 
        right = ColumnNames(right_on, right_suffix)
    )
    # validate type of join if requested
    if validate is not None:
        _validate(left, right, validate, join_column_manager)
    # perform join op
    joined = join_with_indicators(left, right, column_manager, join_column_manager, how)
    if indicator:
        joined = compute_indicator_cols(left, right, joined, join_column_manager)
    # column name cleanup
    joined = restore_old_names(joined, column_manager, join_column_manager)
    if sort:
        joined = joined.orderBy(*join_column_manager.left.old_names)    
    return joined

# =======================================================================


import pandas as pd

def setup():
    pk = [3, 2, 2, 4]
    left = pd.DataFrame()
    left["PK"] = pk
    left["NAME"] = ["Bob", "Alex", "Carl", "David"]

    right = pd.DataFrame()
    right["PK"] = pk
    right["NAME"] = ["Bob", "Gill", "Harold", "Ingrid"]
    right["AGE"] = [32, 41, 26, 29]
    return left, right


def main():
    spark = SparkSession.getActiveSession()

    kwargs = {
        "how": "inner", 
        "on": ["PK"], 
        "suffixes": ('_left', '_right'), 
        "sort": True,
        "indicator": True,
        "validate": "m:m"
    }
    
    left, right = setup()

    try:
        m = pd.merge(left, right, **kwargs)
        print(m, end="\n\n")
    except pd.errors.MergeError as e:
        print(e)

    try:
        j = spark_merge(
            spark.createDataFrame(left), 
            spark.createDataFrame(right), 
            **kwargs
        ).toPandas()
        print(j, end="\n\n")
    except SparkMergeError as e:
        print(e)
    

if __name__ == "__main__":
    main()
