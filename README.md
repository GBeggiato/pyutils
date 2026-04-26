# pyutils
- collection of small python scripts with various purposes
- intended usage is to be copied in bigger projects and used / imported as needed
- each one is independent
- **mostly** we depend only on the standard library

### cls.py
clear screen

### find_repo.py
find all git repos starting from a root path

### greppy.py
poor grep substitute to be used on Windows

### impl_date.py
functions to extend the `datetime` module from the standard library. works also
with the pandas `TimeStamp` due to duck typing

### impl_path.py
functions to extend the `pathlib` module from the standard library

### logger_setup.py
basic example on how to set up a logger using the `logging` module

### mex.py
small utility to select the best analytical model according to user-defined rules.
###### Example
you need to individuate the best linear model on a dataset:

```python3
from statistics import linear_regression, LinearRegression

import pandas as pd

from mex import estimate_models


data = pd.read_csv(some_file)


# all these are column name in the above df
y_col  = "grade"
x_cols = ["age", "hours_studied"]


def _filter_models(combos: list[str]) -> bool:
    "only single regressors"
    return len(combos) == 1


def _filter_results(model: LinearRegression) -> bool:
    "the estimated model needs some caracteristics"
    return model.slope > 0.3


ok_models = estimate_models(
    y            = y_col, 
    xs           = x_cols, 
    data         = data, 
    modelf       = linear_regression, 
    filterxs     = _filter_models, 
    filtermodels = _filter_results, 
)

for m in ok_models:
    print(m)
```

### nb2.py
python notebook to plain file converter. available as cli and function

### oasis.py
basic scheduler

### pdlite.py
pandas-sqlite3 interaction

### pdpipable.py
make a function suitable to be passed to the `pipe` method available on the
pandas DataFrame

### profiling.py
basic profiler

### pytt.py
python test template: CLI to generate the boilerplate to test a python file

### smap.py
small analytical pipeline. useful to replace pandas when all you are doing are
DB operations

### spark_merge.py
extension to the pyspark api to get a pandas-like merge function

### xlsheet.py
read an Excel file using only the standard library

