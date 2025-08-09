"""
basic logging functionality for a local app/cli

# usage
```python
import logging

from logger_setup import configure_logs


PR_LOG = "PR_LOG"


def main():  # or any other func
    configure_logs(PR_LOG)
    log = logging.getLogger(PR_LOG)
    log.info("some info from my project")
```

# many thanks to:
https://www.youtube.com/watch?v=-YelOky3ZRE&list=WL&index=4
"""

import logging
import logging.handlers
from pathlib import Path
import sys


def configure_logs(app_name: str):
    """ basic logging config """
    MB = 1<<20
    DATE_FMT = "%Y-%m-%d %H:%M:%S"
    log_path = Path(__file__).parent / "logs" 
    log_path.mkdir(parents=True, exist_ok=True)

    stream_h = logging.StreamHandler(stream=sys.stdout)
    rotating_file_h = logging.handlers.RotatingFileHandler(
        filename = log_path / "rotating.log",
        maxBytes = MB * 10,
        backupCount = 6
    )

    fmter = logging.Formatter(
        fmt="{name} [{asctime}] {levelname} ({funcName}) - {message}",
        style="{",
        datefmt=DATE_FMT
    )
    stream_h.setFormatter(fmter)
    rotating_file_h.setFormatter(fmter)

    app_log = logging.getLogger(app_name)
    app_log.setLevel(logging.INFO)

    app_log.addHandler(stream_h)
    app_log.addHandler(rotating_file_h)

