"""
basic logging functionality for a local app/cli

# usage
>>> import logger
>>>
>>>
>>> PR_LOG = "PR_LOG"
>>>
>>>
>>> def main():  # or any other func
>>>     logger.configure(PR_LOG)
>>>     lgr = logger.get(PR_LOG)
>>>     lgr.info("some info from my project")

# many thanks to:
https://www.youtube.com/watch?v=-YelOky3ZRE&list=WL&index=4
"""

import logging
import logging.handlers
from pathlib import Path
import sys


def get(name: str) -> logging.Logger:
    return logging.getLogger(name)


def configure(app_name: str):
    """ basic logging config """
    fmter = logging.Formatter(
        fmt="{name} [{asctime}] {levelname} - {message} ({funcName})",
        style="{",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    stream_h = logging.StreamHandler(stream=sys.stdout)
    stream_h.setFormatter(fmter)

    log_path = Path(__file__).parent / "logs"
    log_path.mkdir(parents=True, exist_ok=True)
    rotating_file_h = logging.handlers.RotatingFileHandler(
        filename = log_path / "rotating.log",
        maxBytes = (1<<20) * 10,
        backupCount = 6
    )
    rotating_file_h.setFormatter(fmter)

    app_log = logging.getLogger(app_name)
    app_log.setLevel(logging.INFO)
    if not app_log.hasHandlers():
        app_log.addHandler(stream_h)
        app_log.addHandler(rotating_file_h)


def configure_for_library(name: str):
    """
    """
    lgr = get(name)
    lgr.addHandler(logging.NullHandler())
