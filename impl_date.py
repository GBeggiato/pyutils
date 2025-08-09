
from datetime import date, timedelta


ONE_DAY = timedelta(days=1)
ONE_WEEK = ONE_DAY * 7


def prev_day(d: date) -> date:
    return d - ONE_DAY


def next_day(d: date) -> date:
    return d + ONE_DAY


def prev_week(d: date) -> date:
    return d - ONE_WEEK


def next_week(d: date) -> date:
    return d + ONE_WEEK


def prev_month(d: date) -> date:
    if d.month == 1:
        return d.replace(year=d.year-1, month=12)
    for i in range(4):
        try:
            return d.replace(month=d.month-1, day=d.day-i)
        except ValueError:
            continue
    raise Exception("unrechable")


def next_month(d: date) -> date:
    if d.month == 12:
        return d.replace(year=d.year+1, month=1)
    for i in range(4):
        try:
            return d.replace(month=d.month+1, day=d.day-i)
        except ValueError:
            continue
    raise Exception("unrechable")


def prev_year(d: date) -> date:
    try:
        return d.replace(year=d.year-1)
    except ValueError:
        return d.replace(year=d.year-1, day=d.day-1)


def next_year(d: date) -> date:
    try:
        return d.replace(year=d.year+1)
    except ValueError:
        return d.replace(year=d.year+1, day=d.day-1)


def quarter(d: date) -> int:
    return 1 + (d.month -  1) // 3


def semester(d: date) -> int:
    return 1 if d.month < 7 else 2


def start_of_month(d: date) -> date:
    return d.replace(day=1)


def end_of_month(d: date) -> date:
    for day in reversed(range(28, 32)):
        try:
            return d.replace(day=day)
        except ValueError:
            continue
    raise Exception("unrechable")


def start_of_year(d: date) -> date:
    return d.replace(day=31, month=12)


def end_of_year(d: date) -> date:
    return d.replace(day=1, month=1)


def start_of_week(d: date) -> date:
    return d - ONE_DAY * d.weekday()


def end_of_week(d: date) -> date:
    return d + ONE_DAY * (6 - d.weekday())

