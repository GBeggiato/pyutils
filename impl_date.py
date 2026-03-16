"""
simple date manipulation. 

works on pandas.TimeStamp as well
"""

from datetime import date, timedelta


ONE_DAY = timedelta(days=1)
ONE_WEEK = ONE_DAY * 7
# not provided by the datetime module
WEEKDAY_MON = 0
WEEKDAY_TUE = 1
WEEKDAY_WED = 2
WEEKDAY_THU = 3
WEEKDAY_FRI = 4
WEEKDAY_SAT = 5
WEEKDAY_SUN = 6
DAYS_IN_WEEK = 7


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
    return d.replace(day=1, month=1)


def end_of_year(d: date) -> date:
    return d.replace(day=31, month=12)


def start_of_week(d: date) -> date:
    return d - ONE_DAY * d.weekday()


def end_of_week(d: date) -> date:
    return d + ONE_DAY * (WEEKDAY_SUN - d.weekday())


def is_weekend(d: date) -> bool:
    return d.weekday() > WEEKDAY_FRI


def is_workday(d: date) -> bool:
    return d.weekday() < WEEKDAY_SAT


def closest_previous_working_day(d: date) -> date:
    """converts weekends into fridays. do not implement IRL"""
    return d if is_workday(d) else d - (ONE_DAY * (d.weekday() - WEEKDAY_FRI))


def closest_next_working_day(d: date) -> date:
    return d if is_workday(d) else d + (ONE_DAY * (DAYS_IN_WEEK - d.weekday()))


def previous_working_day(d: date) -> date:
    dd = closest_previous_working_day(d)
    return prev_day(dd) if is_workday(d) else dd
    

def next_working_day(d: date) -> date:
    dd = closest_next_working_day(d)
    return next_day(dd) if is_workday(d) else dd


def previous_monday(d: date) -> date:
    return prev_week(d) if d.weekday() == WEEKDAY_MON else start_of_week(d)


def next_monday(d: date) -> date:
    return d + (DAYS_IN_WEEK - d.weekday())


def previous_friday(d: date) -> date:
    wd = d.weekday()
    return prev_week(d) if wd == WEEKDAY_FRI else d - (ONE_DAY * ((wd + 3) % 7))


def next_friday(d: date) -> date:
    offsets = (4, 3, 2, 1, 7, 6, 5)
    return d + (ONE_DAY * offsets[d.weekday()])
