from datetime import date, timedelta
import unittest

import impl_date


class TestImpl_Date(unittest.TestCase):

    def test_prev_month(self):
        cases = (
            (date(2026, 4, 19), date(2026, 3, 19)),
            (date(2026, 3, 31), date(2026, 2, 28)),
            (date(2024, 3, 31), date(2024, 2, 29)),
            (date(2024, 1, 31), date(2023, 12, 31)),
        )
        for inpt, expected in cases:
            self.assertEqual(impl_date.prev_month(inpt), expected)

    def test_next_month(self):
        cases = (
            (date(2026, 4, 19),  date(2026, 5, 19)),
            (date(2026, 2, 28),  date(2026, 3, 28)),
            (date(2024, 2, 29),  date(2024, 3, 29)),
            (date(2024, 12, 31), date(2025, 1, 31)),
        )
        for inpt, expected in cases:
            self.assertEqual(impl_date.next_month(inpt), expected)

    def test_prev_year(self):
        cases = (
            (date(2026, 4, 19),  date(2025, 4, 19)),
            (date(2026, 3, 31),  date(2025, 3, 31)),
            (date(2026, 2, 28),  date(2025, 2, 28)),
            (date(2024, 2, 29),  date(2023, 2, 28)),
        )
        for inpt, expected in cases:
            self.assertEqual(impl_date.prev_year(inpt), expected)

    def test_next_year(self):
        cases = (
            (date(2025, 4, 19), date(2026, 4, 19)),
            (date(2025, 3, 31), date(2026, 3, 31)),
            (date(2025, 2, 28), date(2026, 2, 28)),
            (date(2024, 2, 29), date(2025, 2, 28)),
        )
        for inpt, expected in cases:
            self.assertEqual(impl_date.next_year(inpt), expected)

    def test_start_of_week(self):
        base = date(2026, 4, 20) # monday
        for offset in range(7):
            dt = timedelta(days=offset)
            self.assertEqual(base, impl_date.start_of_week(base+dt))
        dt = timedelta(days=8)
        self.assertEqual(
            impl_date.next_week(base), 
            impl_date.start_of_week(base+dt)
        )

    def test_end_of_week(self):
        base = date(2026, 4, 19) # sunday
        for offset in range(7):
            dt = timedelta(days=offset)
            self.assertEqual(base, impl_date.end_of_week(base-dt))
        dt = timedelta(days=8)
        self.assertEqual(
            impl_date.prev_week(base), 
            impl_date.end_of_week(base-dt)
        )

    def test_previous_working_day(self):
        cases = (
            (date(2026, 4, 20), date(2026, 4, 17)),
            (date(2026, 4, 21), date(2026, 4, 20)),
            (date(2026, 4, 22), date(2026, 4, 21)),
            (date(2026, 4, 23), date(2026, 4, 22)),
            (date(2026, 4, 24), date(2026, 4, 23)),
            (date(2026, 4, 25), date(2026, 4, 24)),
            (date(2026, 4, 26), date(2026, 4, 24)),
            (date(2026, 4, 27), date(2026, 4, 24)),
        )
        for inpt, expected in cases:
            self.assertEqual(impl_date.previous_working_day(inpt), expected)

    def test_next_working_day(self):
        cases = (
            (date(2026, 4, 20), date(2026, 4, 21)),
            (date(2026, 4, 21), date(2026, 4, 22)),
            (date(2026, 4, 22), date(2026, 4, 23)),
            (date(2026, 4, 23), date(2026, 4, 24)),
            (date(2026, 4, 24), date(2026, 4, 27)),
            (date(2026, 4, 25), date(2026, 4, 27)),
            (date(2026, 4, 26), date(2026, 4, 27)),
            (date(2026, 4, 27), date(2026, 4, 28)),
        )
        for inpt, expected in cases:
            self.assertEqual(impl_date.next_working_day(inpt), expected)

    def test_previous_monday(self):
        cases = (
            (date(2026, 4, 20), date(2026, 4, 13)),
            (date(2026, 4, 21), date(2026, 4, 20)),
            (date(2026, 4, 22), date(2026, 4, 20)),
            (date(2026, 4, 23), date(2026, 4, 20)),
            (date(2026, 4, 24), date(2026, 4, 20)),
            (date(2026, 4, 25), date(2026, 4, 20)),
            (date(2026, 4, 26), date(2026, 4, 20)),
            (date(2026, 4, 27), date(2026, 4, 20)),
        )
        for inpt, expected in cases:
            self.assertEqual(impl_date.previous_monday(inpt), expected)

    def test_next_monday(self):
        cases = (
            (date(2026, 4, 13), date(2026, 4, 20)),
            (date(2026, 4, 14), date(2026, 4, 20)),
            (date(2026, 4, 15), date(2026, 4, 20)),
            (date(2026, 4, 16), date(2026, 4, 20)),
            (date(2026, 4, 17), date(2026, 4, 20)),
            (date(2026, 4, 18), date(2026, 4, 20)),
            (date(2026, 4, 19), date(2026, 4, 20)),
            (date(2026, 4, 20), date(2026, 4, 27)),
        )
        for inpt, expected in cases:
            self.assertEqual(impl_date.next_monday(inpt), expected)

    def test_previous_friday(self):
        cases = (
            (date(2026, 4, 20), date(2026, 4, 17)),
            (date(2026, 4, 21), date(2026, 4, 17)),
            (date(2026, 4, 22), date(2026, 4, 17)),
            (date(2026, 4, 23), date(2026, 4, 17)),
            (date(2026, 4, 24), date(2026, 4, 17)),
            (date(2026, 4, 25), date(2026, 4, 24)),
            (date(2026, 4, 26), date(2026, 4, 24)),
            (date(2026, 4, 27), date(2026, 4, 24)),
        )
        for inpt, expected in cases:
            self.assertEqual(impl_date.previous_friday(inpt), expected)

    def test_next_friday(self):
        cases = (
            (date(2026, 4, 13), date(2026, 4, 17)),
            (date(2026, 4, 14), date(2026, 4, 17)),
            (date(2026, 4, 15), date(2026, 4, 17)),
            (date(2026, 4, 16), date(2026, 4, 17)),
            (date(2026, 4, 17), date(2026, 4, 24)),
            (date(2026, 4, 18), date(2026, 4, 24)),
            (date(2026, 4, 19), date(2026, 4, 24)),
            (date(2026, 4, 20), date(2026, 4, 24)),
        )
        for inpt, expected in cases:
            self.assertEqual(impl_date.next_friday(inpt), expected)


if __name__ == "__main__":
    unittest.main()
