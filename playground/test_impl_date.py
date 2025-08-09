# automatically generated on 2025-05-26 12:22

from datetime import date
import unittest

import impl_date


class TestImpl_Date(unittest.TestCase):

    def test_prev_day(self):
        res = impl_date.prev_day(date(2025, 5, 26))
        exp = date(2025, 5, 25)
        self.assertEqual(res, exp)

    def test_next_day(self):
        res = impl_date.next_day(date(2025, 5, 26))
        exp = date(2025, 5, 27)
        self.assertEqual(res, exp)

    def test_prev_week(self):
        res = impl_date.prev_week(date(2025, 5, 21))
        exp = date(2025, 5, 14)
        self.assertEqual(res, exp)

    def test_next_week(self):
        res = impl_date.next_week(date(2025, 5, 21))
        exp = date(2025, 5, 28)
        self.assertEqual(res, exp)

    def test_prev_month(self):
        res = impl_date.prev_month(date(2025, 5, 1))
        exp = date(2025, 4, 1)
        self.assertEqual(res, exp)

        res = impl_date.prev_month(date(2025, 5, 31))
        exp = date(2025, 4, 30)
        self.assertEqual(res, exp)

        res = impl_date.prev_month(date(2025, 1, 1))
        exp = date(2024, 12, 1)
        self.assertEqual(res, exp)

        res = impl_date.prev_month(date(2025, 5, 1))
        exp = date(2025, 4, 1)
        self.assertEqual(res, exp)

        res = impl_date.prev_month(date(2025, 3, 31))
        exp = date(2025, 2, 28)
        self.assertEqual(res, exp)

        res = impl_date.prev_month(date(2024, 3, 31))
        exp = date(2024, 2, 29)
        self.assertEqual(res, exp)

    def test_next_month(self):
        res = impl_date.next_month(date(2025, 4, 1))
        exp = date(2025, 5, 1)
        self.assertEqual(res, exp)

        res = impl_date.next_month(date(2025, 4, 30))
        exp = date(2025, 5, 30)
        self.assertEqual(res, exp)

        res = impl_date.next_month(date(2024, 12, 1))
        exp = date(2025, 1, 1)
        self.assertEqual(res, exp)

        res = impl_date.next_month(date(2025, 4, 1))
        exp = date(2025, 5, 1)
        self.assertEqual(res, exp)

        res = impl_date.next_month(date(2025, 1, 31))
        exp = date(2025, 2, 28)
        self.assertEqual(res, exp)

        res = impl_date.next_month(date(2024, 1, 31))
        exp = date(2024, 2, 29)
        self.assertEqual(res, exp)

    def test_prev_year(self):
        res = impl_date.prev_year(date(2025, 2, 28))
        exp = date(2024, 2, 28)
        self.assertEqual(res, exp)

        res = impl_date.prev_year(date(2024, 2, 29))
        exp = date(2023, 2, 28)
        self.assertEqual(res, exp)

    def test_next_year(self):
        res = impl_date.next_year(date(2023, 2, 28))
        exp = date(2024, 2, 28)
        self.assertEqual(res, exp)

        res = impl_date.next_year(date(2024, 2, 29))
        exp = date(2025, 2, 28)
        self.assertEqual(res, exp)

    def test_quarter(self):
        res = impl_date.quarter(date(2024, 1, 1))
        exp = 1
        self.assertEqual(res, exp)

        res = impl_date.quarter(date(2024, 2, 1))
        exp = 1
        self.assertEqual(res, exp)

        res = impl_date.quarter(date(2024, 3, 1))
        exp = 1
        self.assertEqual(res, exp)

        res = impl_date.quarter(date(2024, 4, 1))
        exp = 2
        self.assertEqual(res, exp)

        res = impl_date.quarter(date(2024, 5, 1))
        exp = 2
        self.assertEqual(res, exp)

        res = impl_date.quarter(date(2024, 6, 1))
        exp = 2
        self.assertEqual(res, exp)

        res = impl_date.quarter(date(2024, 7, 1))
        exp = 3
        self.assertEqual(res, exp)

        res = impl_date.quarter(date(2024, 8, 1))
        exp = 3
        self.assertEqual(res, exp)

        res = impl_date.quarter(date(2024, 9, 1))
        exp = 3
        self.assertEqual(res, exp)

        res = impl_date.quarter(date(2024, 10, 1))
        exp = 4
        self.assertEqual(res, exp)

        res = impl_date.quarter(date(2024, 11, 1))
        exp = 4
        self.assertEqual(res, exp)

        res = impl_date.quarter(date(2024, 12, 1))
        exp = 4
        self.assertEqual(res, exp)

    def test_semester(self):
        pass

    def test_start_of_month(self):
        pass

    def test_end_of_month(self):
        res = impl_date.end_of_month(date(2025, 1, 1))
        exp = date(2025, 1, 31)
        self.assertEqual(res, exp)

        res = impl_date.end_of_month(date(2025, 4, 1))
        exp = date(2025, 4, 30)
        self.assertEqual(res, exp)

        res = impl_date.end_of_month(date(2024, 2, 1))
        exp = date(2024, 2, 29)
        self.assertEqual(res, exp)

        res = impl_date.end_of_month(date(2025, 2, 1))
        exp = date(2025, 2, 28)
        self.assertEqual(res, exp)

    def test_start_of_year(self):
        pass

    def test_end_of_year(self):
        pass

    def test_start_of_week(self):
        # monday
        res = impl_date.start_of_week(date(2025, 5, 26))
        exp = date(2025, 5, 26)
        self.assertEqual(res, exp)

        res = impl_date.start_of_week(date(2025, 5, 27))
        exp = date(2025, 5, 26)
        self.assertEqual(res, exp)

        res = impl_date.start_of_week(date(2025, 5, 28))
        exp = date(2025, 5, 26)
        self.assertEqual(res, exp)

        res = impl_date.start_of_week(date(2025, 5, 29))
        exp = date(2025, 5, 26)
        self.assertEqual(res, exp)

        res = impl_date.start_of_week(date(2025, 5, 30))
        exp = date(2025, 5, 26)
        self.assertEqual(res, exp)

        res = impl_date.start_of_week(date(2025, 5, 31))
        exp = date(2025, 5, 26)
        self.assertEqual(res, exp)

        res = impl_date.start_of_week(date(2025, 6, 1))
        exp = date(2025, 5, 26)
        self.assertEqual(res, exp)

    def test_end_of_week(self):
        # monday
        res = impl_date.end_of_week(date(2025, 5, 26))
        exp = date(2025, 6, 1)
        self.assertEqual(res, exp)

        res = impl_date.end_of_week(date(2025, 5, 27))
        exp = date(2025, 6, 1)
        self.assertEqual(res, exp)

        res = impl_date.end_of_week(date(2025, 5, 28))
        exp = date(2025, 6, 1)
        self.assertEqual(res, exp)

        res = impl_date.end_of_week(date(2025, 5, 29))
        exp = date(2025, 6, 1)
        self.assertEqual(res, exp)

        res = impl_date.end_of_week(date(2025, 5, 30))
        exp = date(2025, 6, 1)
        self.assertEqual(res, exp)

        res = impl_date.end_of_week(date(2025, 5, 31))
        exp = date(2025, 6, 1)
        self.assertEqual(res, exp)

        res = impl_date.end_of_week(date(2025, 6, 1))
        exp = date(2025, 6, 1)
        self.assertEqual(res, exp)


if __name__ == "__main__":
    unittest.main()

