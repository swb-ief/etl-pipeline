import os
import unittest
from datetime import datetime
import pandas as pd
from numpy.testing import assert_array_equal, assert_allclose
import numpy as np
from backend.metrics.calculations import *

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class TestCalculateMetrics(unittest.TestCase):
    def test_cubic_spline(self):
        # arrange
        column_data = pd.Series([1, 2, 3])
        expected = np.ndarray((3,), buffer=np.array([1., 2., 3.]))

        # act
        result = cubic_spline(column_data)

        # assert
        assert_array_equal(expected, result)

    def test_calculate_metrics(self):
        sample_df = pd.read_csv(os.path.join(THIS_DIR, 'samples/Dashboard PDF SWB - city_stats.csv'),
                                parse_dates=['date'],
                                index_col=['date'])

        expected_shape = (905, 38)

        result = calculate_all_metrics(
            start_date=datetime(2020, 4, 20),
            city_stats=sample_df,
            hospitalizations=None,
        )

        self.assertEqual(expected_shape, result.shape)

    def test_impute_column(self):
        data = pd.Series([2, 3, 4, 5, 6])
        expected = np.array([2., 3., 4., 5., 6.])

        result = impute_column(data)
        assert_array_equal(expected, result)

    def test_impute_missing_(self):
        data = pd.Series([2, 3, 4, np.nan, 14, 7])
        expected = np.array([2., 3., 4., 7., 7., 7.])

        result = impute_column(data)
        assert_array_equal(expected, result)

    def test_impute_missing_with_rounding_error(self):
        # we would have prefered 11 to be split in 5. and 6. but our current implementation is not capable of that
        data = pd.Series([2, 3, 4, np.nan, 11, 7])
        expected = np.array([2., 3., 4., 6., 6., 7.])

        result = impute_column(data)
        assert_array_equal(expected, result)

    def test_impute_hospitalizations(self):
        np.random.seed(27)  # would be better to mock this
        data = pd.Series([2, 3, 4, 5, 6, 7])
        expected = np.array([0.274058, 0.45775, 0.597664, 0.773601, 0.812011, 1.114248])

        result = calculate_or_impute_hospitalizations(
            delta_confirmed=data
        )

        assert_allclose(result, expected, rtol=1e-04)

    def test_calculate_hospitalizations(self):
        np.random.seed(27)  # would be better to mock this
        data = pd.Series([2, 3, 4, 5, 6, 7])
        ratios = pd.Series([.125, .125, .125, .125, .125, .125])
        expected = np.array([0.25, 0.375, 0.5, 0.625, 0.75, 0.875])

        result = calculate_or_impute_hospitalizations(
            delta_confirmed=data,
            hospitalization_ratios=ratios
        )

        assert_allclose(result, expected, rtol=1e-04)

    def test_calculate_or_impute_hospitalizations(self):
        np.random.seed(27)  # would be better to mock this
        data = pd.Series([2, 3, 4, 5, 6, 7])
        ratios = pd.Series([.125, .125, np.nan, .125, .125, .125])
        expected = np.array([0.25, 0.375, 0.597664, 0.625, 0.75, 0.875])

        result = calculate_or_impute_hospitalizations(
            delta_confirmed=data,
            hospitalization_ratios=ratios
        )

        assert_allclose(result, expected, rtol=1e-04)

    def test_calculate_levitt_metric(self):
        data = pd.Series([2, 3, 4, 5, 6, 7])
        expected = np.array([np.nan, 0.40546511, 0.28768207, 0.22314355, 0.18232156,
                             0.15415068])

        result = calculate_levitt_metric(data)

        assert_allclose(expected, result, rtol=1e-04)
