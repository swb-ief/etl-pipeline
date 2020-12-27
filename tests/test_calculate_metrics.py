import os
import unittest
from datetime import datetime
import pandas as pd
from numpy.testing import assert_array_equal, assert_allclose
import numpy as np
from pandas._testing import assert_frame_equal

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

    def test_calculate_levitt_metric(self):
        data = pd.Series([2, 3, 4, 5, 6, 7])
        expected = np.array([np.nan, 0.40546511, 0.28768207, 0.22314355, 0.18232156,
                             0.15415068])

        result = calculate_levitt_metric(data)

        assert_allclose(expected, result, rtol=1e-04)

    def test_calculate_hospitalizations(self):
        np.random.seed(27)  # would be better to mock this
        data = {
            'delta.confirmed': [2, 3, 4, 5, 6, 7],
            'percentages': [.125, .125, .125, .125, .125, .125],
            'hospitalizations': [0.25, 0.375, 0.5, 0.625, 0.75, 0.875]}
        index = [
            datetime(2020, 10, 2),
            datetime(2020, 10, 3),
            datetime(2020, 10, 4),
            datetime(2020, 10, 5),
            datetime(2020, 10, 6),
            datetime(2020, 10, 7),
        ]
        df = pd.DataFrame(data=data, index=index)

        result = calculate_or_impute_hospitalizations(
            delta_confirmed=df['delta.confirmed'],
            hospitalization_ratios=df['percentages']
        )

        assert_frame_equal(result, df)

    def test_impute_hospitalization_percentages(self):
        np.random.seed(27)
        data = {
            'percentages': [.15, .15, np.nan, np.nan, np.nan, .15],
        }
        index = pd.Series([
            datetime(2020, 10, 2),
            datetime(2020, 10, 3),
            datetime(2020, 10, 4),
            datetime(2020, 10, 5),
            datetime(2020, 10, 6),
            datetime(2020, 10, 7),
        ], name='date')
        df = pd.DataFrame(data=data, index=index)

        expected_ratios = [0.15, 0.15, 0.13702885642075582, 0.1525833496197821, 0.14941589160798718, 0.15]
        result = impute_hospitalization_percentages(df, index)

        df['percentages'] = expected_ratios
        assert_frame_equal(result, df)

    def test_extend_hospitalization_percentages(self):
        np.random.seed(27)
        data = {
            'percentages': [.15, .15, np.nan, np.nan],
        }
        index = pd.Series([
            datetime(2020, 10, 2),
            datetime(2020, 10, 3),
            datetime(2020, 10, 4),
            datetime(2020, 10, 5),
            datetime(2020, 10, 6),
            datetime(2020, 10, 7),
        ], name='date')
        df = pd.DataFrame(data=data, index=index[:4])

        expected_ratios = [0.15, 0.15, 0.13702885642075582, 0.1525833496197821, 0.14941589160798718,
                           0.15472012799518942]
        result = impute_hospitalization_percentages(df, index)

        df_expected = pd.DataFrame({'percentages': expected_ratios}, index=index)
        assert_frame_equal(result, df_expected)

    def test_calculate_metrics(self):
        """ This is a tiny bit of an integration test. since we are using an other method to help us build
        our input """

        sample_df = pd.read_csv(os.path.join(THIS_DIR, 'samples/Dashboard PDF SWB - city_stats.csv'),
                                parse_dates=['date'],
                                index_col=['date'])

        hospitalizations = impute_hospitalization_percentages(
            pd.DataFrame({'percentages': [0.13]}, index=[datetime(2020, 10, 3)]), sample_df.index)

        expected_shape = (905, 38)

        result, _ = update_city_stats(
            start_date=datetime(2020, 4, 20),
            city_stats=sample_df,
            hospitalizations=hospitalizations
        )

        self.assertEqual(expected_shape, result.shape)
