import os
import unittest
from numpy.testing import assert_array_equal, assert_allclose
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
            'date': [
                datetime(2020, 10, 2),
                datetime(2020, 10, 3),
                datetime(2020, 10, 4),
                datetime(2020, 10, 5),
                datetime(2020, 10, 6),
                datetime(2020, 10, 7),
            ],
            'delta.confirmed': [2, 3, 4, 5, 6, 7],
            'hospitalizations': [0.25, 0.375, 0.5, 0.625, 0.75, 0.875],
            'percentages': [.125, .125, .125, .125, .125, .125]
        }
        df = pd.DataFrame(data=data)

        result = calculate_or_impute_hospitalizations(
            delta_confirmed=df.drop(columns=['hospitalizations', 'percentages']),
            hospitalization_ratios=df.drop(columns=['hospitalizations', 'delta.confirmed'])
        )

        assert_frame_equal(result, df.drop(columns=['percentages']))

    def test_impute_hospitalization_percentages(self):
        np.random.seed(27)
        data = {
            'date': [
                datetime(2020, 10, 2),
                datetime(2020, 10, 3),
                datetime(2020, 10, 4),
                datetime(2020, 10, 5),
                datetime(2020, 10, 6),
                datetime(2020, 10, 7),
            ],
            'percentages': [.15, .15, np.nan, np.nan, np.nan, .15]
        }
        df = pd.DataFrame(data=data)

        expected_ratios = [0.15, 0.15, 0.13702885642075582, 0.1525833496197821, 0.14941589160798718, 0.15]
        result = impute_hospitalization_percentages(df, df['date'])

        df['percentages'] = expected_ratios
        assert_frame_equal(result, df)

    def test_extend_hospitalization_percentages(self):
        np.random.seed(27)
        data = {
            'date': [
                datetime(2020, 10, 2),
                datetime(2020, 10, 3),
                datetime(2020, 10, 4),
                datetime(2020, 10, 5),
                datetime(2020, 10, 6),
                datetime(2020, 10, 7),
            ],
            'percentages': [.15, .15, np.nan, np.nan, np.nan, np.nan],
        }
        df = pd.DataFrame(data={'date': data['date'][:4], 'percentages': data['percentages'][:4]})

        expected_ratios = [0.15, 0.15, 0.13702885642075582, 0.1525833496197821, 0.14941589160798718,
                           0.15472012799518942]
        result = impute_hospitalization_percentages(df, pd.Series(data['date'], name='date'))

        df_expected = pd.DataFrame({'date': data['date'], 'percentages': expected_ratios})
        assert_frame_equal(result, df_expected)

    def test_calculate_metrics(self):
        """ This is a tiny bit of an integration test. since we are using an other method to help us build
        our input """

        sample_df = pd.read_csv(os.path.join(THIS_DIR, 'samples/Dashboard PDF SWB - city_stats.csv'),
                                parse_dates=['date'])

        hospitalizations = impute_hospitalization_percentages(
            pd.DataFrame({'date': [datetime(2020, 10, 3)], 'percentages': [0.13]}), sample_df['date'])

        expected_shape = (1170, 39)

        result = impute_metrics(
            raw_metrics=sample_df,
            hospitalizations=hospitalizations
        )

        self.assertEqual(expected_shape, result.shape)
