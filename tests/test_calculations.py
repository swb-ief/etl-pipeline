import os
import unittest
from datetime import datetime

import pytest
from numpy.testing import assert_allclose
from pandas.testing import assert_frame_equal
from backend.metrics.calculations import *
from backend.metrics.calculations import _moving_average_grouped
import pandas as pd
import numpy as np

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class TestCalculateMetrics(unittest.TestCase):
    @staticmethod
    def _build_district_input(measurements, districts, values: list):
        district_names = [f'city_{x}' for x in range(districts)]

        data = {
            'date': [datetime(1900 + x, 1, 1) for x in range(measurements * districts)],
            'state': ['my_state'] * measurements * districts,
            'district': district_names * measurements,
        }

        for group in ['delta', 'total']:
            for measurement in ['tested', 'confirmed', 'deceased', 'recovered', 'other']:
                raw_measurements = values * measurements
                data[f'{group}.{measurement}'] = raw_measurements

        df = pd.DataFrame(data)
        return df

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

        result = calculate_hospitalizations(
            delta_confirmed=df.drop(columns=['hospitalizations', 'percentages']).set_index('date'),
            hospitalization_ratios=df.drop(columns=['hospitalizations', 'delta.confirmed'])
        )

        assert_frame_equal(result, df.drop(columns=['percentages']).set_index('date'))

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

        expected_shape = (1170, 32)

        result = extend_and_impute_metrics(
            raw_metrics=sample_df,
            hospitalizations=hospitalizations,
            grouping_columns=['state', 'district']
        )

        self.assertEqual(expected_shape, result.shape)

    def test__moving_average_grouped(self):
        expected = [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, .3, .7, .3, .7]
        mean_window = 4
        measurements = 5
        cities = 2
        group_columns = ['state', 'district']

        data = {
            'date': [datetime(1900 + x, 1, 1) for x in range(measurements * cities)],
            'state': ['my_state'] * measurements * cities,
            'district': ['city_1', 'city_2'] * measurements,
        }

        for group in ['delta']:
            for measurement in ['tested']:
                raw_measurements = [0.3, 0.7] * measurements
                data[f'{group}.{measurement}'] = raw_measurements

        sample_df = pd.DataFrame(data)
        df = sample_df.set_index(['date', *group_columns])
        df = df.sort_index()

        df['moving average'] = _moving_average_grouped(df, group_columns, 'delta.tested', mean_window)

        data['moving average'] = expected
        df_expected = pd.DataFrame(data).set_index(['date', *group_columns]).sort_index()

        assert_frame_equal(df, df_expected)

    def test_mean_calculations(self):

        sample_df = self._build_district_input(measurements=25, districts=2, values=[0.3, 0.7])

        hospitalizations = impute_hospitalization_percentages(
            pd.DataFrame({'date': [datetime(1900, 1, 1)], 'percentages': [0.13]}),
            sample_df['date'])

        # mean window is 21 so expect 20 np.nan's
        expected_means = np.array(
            [np.nan] * 20 + [0.3] * 5 +
            [np.nan] * 20 + [0.7] * 5)

        raw_result = extend_and_impute_metrics(
            raw_metrics=sample_df,
            hospitalizations=hospitalizations,
            grouping_columns=['state', 'district']
        )

        ma_columns = [column for column in raw_result if column.startswith('MA.21')]
        self.assertGreater(len(ma_columns), 0)

        for column in ma_columns:
            if 'positivity' in column or 'hospitalized' in column or 'active' in column:
                continue  # we got a seperate test for these

            result = raw_result[column].to_numpy()
            assert_allclose(
                expected_means,
                result,
                err_msg=f'Column {column} does not have the expected values')

    def test_positivity(self):
        input = self._build_district_input(measurements=25, districts=2, values=[0.3, 0.7])
        hospitalizations = impute_hospitalization_percentages(
            pd.DataFrame({'date': [datetime(1900, 1, 1)], 'percentages': [0.13]}),
            input['date'])

        fixed_tested = 53

        input['delta.confirmed'] = [x for x in range(len(input))]
        input['delta.tested'] = fixed_tested

        expected = np.array([x / fixed_tested * 100 for x in range(len(input))])
        input['expected'] = expected  # storing it in the dataframe becuase extend will also apply a sort

        raw_result = extend_and_impute_metrics(
            raw_metrics=input,
            hospitalizations=hospitalizations,
            grouping_columns=['state', 'district']
        )

        result = raw_result['delta.positivity'].to_numpy()

        assert_allclose(raw_result['expected'].to_numpy(), result)

    def test_percent_case_growth(self):
        input_df = self._build_district_input(measurements=25, districts=1, values=[.3])
        hospitalizations = impute_hospitalization_percentages(
            pd.DataFrame({'date': [datetime(1900, 1, 1)], 'percentages': [0.13]}),
            input_df['date'])

        input_df['delta.confirmed'] = [x for x in range(len(input_df))]

        expected = input_df['delta.confirmed'].pct_change()
        input_df['expected'] = expected  # storing it in the dataframe becuase extend will also apply a sort

        raw_result = extend_and_impute_metrics(
            raw_metrics=input_df,
            hospitalizations=hospitalizations,
            grouping_columns=['state', 'district']
        )

        result = raw_result['delta.percent.case.growth'].to_numpy()

        assert_allclose(raw_result['expected'].to_numpy(), result)

    @pytest.mark.skip("Not yet implemented")
    def test_hospitalized(self):
        self.assertTrue(False)

    @pytest.mark.skip("Not yet implemented")
    def test_active(self):
        self.assertTrue(False)
