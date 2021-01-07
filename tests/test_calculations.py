import os
import unittest
from datetime import datetime

from numpy.testing import assert_allclose
from pandas.testing import assert_frame_equal
from backend.metrics.calculations import *
import pandas as pd
import numpy as np

from backend.metrics.calculations import _moving_average_grouped

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class TestCalculateMetrics(unittest.TestCase):
    @staticmethod
    def _build_district_input(measurements, districts, values: list):
        """ Quickly create a df that has all the required columns, and a few districts to test grouping """
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

    def test_calculate_hospitalizations(self):
        np.random.seed(27)  # make tests reproducible, would be better to mock np.random
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

    def test_calculate_hospitalizations_multi_district(self):
        np.random.seed(27)  # make tests reproducible, would be better to mock np.random
        dates = [
            datetime(2020, 10, 2),
            datetime(2020, 10, 3),
            datetime(2020, 10, 4),
            datetime(2020, 10, 5),
            datetime(2020, 10, 6),
            datetime(2020, 10, 7),
        ]

        df_percentages = pd.DataFrame({
            'date': dates,
            'percentages': [.125, .125, .125, .125, .125, .125]
        })

        # * 2 to simulate multi district input
        df_confirmed = pd.DataFrame({
            'date': dates * 2,
            'delta.confirmed': [2, 3, 4, 5, 6, 7] * 2
        })

        result = calculate_hospitalizations(
            delta_confirmed=df_confirmed.set_index('date'),
            hospitalization_ratios=df_percentages
        )

        df_confirmed['hospitalizations'] = [0.25, 0.375, 0.5, 0.625, 0.75, 0.875] * 2
        df_confirmed = df_confirmed.set_index('date').sort_index()
        assert_frame_equal(result, df_confirmed)

    def test_impute_hospitalization_percentages(self):
        np.random.seed(27)  # make tests reproducible, would be better to mock np.random
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

    def test_impute_hospitalization_percentages_multi_city(self):
        np.random.seed(27)  # make tests reproducible, would be better to mock np.random
        dates = [
            datetime(2020, 10, 2),
            datetime(2020, 10, 3),
            datetime(2020, 10, 4),
            datetime(2020, 10, 5),
            datetime(2020, 10, 6),
            datetime(2020, 10, 7),
            datetime(2020, 10, 2),  # duplicate
        ]

        data = {
            'date': dates[:-1],
            'percentages': [.15, .15, np.nan, np.nan, np.nan, .15]
        }
        df_old = pd.DataFrame(data=data)

        df_new_dates = pd.DataFrame(data={'date': dates})

        expected_ratios = [0.15, 0.15, 0.13702885642075582, 0.1525833496197821, 0.14941589160798718, 0.15]
        result = impute_hospitalization_percentages(df_old, df_new_dates['date'])

        df_old['percentages'] = expected_ratios
        assert_frame_equal(result, df_old)

    def test_extend_hospitalization_percentages(self):
        np.random.seed(27)  # make tests reproducible, would be better to mock np.random
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

        input_df = pd.read_csv(os.path.join(THIS_DIR, 'samples/Dashboard PDF SWB - city_stats.csv'),
                               parse_dates=['date'])

        hospitalizations = impute_hospitalization_percentages(
            pd.DataFrame({'date': [datetime(2020, 10, 3)], 'percentages': [0.13]}), input_df['date'])

        expected_shape = (1170, 31)

        result = extend_and_impute_metrics(
            raw_metrics=input_df,
            hospitalizations=hospitalizations,
            grouping_columns=['state', 'district']
        )

        self.assertEqual(expected_shape, result.shape)

    def test__moving_average_grouped(self):
        mean_window = 4
        group_columns = ['state', 'district']

        input_df = self._build_district_input(measurements=5, districts=2, values=[0.3, 0.7])
        df = input_df.set_index(['date', *group_columns])
        df = df.sort_index()

        # the sort is a side effect, however it preserves the indexes so we don't care except when
        # accessing raw values like this.
        expected = np.array([np.nan, np.nan, np.nan, .3, .3, np.nan, np.nan, np.nan, .7, .7])

        result = _moving_average_grouped(df, group_columns, 'delta.tested', mean_window).to_numpy()

        assert_allclose(expected, result)

    def test_moving_average_calculations(self):

        input_df = self._build_district_input(measurements=25, districts=2, values=[0.3, 0.7])

        hospitalizations = impute_hospitalization_percentages(
            pd.DataFrame({'date': [datetime(2000, 1, 1)], 'percentages': [0.13]}),
            input_df['date'])

        # mean window is 21 so expect 20 np.nan's
        expected_means = np.array(
            [np.nan] * 20 + [0.3] * 5 +
            [np.nan] * 20 + [0.7] * 5)

        raw_result = extend_and_impute_metrics(
            raw_metrics=input_df,
            hospitalizations=hospitalizations,
            grouping_columns=['state', 'district']
        )

        ma_columns = [column for column in raw_result if column.startswith('MA.21')]
        self.assertGreater(len(ma_columns), 0)

        for column in ma_columns:
            if 'positivity' in column or 'hospitalized' in column or 'active' in column:
                continue  # we got a separate test for these

            result = raw_result[column].to_numpy()
            assert_allclose(
                expected_means,
                result,
                err_msg=f'Column {column} does not have the expected values')

    def test_positivity(self):
        input_df = self._build_district_input(measurements=25, districts=2, values=[0.3, 0.7])
        hospitalizations = impute_hospitalization_percentages(
            pd.DataFrame({'date': [datetime(1900, 1, 1)], 'percentages': [0.13]}),
            input_df['date'])

        fixed_tested = 53

        input_df['delta.confirmed'] = [x for x in range(len(input_df))]
        input_df['delta.tested'] = fixed_tested

        expected = np.array([x / fixed_tested * 100 for x in range(len(input_df))])
        input_df['expected'] = expected  # storing it in the dataframe because extend will also apply a sort

        raw_result = extend_and_impute_metrics(
            raw_metrics=input_df,
            hospitalizations=hospitalizations,
            grouping_columns=['state', 'district']
        )

        result = raw_result['delta.positivity'].to_numpy()

        assert_allclose(raw_result['expected'].to_numpy(), result)

    def test_delta_percent_case_growth(self):
        input_df = self._build_district_input(measurements=25, districts=1, values=[.3])
        hospitalizations = impute_hospitalization_percentages(
            pd.DataFrame({'date': [datetime(1900, 1, 1)], 'percentages': [0.13]}),
            input_df['date'])

        input_df['delta.confirmed'] = [x for x in range(len(input_df))]

        expected = input_df['delta.confirmed'].pct_change()
        input_df['expected'] = expected  # storing it in the dataframe because extend will also apply a sort

        raw_result = extend_and_impute_metrics(
            raw_metrics=input_df,
            hospitalizations=hospitalizations,
            grouping_columns=['state', 'district']
        )

        result = raw_result['delta.percent.case.growth'].to_numpy()

        assert_allclose(raw_result['expected'].to_numpy(), result)

    def test_delta_hospitalized(self):
        np.random.seed(27)  # make tests reproducible, would be better to mock np.random
        input_df = self._build_district_input(measurements=3, districts=2, values=[10, 20])
        hospitalizations = impute_hospitalization_percentages(
            pd.DataFrame({'date': [datetime(1900, 1, 1)], 'percentages': [0.13]}),
            input_df['date'])
        expected = np.array([1.3, 1.525833, 1.547201, 2.740577, 2.988318, 2.706705])

        raw_result = extend_and_impute_metrics(
            raw_metrics=input_df,
            hospitalizations=hospitalizations,
            grouping_columns=['state', 'district']
        )
        result = raw_result['delta.hospitalized'].values

        assert_allclose(expected, result, rtol=1e-04)

    def test_delta_active(self):
        value_city_1 = 10
        value_city_2 = 20
        measurements = 3

        # confirmed - deceased - recovered - other
        expected_city_1 = value_city_1 - value_city_1 - value_city_1 - value_city_1
        expected_city_2 = value_city_2 - value_city_2 - value_city_2 - value_city_2

        input_df = self._build_district_input(measurements=measurements, districts=2,
                                              values=[value_city_1, value_city_2])
        hospitalizations = impute_hospitalization_percentages(
            pd.DataFrame({'date': [datetime(1900, 1, 1)], 'percentages': [0.13]}),
            input_df['date'])
        expected = np.array([expected_city_1] * measurements + [expected_city_2] * measurements)

        raw_result = extend_and_impute_metrics(
            raw_metrics=input_df,
            hospitalizations=hospitalizations,
            grouping_columns=['state', 'district']
        )
        result = raw_result['delta.active'].values

        assert_allclose(expected, result, rtol=1e-04)
