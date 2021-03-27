import pandas as pd
from numpy import random
import numpy as np
from typing import List

def impute_hospitalization_percentages(current_hospitalizations: pd.DataFrame, expected_dates: pd.Series):
    """ we impute a random ratio for each day, it is needed to calculate 'hospitalized'

    """
    assert expected_dates.name == 'date'
    assert 'date' in current_hospitalizations.columns
    assert 'percentages' in current_hospitalizations.columns

    # they can have duplicates (multi city/ward/etc..)
    expected_dates = expected_dates.drop_duplicates()
    current_hospitalizations = current_hospitalizations.set_index('date')

    ratio_column = 'percentages'  # incorrectly named percentages but is actually a value between 0 and 1
    df = expected_dates.to_frame().set_index('date')
    df = df.merge(current_hospitalizations, how='left', left_index=True, right_index=True)
    df[ratio_column] = df[ratio_column].apply(lambda x: random.uniform(0.12, 0.16) if pd.isnull(x) else x)
    return df.reset_index()


def calculate_hospitalizations(
        delta_confirmed: pd.DataFrame,
        hospitalization_ratios: pd.DataFrame) -> pd.DataFrame:
    """ :return: merged Dataframe with an extra column 'hospitalizations' with delta confirmed * ratio
            and if there is no ratio it will randomly estimate it between .12 and .16
        :remark: Has the side effect of sorting the date index... due to the join.
        """

    ratio_column = 'percentages'  # incorrectly named percentages but is actually a value between 0 and 1
    assert 'date' in delta_confirmed.index.names
    assert 'delta.confirmed' in delta_confirmed.columns
    assert 'date' in hospitalization_ratios.columns, 'Known issue, if an empty sheet was created delete it first'
    assert ratio_column in hospitalization_ratios.columns

    indexed_ratios = hospitalization_ratios.set_index('date')

    df = delta_confirmed.join(indexed_ratios, how='left', sort=False)

    df['hospitalizations'] = df['delta.confirmed'] * df[ratio_column]
    columns_to_drop = list(indexed_ratios.columns)
    df = df.drop(columns=columns_to_drop)
    return df


def _moving_average_grouped(df: pd.DataFrame, group_columns: List[str], target_column: str, window_size) -> pd.Series:
    """
    :remarks: requires pandas 1.2 (there is a breaking api change in 1.x and 1.2)
    """
    assert 'date' in df.index.names

    df_reset = df.reset_index()

    return df_reset \
        .groupby(group_columns) \
        .rolling(window_size, on='date')[target_column] \
        .mean()


def fourteen_day_avg_ratio(values: pd.Series) -> pd.Series:
    """ Calculates the ratio of the 14 day average compared to the previous 14 day average
        When not enough data is available it starts averaging if there are at least 7 days
        Calculation: on day 28 = (day 1 day - 14 day avg) / (day 15 -  day 28 day avg)
        nan values are ignored, the avg is calculated over the other values
        :param values: the values over which to calculate the ratio
        :return:  (sliding window)
    """

    series = values.copy()

    dates = values.index.get_level_values('date').values
    other_index_names = values.index.droplevel('date').names
    other_index_values = values.index.droplevel('date').unique().tolist()

    assert len(other_index_values) == 1, "Should be called with a grouping by location (state/district/ward)"

    # other_index_values can be ['state'] or [('state', 'district')], etc.. notice the tuple
    # we just want to iterate over these so in the second case we just pick the tuple
    other_index_values = other_index_values if isinstance(other_index_values[0], str) else other_index_values[0]

    series.index = series.index.droplevel(other_index_names)

    idx = pd.date_range(min(dates), max(dates), name='date')
    reindexed = series.reindex(idx)  # This also sorts the dates

    two_week_avg = reindexed.rolling(window='14D', min_periods=7).mean()
    prev_two_week_avg = reindexed.shift(periods=14, freq='D').rolling(window='14D', min_periods=7).mean()
    ratio = two_week_avg / prev_two_week_avg

    df = ratio.reset_index()
    for i, column in enumerate(other_index_names):
        df[column] = other_index_values[i]
    df = df.set_index([*other_index_names, 'date'])

    just_for_original_values = df.loc[values.index]

    return just_for_original_values[values.name]


def extend_and_impute_metrics(
        raw_metrics: pd.DataFrame,
        hospitalizations: pd.DataFrame,
        grouping_columns: List[str]
) -> pd.DataFrame:
    """
    :returns: extended and imputed metrics
    """

    # we want to work with a nicely date sorted index
    # the date ordering is important for the rolling means
    # ordering by the other parts is for human readers
    df = raw_metrics \
        .set_index([*grouping_columns, 'date']) \
        .sort_index()

    measurements = ['tested', 'confirmed', 'deceased', 'recovered', 'other']
    rolling_window = 21

    for group in ['delta', 'total']:
        for measurement in measurements:
            df[f'{group}.{measurement}'] = df[f'{group}.{measurement}'].fillna(value=0)

            df.loc[:, f'MA.21.{group}.{measurement}'] = _moving_average_grouped(df, grouping_columns,
                                                                                f'{group}.{measurement}',
                                                                                rolling_window)

    # TPR% per day
    df.loc[:, "delta.positivity"] = (df["delta.confirmed"] / df["delta.tested"]) * 100.0

    # daily percent case growth
    df.loc[:, "delta.percent.case.growth"] = df.groupby(grouping_columns)["delta.confirmed"].pct_change()

    hospitalization_ratios_updated = calculate_hospitalizations(df, hospitalizations)

    df["delta.hospitalized"] = hospitalization_ratios_updated['hospitalizations'].values

    # total hospitalizations
    df.loc[:, "total.hospitalized"] = df.groupby(grouping_columns)["delta.hospitalized"].cumsum()

    # active cases by day
    df.loc[:, "delta.active"] = (
            df["total.confirmed"]
            - df["total.deceased"]
            - df["total.recovered"]
            - df["delta.other"]
    )

    df['total.confirmed.14_day_ratio'] = df.groupby(grouping_columns)["delta.hospitalized"].apply(
        fourteen_day_avg_ratio)

    # ratios per million
    ratios_needed_for = ['delta.confirmed', 'delta.deceased', 'total.confirmed', 'total.deceased']
    for column in ratios_needed_for:
        df.loc[:, f'{column}.ratio_per_million'] = df[column] / (df['population'] / 1_000_000)

    # moving averages of some of our calculated columns
    for column in ['delta.positivity', 'delta.hospitalized', 'delta.active']:
        df.loc[:, f'MA.21.{column}'] = _moving_average_grouped(df, grouping_columns, column, rolling_window)

    return df.reset_index()
