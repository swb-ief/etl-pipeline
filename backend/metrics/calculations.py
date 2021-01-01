import numpy as np
import pandas as pd
from numpy import random
from scipy.interpolate import CubicSpline


def calculate_levitt_metric(column: pd.Series) -> pd.Series:
    """ calculate and return levitt metric for a column
    """
    shifted = column.shift(1)
    return np.log(column / shifted)


def impute_hospitalization_percentages(current_hospitalizations: pd.DataFrame, expected_dates: pd.Series):
    """ we impute a random ratio for each day, it is needed to calculate 'hospitalized'

    """
    assert expected_dates.name == 'date'
    assert 'date' in current_hospitalizations.columns
    assert 'percentages' in current_hospitalizations.columns

    # they can have duplicates (multi city/ward/etc..)
    expected_dates = expected_dates.drop_duplicates()
    current_hospitalizations = current_hospitalizations.set_index('date')

    ratio_column = 'percentages'  # incorrectly named percentages but is actualy a value between 0 and 1
    df = expected_dates.to_frame().set_index('date')
    df = df.merge(current_hospitalizations, how='left', left_index=True, right_index=True)
    df[ratio_column] = df[ratio_column].apply(lambda x: random.uniform(0.12, 0.16) if pd.isnull(x) else x)
    return df.reset_index()


def calculate_hospitalizations(
        delta_confirmed: pd.DataFrame,
        hospitalization_ratios: pd.DataFrame) -> pd.DataFrame:
    """ :return: merged Dataframe with an extra column 'hospitalizations' with delta confirmed * ratio
            and if there is no ratio it will randomly estimate it between .12 and .16"""

    ratio_column = 'percentages'  # incorrectly named percentages but is actualy a value between 0 and 1
    assert 'date' in delta_confirmed.index.names
    assert 'delta.confirmed' in delta_confirmed.columns
    assert 'date' in hospitalization_ratios.columns
    assert ratio_column in hospitalization_ratios.columns

    indexed_ratios = hospitalization_ratios.set_index('date')

    df = delta_confirmed.join(indexed_ratios, how='left')

    df['hospitalizations'] = df['delta.confirmed'] * df[ratio_column]
    columns_to_drop = list(indexed_ratios.columns)
    df = df.drop(columns=columns_to_drop)
    return df


def _moving_average_grouped(df: pd.DataFrame, group_columns: list[str], target_column: str, window_size) -> pd.Series:
    """
    :remarks: Does not sort the data
    """
    group_index_count = len(group_columns)
    indexes_to_drop = list(range(group_index_count))
    return df.groupby(level=group_columns)[target_column] \
        .rolling(window_size) \
        .mean() \
        .reset_index(level=indexes_to_drop, drop=True)


def extend_and_impute_metrics(
        raw_metrics: pd.DataFrame,
        hospitalizations: pd.DataFrame,
        grouping_columns: list[str]
) -> (pd.DataFrame, pd.DataFrame):
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
            # TODO: argument for fillna() still being considered not imputed?
            df[f'{group}.{measurement}'] = df[f'{group}.{measurement}'].fillna(value=0)

            df.loc[:, f'MA.21.{group}.{measurement}'] = _moving_average_grouped(df, grouping_columns,
                                                                                f'{group}.{measurement}',
                                                                                rolling_window)

    # generate Levitt Metric
    # TODO needs to be grouped first
    df.loc[:, "total.deceased.levitt"] = calculate_levitt_metric(df["total.deceased"])

    # TPR% per day
    df.loc[:, "delta.positivity"] = (df["delta.confirmed"] / df["delta.tested"]) * 100.0

    # 21-Day MA of TPR%
    df.loc[:, "MA.21.delta.positivity"] = _moving_average_grouped(df, grouping_columns, 'delta.positivity',
                                                                  rolling_window)

    # daily percent case growth
    df.loc[:, "delta.percent.case.growth"] = df.groupby(
        grouping_columns)["delta.confirmed"].pct_change()

    hospitalization_ratios_updated = calculate_hospitalizations(df,
                                                                hospitalizations)

    df["delta.hospitalized"] = hospitalization_ratios_updated['hospitalizations'].values

    # total hospitalizations
    df.loc[:, "total.hospitalized"] = df.groupby(
        grouping_columns)["delta.hospitalized"].cumsum()

    # active cases by day
    df.loc[:, "delta.active"] = (
            df["total.confirmed"]
            - df["total.deceased"]
            - df["total.recovered"]
            - df["delta.other"]
    )

    return df.reset_index()
