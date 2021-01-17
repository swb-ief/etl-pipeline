import pandas as pd
from numpy import random


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


def _moving_average_grouped(df: pd.DataFrame, group_columns: list[str], target_column: str, window_size) -> pd.Series:
    """
    :remarks: requires pandas 1.2 (there is a breaking api change in 1.x and 1.2)
    """
    assert 'date' in df.index.names

    df_reset = df.reset_index()

    return df_reset \
        .groupby(group_columns) \
        .rolling(window_size, on='date')[target_column] \
        .mean()


def extend_and_impute_metrics(
        raw_metrics: pd.DataFrame,
        hospitalizations: pd.DataFrame,
        grouping_columns: list[str]
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

    # ratios per million
    ratios_needed_for = ['delta.confirmed', 'delta.deceased', 'total.confirmed', 'total.deceased']
    for column in ratios_needed_for:
        df.loc[:, f'{column}.ratio_per_million'] = df[column] / (df['population'] / 1_000_000)

    # moving averages of some of our calculated columns
    for column in ['delta.positivity', 'delta.hospitalized', 'delta.active']:
        df.loc[:, f'MA.21.{column}'] = _moving_average_grouped(df, grouping_columns, column, rolling_window)

    return df.reset_index()
