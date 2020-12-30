import numpy as np
import pandas as pd
from numpy import random
from scipy.interpolate import CubicSpline


def impute_column(column: pd.Series) -> np.ndarray:
    """
    Uniformly distributes the accumulated figures over days with zero values.

    Input- Column that needs to be imputed
    Output- Numpy imputed column

    For e.g if there are non tested reported on Day1,2 and 3 and there are 400 on day 4, then imputation
    spread it out to 100 each on Day1,2,3,4

    Issue identified- Since the tested/decesed etc has to be whole numbers hence we are loosing some numbers in distribution.
    For e.g If on Day 1,2,3 no testing reported and on Day 4,  27 tested then This will spread out as Day1-6 tested Day2- 6 tested
    Day 3-6 tested Day 4-6 tested, which is equal to 24. Hnece we will loose out 3 tested.
    Over a period of time it cumulates to large numbers.
    """
    column = pd.Series(np.where(column.values < 0, np.NaN, column))
    pre_nan = (
            column.isnull()
            .astype(int)
            .groupby(column.notnull().astype(int).cumsum())
            .cumsum()
            .shift(1)
            + 1
    )
    avgs = np.round(column.values / pre_nan)
    avgs = avgs.bfill()
    avgs = np.where(np.logical_or(np.isnan(column), avgs < column), avgs, column)
    avgs = pd.Series(avgs)

    # TODO: Is this here in case avgs still has np.nan? If so make a test cases for this. Else remove this line
    # and skip the conversion to series and back to numpy
    avgs.fillna(np.round(avgs.mean()), inplace=True)

    return avgs.to_numpy()


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


def calculate_or_impute_hospitalizations(
        delta_confirmed: pd.DataFrame,
        hospitalization_ratios: pd.DataFrame) -> pd.DataFrame:
    """ :return: merged Dataframe with an extra column 'hospitalizations' with delta confirmed * ratio
            and if there is no ratio it will randomly estimate it between .12 and .16"""

    ratio_column = 'percentages'  # incorrectly named percentages but is actualy a value between 0 and 1
    assert 'date' in delta_confirmed.columns
    assert 'delta.confirmed' in delta_confirmed.columns
    assert 'date' in hospitalization_ratios.columns
    assert ratio_column in hospitalization_ratios.columns

    df = delta_confirmed.merge(hospitalization_ratios, how='left', left_on='date', right_on='date')

    df['hospitalizations'] = df['delta.confirmed'] * df[ratio_column]
    columns_to_drop = list(hospitalization_ratios.columns)
    columns_to_drop.remove('date')
    df = df.drop(columns=columns_to_drop)
    return df


def cubic_spline(column: pd.Series) -> np.array:
    cs = CubicSpline(range(len(column)), column)
    return cs(range(len(column)))


def extend_and_impute_metrics(
        raw_metrics: pd.DataFrame,
        hospitalizations: pd.DataFrame = None,
) -> (pd.DataFrame, pd.DataFrame):
    """
    :returns: imputed metrics
    """
    measurements = ['tested', 'confirmed', 'deceased', 'recovered', 'other']
    mean_window = 21

    for measurement in measurements:
        # TODO: argument for fillna still being considered not imputed
        raw_metrics.loc[:, f'delta.{measurement}.non.impu'] = raw_metrics[f'delta.{measurement}'].fillna(value=0)
        raw_metrics.loc[:, f'total.{measurement}.non.impu'] = raw_metrics[f'delta.{measurement}.non.impu'].cumsum()

        raw_metrics.loc[:, f'delta.{measurement}'] = impute_column(raw_metrics[f'delta.{measurement}'])
        raw_metrics.loc[:, f'total.{measurement}'] = raw_metrics[f'delta.{measurement}'].cumsum()

        # TODO: move mean calculations here

    # generate Levitt Metric
    raw_metrics.loc[:, "total.deceased.levitt"] = calculate_levitt_metric(raw_metrics["total.deceased"])

    # 21-Day MA of daily tests
    raw_metrics.loc[:, "MA.21.daily.tests"] = raw_metrics["delta.tested"].rolling(window=mean_window).mean()

    # TPR% per day
    raw_metrics.loc[:, "delta.positivity"] = (raw_metrics["delta.confirmed"] / raw_metrics["delta.tested"]) * 100.0

    # 21-Day MA of TPR%
    raw_metrics.loc[:, "MA.21.delta.positivity"] = raw_metrics["delta.positivity"].rolling(window=mean_window).mean()

    # daily percent case growth
    raw_metrics.loc[:, "delta.percent.case.growth"] = raw_metrics["delta.confirmed"].pct_change()

    hospitalization_ratios_updated = calculate_or_impute_hospitalizations(raw_metrics,
                                                                          hospitalizations)

    # bit tricky this... we assume no kind of sorting happend
    raw_metrics["delta.hospitalized"] = hospitalization_ratios_updated['hospitalizations'].values

    hospitalization_ratios_updated = hospitalization_ratios_updated.drop(columns=['hospitalizations'])

    # total hospitalizations
    raw_metrics.loc[:, "total.hospitalized"] = raw_metrics["delta.hospitalized"].cumsum()

    # active cases by day
    raw_metrics.loc[:, "delta.active"] = (
            raw_metrics["total.confirmed"]
            - raw_metrics["total.deceased"]
            - raw_metrics["total.recovered"]
            - raw_metrics["delta.other"]
    )

    # cubic splines
    raw_metrics.loc[:, "spline.active"] = cubic_spline(raw_metrics["delta.active"])
    raw_metrics.loc[:, "spline.deceased"] = cubic_spline(raw_metrics["delta.deceased"])
    raw_metrics.loc[:, "spline.hospitalized"] = cubic_spline(raw_metrics["delta.hospitalized"])
    raw_metrics.loc[:, "spline.recovered"] = cubic_spline(raw_metrics["delta.recovered"])

    # active cases by day non imputed
    raw_metrics.loc[:, "delta.active.non.impu"] = (
            raw_metrics["total.confirmed.non.impu"]
            - raw_metrics["total.deceased.non.impu"]
            - raw_metrics["total.recovered.non.impu"]
            - raw_metrics["delta.other.non.impu"]
    )

    # 21 day moving average
    raw_metrics.loc[:, "MA.21.daily.active.non.impu"] = raw_metrics["delta.active.non.impu"].rolling(
        window=mean_window).mean()
    raw_metrics.loc[:, "MA.21.daily.deceased.non.impu"] = raw_metrics["delta.deceased.non.impu"].rolling(
        window=mean_window).mean()
    # Hospitilasation function is picking data from imputed columns, hence will be created once imputed is replaced by non imputed
    # df["MA.21.daily.hospitalized"] = df["delta.hospitalized"].rolling(window=mean_window).mean()
    raw_metrics.loc[:, "MA.21.daily.recovered.non.impu"] = raw_metrics["delta.recovered.non.impu"].rolling(
        window=mean_window).mean()

    return raw_metrics
