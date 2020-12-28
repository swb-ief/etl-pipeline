from datetime import datetime
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
    assert isinstance(current_hospitalizations.index, pd.DatetimeIndex)

    # they can have duplicates (multi city/ward/etc..)
    expected_dates = expected_dates.drop_duplicates()

    ratio_column = 'percentages'  # incorrectly named percentages but is actualy a value between 0 and 1
    df = expected_dates.to_frame().set_index('date')
    df = df.merge(current_hospitalizations, how='left', left_index=True, right_index=True)
    df[ratio_column] = df[ratio_column].apply(lambda x: random.uniform(0.12, 0.16) if pd.isnull(x) else x)
    return df


def calculate_or_impute_hospitalizations(
        delta_confirmed: pd.Series,
        hospitalization_ratios: pd.Series) -> pd.DataFrame:
    """ :return: merged Dataframe with an extra column 'hospitalizations' with delta confirmed * ratio
            and if there is no ratio it will randomly estimate it between .12 and .16"""

    ratio_column = 'percentages'  # incorrectly named percentages but is actualy a value between 0 and 1
    assert isinstance(delta_confirmed.index, pd.DatetimeIndex)
    assert isinstance(hospitalization_ratios.index, pd.DatetimeIndex)
    assert delta_confirmed.name == 'delta.confirmed'
    assert hospitalization_ratios.name == ratio_column

    df = delta_confirmed.to_frame()
    df = df.merge(hospitalization_ratios, how='left', left_index=True, right_index=True)

    df['hospitalizations'] = df['delta.confirmed'] * df[ratio_column]
    return df


def cubic_spline(column: pd.Series) -> np.array:
    cs = CubicSpline(range(len(column)), column)
    return cs(range(len(column)))


def update_city_stats(
        start_date: datetime,
        city_stats: pd.DataFrame,
        hospitalizations: pd.DataFrame = None,
) -> (pd.DataFrame, pd.DataFrame):
    """
    :param start_date: Don't process any data before it
    :returns: metrics
    """
    measurements = ['tested', 'confirmed', 'deceased', 'recovered', 'other']
    mean_window = 21

    # add "other" columns
    if "delta.other" not in city_stats.columns:
        city_stats["delta.other"] = 0
    if "total.other" not in city_stats.columns:
        city_stats["total.other"] = 0

    # Nozziel: 2020-12-27 why are we doining this?
    city_stats.loc[city_stats["delta.other"] < 0, ["delta.other"]] = np.nan

    # we start all the data from this date as a baseline date
    city_stats = city_stats[city_stats.index >= start_date]
    hospitalizations = hospitalizations[hospitalizations.index >= start_date]

    for measurement in measurements:
        # TODO: argument for fillna still being considered not imputed
        city_stats.loc[:, f'delta.{measurement}.non.impu'] = city_stats[f'delta.{measurement}'].fillna(value=0)
        city_stats.loc[:, f'total.{measurement}.non.impu'] = city_stats[f'delta.{measurement}.non.impu'].cumsum()

        city_stats.loc[:, f'delta.{measurement}'] = impute_column(city_stats[f'delta.{measurement}'])
        city_stats.loc[:, f'total.{measurement}'] = city_stats[f'delta.{measurement}'].cumsum()

        # TODO: move mean calculations here

    # generate Levitt Metric
    city_stats.loc[:, "levitt.Metric"] = calculate_levitt_metric(city_stats["total.deceased"])

    # 21-Day MA of daily tests
    city_stats.loc[:, "MA.21.daily.tests"] = city_stats["delta.tested"].rolling(window=mean_window).mean()

    # TPR% per day
    city_stats.loc[:, "delta.positivity"] = (city_stats["delta.confirmed"] / city_stats["delta.tested"]) * 100.0

    # 21-Day MA of TPR%
    city_stats.loc[:, "MA.21.delta.positivity"] = city_stats["delta.positivity"].rolling(window=mean_window).mean()

    # daily percent case growth
    city_stats.loc[:, "delta.percent.case.growth"] = city_stats["delta.confirmed"].pct_change()

    # Impute hospitalization data
    if 'percentages' in hospitalizations.columns:
        hospitalization_ratios = hospitalizations['percentages']
    else:
        hospitalization_ratios = None

    hospitalization_ratios_updated = calculate_or_impute_hospitalizations(city_stats['delta.confirmed'],
                                                                          hospitalization_ratios)

    # bit tricky this... we assume no kind of sorting happend
    city_stats["delta.hospitalized"] = hospitalization_ratios_updated['hospitalizations'].values

    hospitalization_ratios_updated = hospitalization_ratios_updated.drop(columns=['hospitalizations'])

    # total hospitalizations
    city_stats.loc[:, "total.hospitalized"] = city_stats["delta.hospitalized"].cumsum()

    # active cases by day
    city_stats.loc[:, "delta.active"] = (
            city_stats["total.confirmed"]
            - city_stats["total.deceased"]
            - city_stats["total.recovered"]
            - city_stats["delta.other"]
    )

    # cubic splines
    city_stats.loc[:, "spline.active"] = cubic_spline(city_stats["delta.active"])
    city_stats.loc[:, "spline.deceased"] = cubic_spline(city_stats["delta.deceased"])
    city_stats.loc[:, "spline.hospitalized"] = cubic_spline(city_stats["delta.hospitalized"])
    city_stats.loc[:, "spline.recovered"] = cubic_spline(city_stats["delta.recovered"])

    # active cases by day non imputed
    city_stats.loc[:, "delta.active.non.impu"] = (
            city_stats["total.confirmed.non.impu"]
            - city_stats["total.deceased.non.impu"]
            - city_stats["total.recovered.non.impu"]
            - city_stats["delta.other.non.impu"]
    )

    # 21 day moving average
    city_stats.loc[:, "MA.21.daily.active.non.impu"] = city_stats["delta.active.non.impu"].rolling(
        window=mean_window).mean()
    city_stats.loc[:, "MA.21.daily.deceased.non.impu"] = city_stats["delta.deceased.non.impu"].rolling(
        window=mean_window).mean()
    # Hospitilasation function is picking data from imputed columns, hence will be created once imputed is replaced by non imputed
    # df["MA.21.daily.hospitalized"] = df["delta.hospitalized"].rolling(window=mean_window).mean()
    city_stats.loc[:, "MA.21.daily.recovered.non.impu"] = city_stats["delta.recovered.non.impu"].rolling(
        window=mean_window).mean()

    updated_city_stats = pd.concat([city_stats_before, city_stats])
    return updated_city_stats
