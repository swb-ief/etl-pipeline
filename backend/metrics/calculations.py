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


def calculate_or_impute_hospitalizations(
        delta_confirmed: pd.Series,
        hospitalization_ratios: pd.Series = None) -> pd.Series:
    """ returns delta confirmed * ratio and if there is no ratio it will randomly estimate it between .12 and .13"""

    estimated_ratios = random.uniform(0.12, 0.16, size=len(delta_confirmed))
    if hospitalization_ratios is None:
        ratio = estimated_ratios
    else:
        temp_series = pd.Series(estimated_ratios)
        # use the update() to copy all non nan values from hospitalizations_ratios.
        # This achieves filling all nan with random values.
        pd.Series(estimated_ratios).update(hospitalization_ratios)
        ratio = temp_series.values

    return delta_confirmed * ratio


def cubic_spline(column: pd.Series) -> np.array:
    cs = CubicSpline(range(len(column)), column)
    return cs(range(len(column)))


def calculate_all_metrics(
        start_date: datetime,
        city_stats: pd.DataFrame,
        hospitalizations: pd.DataFrame = None,
) -> pd.DataFrame:
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

    for measurement in measurements:
        # TODO: argument for fillna still being considered not imputed
        city_stats[f'delta.{measurement}.non.impu'] = city_stats[f'delta.{measurement}'].fillna(value=0)
        city_stats[f'total.{measurement}.non.impu'] = city_stats[f'delta.{measurement}.non.impu'].cumsum()

        city_stats[f'delta.{measurement}'] = impute_column(city_stats[f'delta.{measurement}'])
        city_stats[f'total.{measurement}'] = city_stats[f'delta.{measurement}'].cumsum()

        # TODO: move mean calculations here

    # generate Levitt Metric
    city_stats["levitt.Metric"] = calculate_levitt_metric(city_stats["total.deceased"])

    # 21-Day MA of daily tests
    city_stats["MA.21.daily.tests"] = city_stats["delta.tested"].rolling(window=mean_window).mean()

    # TPR% per day
    city_stats["delta.positivity"] = (city_stats["delta.confirmed"] / city_stats["delta.tested"]) * 100.0

    # 21-Day MA of TPR%
    city_stats["MA.21.delta.positivity"] = city_stats["delta.positivity"].rolling(window=mean_window).mean()

    # daily percent case growth
    city_stats["delta.percent.case.growth"] = city_stats["delta.confirmed"].pct_change()

    # Impute hospitalization data
    if hospitalizations and 'percentages' in hospitalizations:
        hospitalization_ratios = hospitalizations['percentages']
    else:
        hospitalization_ratios = None

    city_stats["delta.hospitalized"] = calculate_or_impute_hospitalizations(city_stats['delta.confirmed'],
                                                                            hospitalization_ratios)

    # total hospitalizations
    city_stats["total.hospitalized"] = city_stats["delta.hospitalized"].cumsum()

    # active cases by day
    city_stats["delta.active"] = (
            city_stats["total.confirmed"]
            - city_stats["total.deceased"]
            - city_stats["total.recovered"]
            - city_stats["delta.other"]
    )

    # cubic splines
    city_stats["spline.active"] = cubic_spline(city_stats["delta.active"])
    city_stats["spline.deceased"] = cubic_spline(city_stats["delta.deceased"])
    city_stats["spline.hospitalized"] = cubic_spline(city_stats["delta.hospitalized"])
    city_stats["spline.recovered"] = cubic_spline(city_stats["delta.recovered"])

    # active cases by day non imputed
    city_stats["delta.active.non.impu"] = (
            city_stats["total.confirmed.non.impu"]
            - city_stats["total.deceased.non.impu"]
            - city_stats["total.recovered.non.impu"]
            - city_stats["delta.other.non.impu"]
    )

    # 21 day moving average
    city_stats["MA.21.daily.active.non.impu"] = city_stats["delta.active.non.impu"].rolling(window=mean_window).mean()
    city_stats["MA.21.daily.deceased.non.impu"] = city_stats["delta.deceased.non.impu"].rolling(
        window=mean_window).mean()
    # Hospitilasation function is picking data from imputed columns, hence will be created once imputed is replaced by non imputed
    # df["MA.21.daily.hospitalized"] = df["delta.hospitalized"].rolling(window=mean_window).mean()
    city_stats["MA.21.daily.recovered.non.impu"] = city_stats["delta.recovered.non.impu"].rolling(
        window=mean_window).mean()

    return city_stats
