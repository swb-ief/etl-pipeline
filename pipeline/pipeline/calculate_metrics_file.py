import numpy as np
import pandas as pd
from numpy import random
from scipy.interpolate import CubicSpline


def imputeCols(col):

    '''
    Uniformly distributes the accumulated figures over days with zero values.

    Input- Column that needs to be imputed
    Output- Numpy imputed column

    For e.g if there are non tested reported on Day1,2 and 3 and there are 400 on day 4, then imputation
    spread it out to 100 each on Day1,2,3,4

    Issue identified- Since the tested/decesed etc has to be whole numbers hence we are loosing some numbers in distribution.
    For e.g If on Day 1,2,3 no testing reported and on Day 4,  27 tested then This will spread out as Day1-6 tested Day2- 6 tested
    Day 3-6 tested Day 4-6 tested, which is equal to 24. Hnece we will loose out 3 tested.
    Over a period of time it cumulates to large numbers.
    '''
    col = pd.Series(np.where(col < 0, np.NaN, col))
    preNaNs = (
        col.isnull()
        .astype(int)
        .groupby(col.notnull().astype(int).cumsum())
        .cumsum()
        .shift(1)
        + 1
    )
    avgs = np.round(col / preNaNs)
    avgs = avgs.bfill()
    avgs = np.where(np.logical_or(np.isnan(col), avgs < col), avgs, col)
    avgs = pd.Series(avgs)
    avgs.fillna(np.round(avgs.mean()), inplace=True)
    return avgs.to_numpy()


def generate_hospitalizations(df, hospitalizations):
    try:
        df_bck = pd.read_csv(hospitalizations, index_col=["date"])
    except Exception as e:
        print("Unable to find hospitalizatons file. Re-generating data")
        df["percentages"] = random.uniform(0.12, 0.16, size=df.shape[0])
        df_bck = pd.DataFrame(df.pop("percentages"), index=df.index)
        df_bck.to_csv(hospitalizations)
    df = df.join(df_bck)
    if sum(np.isnan(df["percentages"])) > 0:
        imp = pd.Series(
            random.uniform(0.12, 0.16, size=sum(np.isnan(df["percentages"])))
        )
        df.loc[np.isnan(df["percentages"]), ["percentages"]] = imp[0]
        df_bck = pd.DataFrame(df["percentages"], index=df.index)
        df_bck.to_csv(hospitalizations)
    return df.pop("percentages") * df["delta.confirmed"]


def cubic_spline(col):
    cs = CubicSpline(range(len(col)), col)
    return cs(range(len(col)))


def calculate_metrics(
    df,
    start_date="2020-04-20",
    hospitalizations_csv="output/percentages_for_hospitalizations.csv",
    output_city_metrics_csv="output/city_metrics.csv",
    header=True,
):
    # add "other" columns
    if not "delta.other" in df.columns:
        df["delta.other"] = 0
    if not "total.other" in df.columns:
        df["total.other"] = 0

    df.loc[df["delta.other"] < 0, ["delta.other"]] = np.nan

    # we start all the data from this date as a baseline date
    drop_rows = df[df.index < start_date]
    # print(drop_rows.index[0:5])
    df.drop(drop_rows.index, axis=0, inplace=True)

    # create non imputed temp column before it gets imputed.Temp column is created just to store values
    # It will be deleted and values transferred to another column to not affect the position of columns
    df["delta.tested.non.impu.temp"] = df["delta.tested"].fillna(value=0)
    df["delta.confirmed.non.impu.temp"] = df["delta.confirmed"].fillna(value=0)
    df["delta.deceased.non.impu.temp"] = df["delta.deceased"].fillna(value=0)
    df["delta.recovered.non.impu.temp"] = df["delta.recovered"].fillna(value=0)
    df["delta.other.non.impu.temp"] = df["delta.other"].fillna(value=0)

    # impute data
    df["delta.tested"] = imputeCols(df["delta.tested"])
    df["delta.confirmed"] = imputeCols(df["delta.confirmed"])
    df["delta.deceased"] = imputeCols(df["delta.deceased"])
    df["delta.recovered"] = imputeCols(df["delta.recovered"])
    df["delta.other"] = imputeCols(df["delta.other"])

    # generate new "total" columns
    df["total.confirmed"] = df["delta.confirmed"].cumsum()
    df["total.tested"] = df["delta.tested"].cumsum()
    df["total.recovered"] = df["delta.recovered"].cumsum()
    df["total.deceased"] = df["delta.deceased"].cumsum()
    df["total.other"] = df["delta.other"].cumsum()

    # generate Levitt Metric
    df["total.deceased.shift"] = df["total.deceased"].shift(1)
    df["levitt.Metric"] = np.log(df["total.deceased"] / df.pop("total.deceased.shift"))

    # 21-Day MA of daily tests
    df["MA.21.daily.tests"] = df["delta.tested"].rolling(window=21).mean()

    # TPR% per day
    df["delta.positivity"] = (df["delta.confirmed"] / df["delta.tested"]) * 100.0

    # 21-Day MA of TPR%
    df["MA.21.delta.positivity"] = df["delta.positivity"].rolling(window=21).mean()

    # daily percent case growth
    df["delta.percent.case.growth"] = df["delta.confirmed"].pct_change()

    # Impute hospitalization data
    df["delta.hospitalized"] = generate_hospitalizations(df, hospitalizations_csv)

    # total hospitalizations
    df["total.hospitalized"] = df["delta.hospitalized"].cumsum()

    # active cases by day
    df["delta.active"] = (
        df["total.confirmed"]
        - df["total.deceased"]
        - df["total.recovered"]
        - df["delta.other"]
    )

    # cubic splines
    df["spline.active"] = cubic_spline(df["delta.active"])
    df["spline.deceased"] = cubic_spline(df["delta.deceased"])
    df["spline.hospitalized"] = cubic_spline(df["delta.hospitalized"])
    df["spline.recovered"] = cubic_spline(df["delta.recovered"])


    # Create new columns for non imputed data and delete the temp column

    #daily data

    df["delta.tested.non.impu"] = df["delta.tested.non.impu.temp"]
    df["delta.confirmed.non.impu"] = df["delta.confirmed.non.impu.temp"]
    df["delta.deceased.non.impu"] = df["delta.deceased.non.impu.temp"]
    df["delta.recovered.non.impu"] = df["delta.recovered.non.impu.temp"]
    df["delta.other.non.impu"] = df["delta.other.non.impu.temp"]

    df=df.drop(columns=["delta.tested.non.impu.temp","delta.confirmed.non.impu.temp","delta.deceased.non.impu.temp","delta.recovered.non.impu.temp","delta.other.non.impu.temp"])


    #total column

    df["total.tested.non.impu"] = df["delta.tested.non.impu"].cumsum()
    df["total.confirmed.non.impu"] = df["delta.confirmed.non.impu"].cumsum()
    df["total.recovered.non.impu"] = df["delta.recovered.non.impu"].cumsum()
    df["total.deceased.non.impu"] = df["delta.deceased.non.impu"].cumsum()
    df["total.other.non.impu"] = df["delta.other.non.impu"].cumsum()

    # active cases by day non imputed
    df["delta.active.non.impu"] = (
            df["total.confirmed.non.impu"]
            - df["total.deceased.non.impu"]
            - df["total.recovered.non.impu"]
            - df["delta.other.non.impu"]
    )

    #21 day moving average


    df["MA.21.daily.active.non.impu"] = df["delta.active.non.impu"].rolling(window=21).mean()
    df["MA.21.daily.deceased.non.impu"] = df["delta.deceased.non.impu"].rolling(window=21).mean()
    #Hospitilasation function is picking data from imputed columns, hence will be created once imputed is replaced by non imputed
    #df["MA.21.daily.hospitalized"] = df["delta.hospitalized"].rolling(window=21).mean()
    df["MA.21.daily.recovered.non.impu"] = df["delta.recovered.non.impu"].rolling(window=21).mean()



    df.to_csv(output_city_metrics_csv, mode="w" if header else "a", header=header)


# df = pd.read_csv("output/city_stats.csv", index_col=["date"])
# df.index = pd.to_datetime(df.index)
##df = df[df.district == "Mumbai"]
# calculate_metrics(df=df)
