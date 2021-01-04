import numpy as np
import pandas as pd
from numpy import random
from scipy.interpolate import CubicSpline



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

    #  filling out na with 0 values
    df["delta.tested"] = df["delta.tested"].fillna(value=0)
    df["delta.confirmed"] = df["delta.confirmed"].fillna(value=0)
    df["delta.deceased"] = df["delta.deceased"].fillna(value=0)
    df["delta.recovered"] = df["delta.recovered"].fillna(value=0)
    df["delta.other"] = df["delta.other"].fillna(value=0)

    # generate new "total" columns
    df["total.confirmed"] = df["delta.confirmed"].cumsum().fillna(value=0)
    df["total.tested"] = df["delta.tested"].cumsum().fillna(value=0)
    df["total.recovered"] = df["delta.recovered"].cumsum().fillna(value=0)
    df["total.deceased"] = df["delta.deceased"].cumsum().fillna(value=0)
    df["total.other"] = df["delta.other"].cumsum().fillna(value=0)

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

    df["MA.21.daily.active"] = df["delta.active"].rolling(window=21).mean()
    df["MA.21.daily.deceased"] = df["delta.deceased"].rolling(window=21).mean()
    df["MA.21.daily.hospitalized"] = df["delta.hospitalized"].rolling(window=21).mean()
    df["MA.21.daily.recovered"] = df["delta.recovered"].rolling(window=21).mean()

    df = df.replace([np.inf, -np.inf], np.nan)

    df.to_csv(output_city_metrics_csv, mode="w" if header else "a", header=header)


# df = pd.read_csv("output/city_stats.csv", index_col=["date"])
# df.index = pd.to_datetime(df.index)
##df = df[df.district == "Mumbai"]
# calculate_metrics(df=df)
