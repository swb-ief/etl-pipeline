import random
import logging
from functools import partial

import pandas as pd
import numpy as np
from numpy import random
from sklearn.impute import KNNImputer


def calculate_city_states_without_hospitalizations(input_path, output_path, city_name):
    df = pd.read_csv(input_path, index_col=["date"])
    df.index = pd.to_datetime(df.index)
    # The cities would be iterated over in the actual case
    df = df[df.district == city_name]
    # We check if there are any other columns
    df.dropna(subset=["total.confirmed"], inplace=True)
    # We check if there are any other columns
    if not "delta.other" in df.columns:
        df["delta.other"] = 0
    if not "total.other" in df.columns:
        df["total.other"] = 0
    # Open Question: Impute NaN for daily tests
    # What is a good strategy to impute NaN for delta tests? We need to ensure that the imputations match the total tests.
    # Currently, we drop the rows that have empty delta tests.
    df.dropna(subset=["delta.tested"], inplace=True)
    df.loc[df["delta.tested"] < 0, ["delta.tested"]] = np.nan
    df.loc[df["delta.confirmed"] < 0, ["delta.confirmed"]] = np.nan
    imputer = KNNImputer(n_neighbors=7)
    df.loc[:, ["delta.confirmed", "delta.tested"]] = np.round(
        imputer.fit_transform(df.loc[:, ["delta.confirmed", "delta.tested"]])
    )
    # Find Levitt metric¶
    # We first shift the deceased cases and apply log
    df["total.deceased.shift"] = df["total.deceased"].shift(1)
    df["levitt.Metric"] = np.log(df["total.deceased"] / df.pop("total.deceased.shift"))
    # 21-day moving averages
    # MA of daily tests
    df["MA.21.daily.tests"] = df["delta.tested"].rolling(window=21).mean()
    df["delta.positivity"] = (df["delta.confirmed"] / df["delta.tested"]) * 100.0
    df["MA.21.delta.positivity"] = df["delta.positivity"].rolling(window=21).mean()
    df["delta.percent.case.growth"] = df["delta.confirmed"].pct_change()
    df["MA.21.delta.percent.case.growth"] = (
        df["delta.percent.case.growth"].rolling(window=21).mean()
    )
    # Daily case positivity¶
    # We find daily positivity by dividing daily confirmed cases by the number of tests
    df["delta.positivity"] = (df["delta.confirmed"] / df["delta.tested"]) * 100.0
    df["MA.21.delta.positivity"] = df["delta.positivity"].rolling(window=21).mean()
    # Percent case growth
    df["delta.percent.case.growth"] = df["delta.confirmed"].pct_change()
    df["MA.21.delta.percent.case.growth"] = (
        df["delta.percent.case.growth"].rolling(window=21).mean()
    )
    df.to_csv(output_path)


def reset_hospitalization_percentages(input_path, output_path, city_name):
    df = pd.read_csv(input_path, index_col=["date"])
    df.index = pd.to_datetime(df.index)
    # The cities would be iterated over in the actual case
    df = df[df.district == city_name]
    df["percentages"] = random.uniform(0.12, 0.16, size=df.shape[0])
    df_bck = pd.DataFrame(df["percentages"], index=df.index)
    df_bck.to_csv(output_path)


def calculate_city_states_hospitalizations(
    previous_hospitalization_path, input_path, output_path, city_name
):
    df = pd.read_csv(input_path, index_col=["date"])
    df.index = pd.to_datetime(df.index)
    # The cities would be iterated over in the actual case
    df = df[df.district == city_name]

    df_bck = pd.read_csv(previous_hospitalization_path, index_col=["date"])
    df = df.join(df_bck)

    percentages_sum = sum(np.isnan(df["percentages"]))
    if sum(np.isnan(df["percentages"])) > 0:
        imp = pd.Series(
            random.uniform(0.12, 0.16, size=sum(np.isnan(df["percentages"])))
        )
        df.loc[np.isnan(df["percentages"]), ["percentages"]] = imp[0]
        df_bck = pd.DataFrame(df["percentages"], index=df.index)
    df_bck.to_csv(output_path)


def calculate_city_stats_with_hospitalizations(
    input_path, hospitalization_path, output_path, city_name
):
    df = pd.read_csv(input_path, index_col=["date"])
    df.index = pd.to_datetime(df.index)
    # The cities would be iterated over in the actual case
    df = df[df.district == city_name]
    # Impute Hospitalization data
    # We calculate values from 12% to 16% uniformly and store them in a file for persistence and consistency
    df_bck = pd.read_csv(hospitalization_path, index_col=["date"])
    df = df.join(df_bck)
    df["delta.hospitalized"] = df.pop("percentages") * df["delta.confirmed"]
    df["total.hospitalized"] = df["delta.hospitalized"].cumsum()
    df["delta.active"] = (
        df["total.confirmed"] - df["total.deceased"] - df["total.recovered"]
    )

    # Total active cases
    # We find active cases by:
    # Active = Confirmed - deceased - recovered
    df.to_csv(output_path)
