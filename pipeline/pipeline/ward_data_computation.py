import pandas as pd
import numpy as np


def ward_data_pre_processing(df):
    list_of_ward_names = list(df["Ward_Name"].unique())
    r = pd.date_range(start=df.as_of.min(), end=df.as_of.max())
    r = r.tolist()
    r = r * len(list_of_ward_names)
    r = sorted(r)
    r_df = pd.DataFrame({"as_of": r})

    df = pd.merge(
        left=r_df.astype(str),
        right=df,
        how="left",
        on=["as_of"],
    )

    df[["Ward_Name"]] = df[["Ward_Name"]].fillna(value="EMP")

    df["imputed"] = df.apply(lambda row: 1 if row["Ward_Name"] == "EMP" else 0, axis=1)

    def impute_ward_names(group):
        ward_names = group == "EMP"
        group[ward_names] = list_of_ward_names
        return group

    imputing_ward_names = df.groupby(["as_of"])["Ward_Name"].transform(
        impute_ward_names
    )

    df["Ward_Name"] = imputing_ward_names
    return df


def impute_ward_population_data(df, df_ward_population):
    df = pd.merge(
        left=df,
        right=df_ward_population[["WARD", "POPULATION"]],
        how="left",
        left_on=["Ward_Name"],
        right_on=["WARD"],
    )

    df = df.drop_duplicates()
    df = df.set_index("as_of").sort_index()
    return df


# impute missing metrics
def impute_missing_metrics(df):
    df["Total_Positive_Cases"] = df.groupby("Ward_Name")[
        "Total_Positive_Cases"
    ].transform(lambda x: x.fillna(x.interpolate()))
    df["Total_Deaths"] = df.groupby("Ward_Name")["Total_Deaths"].transform(
        lambda x: x.fillna(x.interpolate())
    )
    df["Daily_deaths"] = (
        df.groupby(["Ward_Name"])["Total_Deaths"].diff().fillna(0).round()
    )
    df["Daily_cases"] = (
        df.groupby(["Ward_Name"])["Total_Positive_Cases"].diff().fillna(0).round()
    )
    df["Case_growth_rate"] = (
        df.groupby(["Ward_Name"])["Daily_cases"]
        .pct_change()
        .fillna(0.01)
        .replace(np.inf, 0.01)
    )
    df_KE = df[df["Ward_Name"] == "KE"]

    df = df.reset_index(drop=False)
    df = df.rename(
        columns={
            "Total_Positive_Cases": "Cumulative_cases",
            "Total_Deaths": "Cumulative_fatalities",
            "POPULATION": "Ward_population",
            "as_of": "date",
        }
    )
    df = df.drop(columns=["Total_Discharged", "WARD", "Total_Active", "downloaded_for"])

    df["Fatalities_21D_MA"] = df["Daily_deaths"].rolling(window=21).mean().fillna(0)
    df["Cases_21D_MA"] = df["Daily_cases"].rolling(window=21).mean().fillna(0)

    return df
