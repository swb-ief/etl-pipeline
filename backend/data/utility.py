import pandas as pd


def last_values_by_date(df: pd.DataFrame):
    """ We assume that on every date all population data is present so we can just do an
    easy filter on last date """

    max_date = df['date'].max()
    filtered_by_date = df[df['date'] == max_date]
    return filtered_by_date


def create_delta_cols( df,group_by_cols, delta_needed_for_cols):
    index_cols = df.index.names
    df = df.reset_index()
    df = df.sort_values(index_cols) # this may not be strictly needed. Does DF already sort by index?

    for column in delta_needed_for_cols:
        df[f'delta.{column}'] = df.groupby(group_by_cols)[f'total.{column}'].diff().fillna(
            0)
    df = df.set_index(index_cols)

    return df


def interpolate_values(df, group_by_cols, delta_needed_for_cols):
    # create copy of original dataframe
    df_cp = df.copy()

    # concatenate join columns
    df_cp["combo"] = df_cp[group_by_cols].apply(lambda x: "|".join(x.tolist()), axis=1)
    
    # obtain list of wards
    list_of_wards = list(df_cp["combo"].unique())

    ### create dataframe such that every ward has a row for every date in the original dataframe ###
    # replicate dates as many times as there are wards
    r = pd.date_range(start=df_cp.date.min(), end=df_cp.date.max())
    num_dates = len(r)
    num_wards = len(list_of_wards)
    r = r.tolist()
    r = r * num_wards
    r = sorted(r)
    # replicate wards as many times as there are unique dates
    wardrep = list_of_wards*num_dates

    # create new dataframe with the complete series of dates for each ward
    complete_df = pd.DataFrame({"date": r, "combo": wardrep})
    # obtain state/district/ward from the concatenated join-column
    complete_df["state"] = complete_df["combo"].apply(lambda x: x.split("|")[0])
    complete_df["district"] = complete_df["combo"].apply(lambda x: x.split("|")[1])
    complete_df["ward"] = complete_df["combo"].apply(lambda x: x.split("|")[2])
    # obtain all columns in the original dataframe (except join columns and state/district/ward columns)
    original_cols = [x for x in df.columns if x not in ["combo", "date", "state", "district", "ward"]]
    # join new dataframe with original dataframe to obtain available values against each date
    complete_df = complete_df.merge(df_cp[["combo", "date"]+original_cols], on=["combo", "date"], how="left")

    # sort dataframe by date/ward
    complete_df.sort_values(by=["combo", "date"], inplace=True)

    # fill missing values with linearly interpolated values, and round interpolated values to the nearest integer
    for item in delta_needed_for:
        complete_df[item] = complete_df.groupby("combo")[item].transform(lambda x: x.fillna(x.interpolate()))
        complete_df[item] = complete_df[item].apply(lambda x: round(x, 0))

    # sort again on ward/date
    complete_df.sort_values(by=["combo", "date"], inplace=True)

    # delete concatenated join column
    del complete_df['combo']
    
    return complete_df

    return complete_df
