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
    """
    Function to interpolate values for columns specified in 'delta_needed_for_cols' list, when data for wards are missing on certain days
    
    df - Dataframe containing day-wise count of total confirmed/recovered/fatalities etc for each ward
    group_by_cols - columns used to identify wards with
    delta_needed_for_cols - columns whose values need to be interpolated
    
    Contributor: aswinjayan94
    """
   
    # create copy of original dataframe after resetting index, and saving index names
    index_cols = df.index.names
    df = df.reset_index()
    df_cp = df.copy()

    # obtain list of wards
    list_of_wards = df_cp[group_by_cols].drop_duplicates().reset_index(drop=True)

    ### create dataframe such that every ward has a row for every date in the original dataframe ###
    # replicate dates as many times as there are wards
    r = pd.date_range(start=df_cp.date.min(), end=df_cp.date.max())
    num_dates = len(r)
    num_wards = len(list_of_wards)
    r = r.tolist()
    r = r * num_wards
    r = sorted(r)
    # replicate wards as many times as there are unique dates
    wardrep = pd.concat([list_of_wards]*num_dates)

    # create new dataframe with the complete series of dates for each ward
    complete_df = pd.DataFrame({"date": r})
    complete_df = pd.concat([complete_df, wardrep.reset_index(drop=True)], axis=1)
    # obtain all columns in the original dataframe (except join columns and state/district/ward columns)
    original_cols = [x for x in df.columns if x not in ["date"]+group_by_cols]
    original_cols_order = list(df)
    # join new dataframe with original dataframe to obtain available values against each date
    complete_df = complete_df.merge(df_cp[["date"]+group_by_cols+original_cols], on=["date"]+group_by_cols, how="left")
    complete_df = complete_df[original_cols_order]
    
    # sort dataframe by date/ward
    complete_df.sort_values(by=group_by_cols+["date"], inplace=True)

    # fill missing values with linearly interpolated values, and round interpolated values to the nearest integer
    for item in delta_needed_for_cols:
        complete_df[f'total.{item}'] = complete_df.groupby(group_by_cols)[f'total.{item}'].transform(lambda x: x.fillna(x.interpolate()))
        complete_df[f'total.{item}'] = complete_df[f'total.{item}'].apply(lambda x: round(x, 0))

    # sort again on ward/date
    complete_df.sort_values(by=group_by_cols+["date"], inplace=True)
    complete_df = complete_df.set_index(index_cols)

    return complete_df
