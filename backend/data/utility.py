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
