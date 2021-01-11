import pandas as pd


def last_values_by_date(df: pd.DataFrame):
    """ We assume that on every date all population data is present so we can just do an
    easy filter on last date """

    max_date = df['date'].max()
    filtered_by_date = df[df['date'] == max_date]
    return filtered_by_date
