from tasks.fetch_ward_data import create_delta_cols
from backend.data.utility import create_delta_cols
import pandas as pd
import os

THIS_DIR = os.path.dirname(os.path.abspath(__file__))

def test_delta_calc():

    # create a  dataframe with some rows and cols

    df = pd.read_csv(
        os.path.join(THIS_DIR, 'samples/wards_sample_test_delta.csv'),
        parse_dates=['date'])
    expected = (6, 8)

    # invoke create_delta_cols with this dataframe

    df = df.set_index(['state', 'district', 'ward', 'date'])
    df_delta = create_delta_cols(df,['state', 'district', 'ward'], ['confirmed','recovered','deceased','active'])

    # check that the result given by create_delta_cols has new cols and values as expected

    assert df_delta is not None
    assert df_delta.at[('MH', 'Mumbai', 'A', '2021-01-01 0:00:00'), 'delta.confirmed'] == 0
    assert df_delta.at[('MH', 'Mumbai', 'C', '2021-01-02 0:00:00'), 'delta.confirmed'] == 2000
    assert df_delta.at[('MH', 'Mumbai', 'C', '2021-01-03 0:00:00'), 'delta.recovered'] == 100

    assert df_delta.shape == expected



