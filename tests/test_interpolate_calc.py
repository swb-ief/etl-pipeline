from backend.data.utility import interpolate_values
import pandas as pd
import os

THIS_DIR = os.path.dirname(os.path.abspath(__file__))

def test_interpolate_calc():

    # create a  dataframe with some rows and cols

    df = pd.read_csv(
        os.path.join(THIS_DIR, 'samples/wards_sample_test_interpolation.csv'),
        parse_dates=['date'])
    expected = (14, 4)

    # invoke create_delta_cols with this dataframe

    df = df.set_index(['state', 'district', 'ward', 'date'])
    df_interpolate = interpolate_values(df,['state', 'district', 'ward'], ['confirmed','recovered','deceased','active'])

    # check that the result given by create_delta_cols has new cols and values as expected

    assert df_interpolate is not None
    
    assert df_interpolate.at[('MH', 'Mumbai', 'A', '2021-01-02 0:00:00'), 'total.confirmed'] == 7048
    assert df_interpolate.at[('MH', 'Mumbai', 'A', '2021-01-05 0:00:00'), 'total.confirmed'] == 7109
    assert df_interpolate.at[('MH', 'Mumbai', 'B', '2021-01-04 0:00:00'), 'total.confirmed'] == 2213
    assert df_interpolate.at[('MH', 'Mumbai', 'B', '2021-01-05 0:00:00'), 'total.confirmed'] == 2214
    assert df_interpolate.at[('MH', 'Mumbai', 'B', '2021-01-06 0:00:00'), 'total.confirmed'] == 2216
    
    
    assert df_interpolate.at[('MH', 'Mumbai', 'A', '2021-01-02 0:00:00'), 'total.recovered'] == 6727
    assert df_interpolate.at[('MH', 'Mumbai', 'A', '2021-01-05 0:00:00'), 'total.recovered'] == 6804
    assert df_interpolate.at[('MH', 'Mumbai', 'B', '2021-01-04 0:00:00'), 'total.recovered'] == 2016
    assert df_interpolate.at[('MH', 'Mumbai', 'B', '2021-01-05 0:00:00'), 'total.recovered'] == 2020
    assert df_interpolate.at[('MH', 'Mumbai', 'B', '2021-01-06 0:00:00'), 'total.recovered'] == 2023
    
    assert df_interpolate.at[('MH', 'Mumbai', 'A', '2021-01-02 0:00:00'), 'total.deceased'] == 121
    assert df_interpolate.at[('MH', 'Mumbai', 'A', '2021-01-05 0:00:00'), 'total.deceased'] == 121
    assert df_interpolate.at[('MH', 'Mumbai', 'B', '2021-01-04 0:00:00'), 'total.deceased'] == 144
    assert df_interpolate.at[('MH', 'Mumbai', 'B', '2021-01-05 0:00:00'), 'total.deceased'] == 144
    assert df_interpolate.at[('MH', 'Mumbai', 'B', '2021-01-06 0:00:00'), 'total.deceased'] == 144
    
    assert df_interpolate.at[('MH', 'Mumbai', 'A', '2021-01-02 0:00:00'), 'total.active'] == 192
    assert df_interpolate.at[('MH', 'Mumbai', 'A', '2021-01-05 0:00:00'), 'total.active'] == 176
    assert df_interpolate.at[('MH', 'Mumbai', 'B', '2021-01-04 0:00:00'), 'total.active'] == 46
    assert df_interpolate.at[('MH', 'Mumbai', 'B', '2021-01-05 0:00:00'), 'total.active'] == 44
    assert df_interpolate.at[('MH', 'Mumbai', 'B', '2021-01-06 0:00:00'), 'total.active'] == 42

    assert df_interpolate.shape == expected
