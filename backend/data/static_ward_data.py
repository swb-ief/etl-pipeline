import os
import pandas as pd

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


def get_static_ward_data() -> pd.DataFrame:
    """ Loads the static ward data
    :returns: dataframe indexed by state, district and ward
    """
    df = pd.read_csv(os.path.join(THIS_DIR, 'static_ward_data.csv'), header=0,
                     index_col=['state', 'district', 'ward'])

    return df
