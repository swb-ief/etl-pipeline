import os
import pandas as pd

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


def get_static_ward_data() -> pd.DataFrame:
    df = pd.read_csv(os.path.join(THIS_DIR, 'static_ward_data.csv'), header=0,
                     index_col=['state', 'district'])
    # df = df.set_index(['state', 'district', 'ward'])

    return df
