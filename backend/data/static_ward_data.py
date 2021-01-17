import os
import pandas as pd

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


def get_static_ward_data() -> pd.DataFrame:
    df = pd.read_csv(os.path.join(THIS_DIR, 'static_ward_data.csv'))
    return df
