import unittest
import numpy as np
import pandas as pd
from numpy.testing import assert_array_equal

from backend import GSheetRepository


class TestGSheetRepository(unittest.TestCase):

    def test__df_to_cleaned_data(self):
        df = pd.DataFrame({'col1': [1, 22, 3], 'col22': ['aa', 'aa', 'ccc'], 'col3': [12, '', 24]})
        expected = [
            ['col1', 'col22', 'col3'],
            [1, 'aa', 12.],
            [22, 'aa', np.nan],
            [3, 'ccc', 24.]
        ]
        pd.DataFrame({'col1': [1, 22, 3], 'col22': ['aa', 'bb', 'ccc'], 'col3': [12, np.nan, 24]})

        sut = GSheetRepository('not_important')
        result = sut._df_to_cleaned_data(df)

        def flatten_numpy(a_list):
            # only way to compare nested lists with nan's
            return np.array([item for sublist in a_list for item in sublist])

        assert_array_equal(flatten_numpy(expected), flatten_numpy(result))
