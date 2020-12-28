import unittest
import pytest
import numpy as np
import pandas as pd
from pandas._testing import assert_frame_equal

from backend.gsheet_repository import GSheetRepository


class TestGSheetRepository(unittest.TestCase):

    @pytest.mark.skip("TODO: make conditiona and only run on github")
    def test_get_worksheet_url_from_env(self):
        assert False

    @pytest.mark.skip("TODO: make conditiona and only run on github")
    def test__get_gspread_client(self):
        assert False

    @pytest.mark.skip("TODO: make conditiona and only run on github")
    def test__get_worksheet(self):
        assert False

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

        def flatten(a_list):
            return [item for sublist in a_list for item in sublist]

        self.assertListEqual(flatten(expected), flatten(result))


@pytest.mark.skip("Probably never make a test for this")
def test_store_dataframe(self):
    assert False


def test_get_dataframe(self):
    assert False
