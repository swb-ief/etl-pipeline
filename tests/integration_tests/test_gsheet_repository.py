import os
import unittest
from datetime import datetime, timedelta
import pandas as pd

import pytest
from pandas.testing import assert_frame_equal

from backend import GSheetRepository
from backend.config import get_config


@pytest.mark.skipif('CI' not in os.environ, reason="Can only run on github (due to secrets)")
class TestGSheetRepository(unittest.TestCase):
    def setUp(self) -> None:
        config = get_config()
        self._url = config['google sheets']['url development']

    def test__get_gspread_client(self):
        sut = GSheetRepository(self._url)
        result = sut._get_gspread_client()
        self.assertIsNotNone(result)

    def test__get_worksheet(self):
        sut = GSheetRepository(self._url)
        # small worksheet, but might be removed in the future, then switch to an other sheet with little data
        result = sut._get_worksheet('for_unit_tests')
        self.assertIsNotNone(result)

    def test_get_dataframe(self):
        sut = GSheetRepository(self._url)
        # small worksheet, but might be removed in the future, then switch to an other sheet with little data
        result = sut.get_dataframe('for_unit_tests')
        self.assertIsNotNone(result)

    def test__worksheet_exists(self):
        sut = GSheetRepository(self._url)
        result = sut.exists('for_unit_tests')
        self.assertTrue(result)

    def test__worksheet_not_exists(self):
        sut = GSheetRepository(self._url)
        result = sut.exists('Fake_Name_should_not_exist')
        self.assertFalse(result)

    def test_store_dataframe(self):
        df = pd.DataFrame({
            'date': [datetime.now() - timedelta(days=1), datetime.now],
            'some_metric': [0.1, 3.2]
        })

        sut = GSheetRepository(self._url)
        sut.store_dataframe(df, 'for_unit_tests', allow_create=True)

        result = sut.get_dataframe('for_unit_tests')
        assert_frame_equal(df, result)
