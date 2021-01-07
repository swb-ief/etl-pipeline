import os
import unittest
from datetime import timedelta, date, datetime
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
        self._sheets_created = []
        self._test_storage_location = 'for_integration_tests'

        # be sure there is a basic worksheet to run other tests on.
        # but we are not deleting this one. We just want to make sure its always there
        self.test_create_storage_location()

    def tearDown(self) -> None:
        repository = GSheetRepository(self._url)
        for sheet in self._sheets_created:
            repository.delete_storage_location(sheet)

    @pytest.mark.skip('Called by setup, needed to make sure that other tests work.')
    def test_create_storage_location(self):
        sut = GSheetRepository(self._url)
        sut.create_storage_location(self._test_storage_location)

    def test__get_gspread_client(self):
        sut = GSheetRepository(self._url)
        result = sut._get_gspread_client()
        self.assertIsNotNone(result)

    def test__get_worksheet(self):
        sut = GSheetRepository(self._url)
        # small worksheet, but might be removed in the future, then switch to an other sheet with little data
        result = sut._get_worksheet(self._test_storage_location)
        self.assertIsNotNone(result)

    def test_get_dataframe(self):
        sut = GSheetRepository(self._url)

        result = sut.get_dataframe(self._test_storage_location)
        self.assertIsNotNone(result)

    def test__worksheet_exists(self):
        sut = GSheetRepository(self._url)
        result = sut.exists(self._test_storage_location)
        self.assertTrue(result)

    def test__worksheet_not_exists(self):
        sut = GSheetRepository(self._url)
        result = sut.exists('Fake_Name_should_not_exist')
        self.assertFalse(result)

    def test_store_dataframe(self):
        storage_location = 'test_store_dataframe'
        self._sheets_created.append(storage_location)

        yesterday = datetime.combine(date.today() - timedelta(days=1), datetime.min.time())
        today = datetime.combine(date.today(), datetime.min.time())
        df = pd.DataFrame({
            'date': [yesterday, today],
            'some_metric': [0.1, 3.2]
        })

        sut = GSheetRepository(self._url)
        sut.store_dataframe(df, storage_location, allow_create=True, store_index=False)

        result = sut.get_dataframe(storage_location)
        assert_frame_equal(df, result)

    def test_store_dataframe_with_index(self):
        storage_location = 'test_store_dataframe_with_index'
        self._sheets_created.append(storage_location)

        yesterday = datetime.combine(date.today() - timedelta(days=1), datetime.min.time())
        today = datetime.combine(date.today(), datetime.min.time())
        df = pd.DataFrame({
            'date': [yesterday, today],
            'some_metric': [0.1, 3.2]
        }).set_index('date')

        sut = GSheetRepository(self._url)
        sut.store_dataframe(df, storage_location, allow_create=True, store_index=True)

        # want to ignore the empty columns and rows for now.. so just focus on what we just put in
        result = sut.get_dataframe(storage_location)
        assert_frame_equal(df.reset_index(), result)
