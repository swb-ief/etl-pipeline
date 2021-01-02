import os
import unittest
import pytest

from backend import GSheetRepository


@pytest.mark.skipif('CI' not in os.environ, reason="Can only run on github (due to secrets)")
class TestGSheetRepository(unittest.TestCase):

    def test_get_worksheet_url_from_env(self):
        """ mix of both integration and regular test """
        result = GSheetRepository.get_worksheet_url_from_env()
        self.assertIsNotNone(result)

    def test__get_gspread_client(self):
        sut = GSheetRepository(GSheetRepository.get_worksheet_url_from_env())
        result = sut._get_gspread_client()
        self.assertIsNotNone(result)

    def test__get_worksheet(self):
        sut = GSheetRepository(GSheetRepository.get_worksheet_url_from_env())
        # small worksheet, but might be removed in the future, then switch to an other sheet with little data
        result = sut._get_worksheet('hospitalization')
        self.assertIsNotNone(result)

    def test_get_dataframe(self):
        sut = GSheetRepository(GSheetRepository.get_worksheet_url_from_env())
        # small worksheet, but might be removed in the future, then switch to an other sheet with little data
        result = sut.get_dataframe('hospitalization')
        self.assertIsNotNone(result)

    def test__worksheet_exists(self):
        sut = GSheetRepository(GSheetRepository.get_worksheet_url_from_env())
        result = sut.exists('hospitalization')
        self.assertTrue(result)

    def test__worksheet_not_exists(self):
        sut = GSheetRepository(GSheetRepository.get_worksheet_url_from_env())
        result = sut.exists('Fake_Name_should_not_exist')
        self.assertFalse(result)

    @pytest.mark.skip("Need a dev sheet in google sheets to safely test this.")
    def test_store_dataframe(self):
        assert False
