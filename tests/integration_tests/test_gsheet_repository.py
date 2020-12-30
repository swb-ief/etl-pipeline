import os
import unittest
import pytest
import numpy as np
import pandas as pd
from numpy.testing import assert_array_equal

from backend import GSheetRepository


class TestGSheetRepository(unittest.TestCase):

    def test_get_worksheet_url_from_env(self):
        """ mix of both integration and regular test """
        result = GSheetRepository.get_worksheet_url_from_env()
        self.assertIsNotNone(result)

    @pytest.mark.skipif('CI' not in os.environ, reason="Can only run on github (due to secrets)")
    def test__get_gspread_client(self):
        sut = GSheetRepository(GSheetRepository.get_worksheet_url_from_env())
        result = sut._get_gspread_client()
        self.assertIsNotNone(result)

    @pytest.mark.skip("TODO: make conditional and only run on github")
    def test__get_worksheet(self):
        assert False

    @pytest.mark.skip("TODO: make conditional and only run on github")
    def test_get_dataframe(self):
        assert False

    @pytest.mark.skip("Probably never make a test for this")
    def test_store_dataframe(self):
        assert False
