import unittest

import pytest

from backend.gsheet import _get_gspread_client


class TestGsheet(unittest.TestCase):
    @pytest.mark.skip("Won't run locally since we don't have access to the credentials")
    def test_get_client(self):
        result = _get_gspread_client()
        self.assertIsNotNone(result)
