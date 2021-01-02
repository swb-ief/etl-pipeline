import os
import unittest

from backend.data.extract_mumbai_wards_pdf import scrap_positive_wards_to_df

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class TestExtractMumbaiWardsPDF(unittest.TestCase):
    def test_extract_sample(self):
        sample = os.path.join(THIS_DIR, './samples/dashboard.pdf')
        result = scrap_positive_wards_to_df(sample)
        self.assertTrue(False)
