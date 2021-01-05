import os
import unittest

from backend.data.extract_mumbai_wards_pdf import scrape_mumbai_pdf

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class TestExtractMumbaiWardsPDF(unittest.TestCase):
    def test_extract_sample(self):
        expected = (24, 8)
        sample = os.path.join(THIS_DIR, 'samples/mumbai_dashboard_2020_01_02.pdf')
        result = scrape_mumbai_pdf(sample)
        self.assertIsNotNone(result)

        self.assertEqual(expected, result.shape)

    def test_extract_sample(self):
        expected = (24, 8)
        sample = os.path.join(THIS_DIR, 'samples/mumbai_dashboard_2020_01_04.pdf')
        result = scrape_mumbai_pdf(sample)
        self.assertIsNotNone(result)

        self.assertEqual(expected, result.shape)
