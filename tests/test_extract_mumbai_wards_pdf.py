import os
import unittest

from backend.data.extract_mumbai_wards_pdf import scrape_mumbai_pdf
import pandas as pd
import numpy as np

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class TestExtractMumbaiWardsPDF(unittest.TestCase):
    def test_extract_sample_2020_01_02(self):
        expected = (24, 10)
        expected_first = ['RC', '21197', '19981', '600', '586', 0, np.nan, pd.to_datetime('2021-01-01 00:00:00'),
                          'Mumbai', 'MH']
        expected_last = ['B', '2153', '1942', '144', '60', 0, np.nan, pd.to_datetime('2021-01-01 00:00:00'), 'Mumbai',
                         'MH']

        sample = os.path.join(THIS_DIR, 'samples/mumbai_dashboard_2020_01_02.pdf')
        mumbai_output = scrape_mumbai_pdf(sample)
        
        if len(mumbai_output)>1:
            result = mumbai_output[0]
        else:
            result = mumbai_output
            
        result_first = result.iloc[0].values
        result_last = result.iloc[-1].values

        self.assertIsNotNone(result)

        self.assertEqual(expected, result.shape)

        np.array_equal(result_first, expected_first)
        np.array_equal(result_last, expected_last)

    def test_extract_sample_2020_01_04(self):
        expected = (24, 10)
        expected_first = ['RC', '21264', '20064', '601', '568', 0, np.nan, pd.to_datetime('2021-01-03 00:00:00'),
                          'Mumbai', 'MH']
        expected_last = ['B', '2158', '1949', '144', '58', 0, np.nan, pd.to_datetime('2021-01-03 00:00:00'), 'Mumbai',
                         'MH']

        sample = os.path.join(THIS_DIR, 'samples/mumbai_dashboard_2020_01_04.pdf')
        result = scrape_mumbai_pdf(sample)
        result_first = result.iloc[0].values
        result_last = result.iloc[-1].values

        self.assertIsNotNone(result)

        self.assertEqual(expected, result.shape)

        np.array_equal(result_first, expected_first)
        np.array_equal(result_last, expected_last)

    def test_extract_sample_2020_01_20(self):
        # alignment issues in this pdf
        expected = (24, 10)
        expected_first = ['RC', '21813', '20679', '615', '488', 0, np.nan, pd.to_datetime('2021-01-18 00:00:00'),
                          'Mumbai', 'MH']
        expected_last = ['B', '2195', '1995', '144', '49', 0, np.nan, pd.to_datetime('2021-01-18 00:00:00'), 'Mumbai',
                         'MH']
        sample = os.path.join(THIS_DIR, 'samples/mumbai_dashboard_2021_01_20.pdf')
        result = scrape_mumbai_pdf(sample)
        result_first = result.iloc[0].values
        result_last = result.iloc[-1].values

        self.assertIsNotNone(result)

        self.assertEqual(expected, result.shape)

        np.array_equal(result_first, expected_first)
        np.array_equal(result_last, expected_last)

    def test_extract_sample_2020_02_27(self):
        # alignment issues in this pdf
        expected = (24, 10)
        expected_first = ['RC', '22879', '21544', '648', '649', 0, np.nan, pd.to_datetime('2021-02-25 00:00:00'),
                          'Mumbai', 'MH']
        expected_last = ['B', '2302', '2084', '146', '65', 0, np.nan, pd.to_datetime('2021-02-25 00:00:00'), 'Mumbai',
                         'MH']
        sample = os.path.join(THIS_DIR, 'samples/mumbai_dashboard_2021_02_27.pdf')
        result = scrape_mumbai_pdf(sample)
        result_first = result.iloc[0].values
        result_last = result.iloc[-1].values

        self.assertIsNotNone(result)

        self.assertEqual(expected, result.shape)

        np.array_equal(result_first, expected_first)
        np.array_equal(result_last, expected_last)

    def test_extract_sample_2020_03_02(self):
        # alignment issues in this pdf
        expected = (24, 10)
        expected_first = ['RC', '23059', '21672', '648', '700', 0, np.nan, pd.to_datetime('2021-02-28 00:00:00'),
                          'Mumbai', 'MH']
        expected_last = ['B', '2321', '2097', '146', '71', 0, np.nan, pd.to_datetime('2021-02-28 00:00:00'), 'Mumbai',
                         'MH']
        sample = os.path.join(THIS_DIR, 'samples/mumbai_dashboard_2021_02_28.pdf')
        result = scrape_mumbai_pdf(sample)
        result_first = result.iloc[0].values
        result_last = result.iloc[-1].values

        self.assertIsNotNone(result)

        self.assertEqual(expected, result.shape)

        np.array_equal(result_first, expected_first)
        np.array_equal(result_last, expected_last)
