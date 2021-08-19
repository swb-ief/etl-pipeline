import os
import unittest
import json
import pandas as pd

from backend.data import ExtractCovid19IndiaData

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class TestExtractCovid19IndiaData(unittest.TestCase):

    def test_load_sample(self):
        
        states_rawdata = pd.read_csv(os.path.join(THIS_DIR, 'samples/states_data_sample.csv'), parse_dates=["date"])
        districts_rawdata = pd.read_csv(os.path.join(THIS_DIR, 'samples/districts_data_sample.csv'), parse_dates=["date"])

        sut = ExtractCovid19IndiaData()
        df_state, df_district = sut.process(states_rawdata, districts_rawdata)

        expected_state = (1000, 14)
        expected_district = (10000, 15)
        self.assertEqual(expected_state, df_state.shape)
        self.assertEqual(expected_district, df_district.shape)
