import os
import unittest
import json

from backend.data import ExtractCovid19IndiaData

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class TestExtractCovid19IndiaData(unittest.TestCase):

    def test_load_sample(self):
        with open(os.path.join(THIS_DIR, 'samples/covid19india_data-all.json')) as file:
            covid_data = json.load(file)

        sut = ExtractCovid19IndiaData()
        df_state, df_district = sut.process(covid_data)

        expected_state = (10347, 12)
        expected_district = (156_792, 11)
        self.assertEqual(expected_state, df_state.shape)
        self.assertEqual(expected_district, df_district.shape)
