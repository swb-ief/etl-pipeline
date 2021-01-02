import unittest

from backend.config import get_config


class TestConfig(unittest.TestCase):
    def test_config(self):
        result = get_config()
        self.assertIsNotNone(result)

    def test_config_gsheet(self):
        result = get_config()
        self.assertTrue('google sheets' in result)
        self.assertTrue('url production' in result['google sheets'])
        self.assertTrue('url development' in result['google sheets'])
     