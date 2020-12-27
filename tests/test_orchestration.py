import unittest

import pytest

from backend.orgestration import update_data


class TestOrchestration(unittest.TestCase):
    @pytest.mark.skip()
    def test_update_data(self):
        result_metrics, result_hospitalizations = update_data(None, None, None, None)
