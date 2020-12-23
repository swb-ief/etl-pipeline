import unittest
import pandas as pd
from numpy.testing import assert_array_equal
import numpy as np

# pycharm does not like this wants an extra pipeline.import
# but it works when running pytest in the root (and thus in the pipeline)
# this will be fixed when we start moving code to backend package.
from pipeline.calculate_metrics_file import cubic_spline


class TestCalculateMetrics(unittest.TestCase):
    def test_cubic_spline(self):
        # arrange
        column_data = pd.Series([1, 2, 3])
        expected = np.ndarray((3,), buffer=np.array([1., 2., 3.]))

        # act
        result = cubic_spline(column_data)

        # assert
        assert_array_equal(expected, result)
