import unittest
from unittest.mock import patch

import gspread
import luigi

from tasks.update_gsheet_task import UpdateGSheetTask


class TestUpdateGSheetTask(unittest.TestCase):

    def test_run(self):
        # we want to get data form gsheet, but we don't want to update it so mock the update function
        with patch.object(gspread.Worksheet, 'update', return_value=None) as mock_method:
            sut = UpdateGSheetTask()
            worker = luigi.worker.Worker()
            worker.add(sut)
            worker.run()

            result = sut.output()
            self.assertEqual('expected', result)
