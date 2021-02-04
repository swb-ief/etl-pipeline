import os
import unittest
import pandas as pd
from datetime import datetime, timedelta, date

import pytest
from pandas.testing import assert_frame_equal

from backend.repository import AWSFileRepository


@pytest.mark.skipif('CI' not in os.environ, reason="Can only run on github (due to secrets)")
class TestAWSFileRepository(unittest.TestCase):
    bucket = 'covid-lens-backend-test'

    storage_locations_to_delete = []

    def tearDown(self) -> None:
        repository = AWSFileRepository(self.bucket)
        for location in self.storage_locations_to_delete:
            try:
                repository.delete_storage_location(location)
            except:
                # just clean up any mess we might have left behind as good as possible so ignore all exceptions
                pass

    def test_store_dataframe(self):
        storage_location = 'test_store_dataframe'
        self.storage_locations_to_delete.append(storage_location)

        yesterday = datetime.combine(date.today() - timedelta(days=1), datetime.min.time())
        today = datetime.combine(date.today(), datetime.min.time())
        df = pd.DataFrame({
            'date': [yesterday, today],
            'some_metric': [0.1, 3.2]
        })

        sut = AWSFileRepository(self.bucket)

        sut.store_dataframe(df, storage_location, allow_create=True, store_index=False)

        result = sut.get_dataframe(storage_location)
        assert_frame_equal(df, result)

    def test_does_not_exist(self):
        sut = AWSFileRepository(self.bucket)
        self.assertFalse(sut.exists('fake_location'))

    def test_exist(self):
        storage_location = 'test_exist'
        self.storage_locations_to_delete.append(storage_location)

        yesterday = datetime.combine(date.today() - timedelta(days=1), datetime.min.time())
        today = datetime.combine(date.today(), datetime.min.time())
        df = pd.DataFrame({
            'date': [yesterday, today],
            'some_metric': [0.1, 3.2]
        })

        sut = AWSFileRepository(self.bucket)
        sut.store_dataframe(df, storage_location, allow_create=True, store_index=False)
        self.assertTrue(sut.exists(storage_location))

        sut.delete_storage_location(storage_location)

    def test_delete(self):
        """ This is almost a full integration test, but good to have individual tests as well
        Remember the order of tests run is never fixed
        """
        storage_location = 'test_delete'
        self.storage_locations_to_delete.append(storage_location)

        yesterday = datetime.combine(date.today() - timedelta(days=1), datetime.min.time())
        today = datetime.combine(date.today(), datetime.min.time())
        df = pd.DataFrame({
            'date': [yesterday, today],
            'some_metric': [0.1, 3.2]
        })

        sut = AWSFileRepository(self.bucket)
        self.assertFalse(sut.exists(storage_location))
        sut.store_dataframe(df, storage_location, allow_create=True, store_index=False)
        self.assertTrue(sut.exists(storage_location))

        sut.delete_storage_location(storage_location)
        self.assertFalse(sut.exists(storage_location))
