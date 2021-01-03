import pandas as pd
import luigi
from datetime import date
import logging

from backend import GSheetRepository
from backend.config import get_config
from tasks.districts import FetchMumbaiWardsTask

log = logging.getLogger(__name__)


class FetchWardDataTask(luigi.Task):
    def requires(self):
        return {
            'Mumbai': FetchMumbaiWardsTask(),
        }

    def run(self):
        ward_storage_location = 'raw_ward_data'
        config = get_config()
        repository = GSheetRepository(config['google sheets']['url production'])

        if repository.exists(ward_storage_location):
            all_wards = repository.get_dataframe(ward_storage_location)
            all_wards = all_wards.set_index(['state', 'district', 'ward', 'date'])
        else:
            all_wards = pd.DataFrame({
                'state': [],
                'district': [], 'ward': [], 'date': []
            }, index=['state', 'district', 'ward', 'date'])

        for district, ward_data in self.input().items():
            log.info(f'Processing: {district}')

            with ward_data.open('r') as json_file:
                ward_data = pd.read_csv(json_file)
                ward_data = ward_data.set_index(['state', 'district', 'ward', 'date'])

            # This needs to support overwriting existing data as well as adding new data
            # TODO make a test for it
            all_wards.concat(ward_data, axis=1)

        # store the raw data, no imputation done yet
        repository.store_dataframe(all_wards, ward_storage_location, allow_create=True)

        # impute delta's atleast for Mumbai this is needed it only provides totals
        all_wards.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget(f'wards_{date.today()}.csv')

    def complete(self):
        return self.output().exists()
