import logging
from datetime import date

from backend.data import get_static_ward_data
from backend.data.utility import create_delta_cols, interpolate_values

import luigi
import pandas as pd

from backend.repository import GSheetRepository, AWSFileRepository
from backend.config import get_config
from tasks.districts import FetchMumbaiWardsTask

log = logging.getLogger(__name__)


class FetchWardDataTask(luigi.Task):
    def requires(self):
        return {
            'Mumbai': FetchMumbaiWardsTask(),
        }

    def run(self):
        # TODO: it needs to partialy fail if one of its dependencies fails
        # as in it should continue processing all the other wards even if a few failed.

        ward_storage_location = 'raw_ward_data'
        config = get_config()
        repository = AWSFileRepository(config['aws']['bucket production'])

        if repository.exists(ward_storage_location):
            all_wards = repository.get_dataframe(ward_storage_location)
            all_wards = all_wards.set_index(['state', 'district', 'ward', 'date'])
        else:
            all_wards = None

        for district, ward_task in self.input().items():
            log.info(f'Processing: {district}')

            with ward_task.open('r') as json_file:
                ward_task = pd.read_csv(json_file, parse_dates=['date'])
                ward_task = ward_task.set_index(['state', 'district', 'ward', 'date'])

            # This needs to support overwriting existing data as well as adding new data
            # TODO make a test for it
            if all_wards is None:
                all_wards = ward_task
            else:
                all_wards = all_wards.combine_first(ward_task)  # update old values and add new values

        # cleanup
        for task in self.input().values():
            task.remove()

        # store the raw data, no imputation done yet
        repository.store_dataframe(all_wards, ward_storage_location, allow_create=True, store_index=True)

        # impute delta's atleast for Mumbai this is needed it only provides totals
        delta_needed_for = ['tested', 'confirmed', 'recovered', 'deceased', 'active', 'other']
        group_by_cols = ['state', 'district', 'ward']
        all_wards = interpolate_values(all_wards, group_by_cols, delta_needed_for)
        all_wards = create_delta_cols(all_wards, group_by_cols, delta_needed_for)

        # add population
        static_ward_data = get_static_ward_data()
        all_wards = all_wards.join(static_ward_data, on=group_by_cols, how='left')

        all_wards.to_csv(self.output().path, index=True)

    def output(self):
        return luigi.LocalTarget(f'wards_{date.today()}.csv')

    def complete(self):
        return self.output().exists()
