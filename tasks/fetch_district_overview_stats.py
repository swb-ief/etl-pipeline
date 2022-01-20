import logging
from datetime import date

from backend.data import get_static_ward_data
from backend.data.utility import create_delta_cols, interpolate_values

import luigi
import pandas as pd

from backend.repository import GSheetRepository, AWSFileRepository
from backend.config import get_config
from tasks.districts import FetchMumbaiOverallTask

log = logging.getLogger(__name__)


class FetchDistrictOverviewTask(luigi.Task):
    def requires(self):
        return {
            'Mumbai': FetchMumbaiOverallTask(),
        }

    def run(self):
        # TODO: it needs to partialy fail if one of its dependencies fails
        # as in it should continue processing all the other wards even if a few failed.

        overall_storage_location = 'raw_district_overview_stats'
        config = get_config()
        repository = AWSFileRepository(config['aws']['bucket production'])

        if repository.exists(overall_storage_location):
            overall_df = repository.get_dataframe(overall_storage_location)
            overall_df = overall_df.set_index(['date', 'metric', 'metric_type'])
        else:
            overall_df = None

        for district, overall_task in self.input().items():
            log.info(f'Processing: {district}')

            with overall_task.open('r') as json_file:
                overall_task = pd.read_csv(json_file, parse_dates=['date'])
                overall_task = overall_task.set_index(['date', 'metric', 'metric_type'])

            # This needs to support overwriting existing data as well as adding new data
            # TODO make a test for it
            if overall_df is None:
                overall_df = overall_task
            else:
                overall_df = overall_df.combine_first(overall_task)  # update old values and add new values

        # cleanup
        for task in self.input().values():
            task.remove()

        # store the raw data, no imputation done yet
        repository.store_dataframe(overall_df, overall_storage_location, allow_create=True, store_index=True)

        # impute delta's atleast for Mumbai this is needed it only provides totals
        delta_needed_for = ['count']
        group_by_cols = ['date', 'metric', 'metric_type']
        try:
            overall_df = interpolate_values(overall_df, group_by_cols, list(overall_df))
            overall_df = create_delta_cols(overall_df, group_by_cols, delta_needed_for)
        except:
            pass

        # add population
        overall_df.to_csv(self.output().path, index=True)

    def output(self):
        return luigi.LocalTarget(f'district_overview_{date.today()}.csv')

    def complete(self):
        return self.output().exists()
