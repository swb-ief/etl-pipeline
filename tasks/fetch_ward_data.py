from io import TextIOWrapper

import pandas as pd

import luigi
from datetime import date
import logging

log = logging.getLogger(__name__)

from tasks.districts import FetchMumbaiWards


class FetchWardData(luigi.Task):
    def requires(self):
        return {
            'Mumbai': FetchMumbaiWards(),
        }

    def run(self):

        all_wards = None

        for district, ward_data in self.input().items():
            log.info(f'Processing: {district}')

            with ward_data.open('r') as json_file:
                ward_data = pd.read_csv(json_file)

            if all_wards:
                all_wards.concat(ward_data, axis=1)
            else:
                all_wards = ward_data

        all_wards.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget(f'wards_{date.today()}.csv')

    def complete(self):
        return self.output().exists()
