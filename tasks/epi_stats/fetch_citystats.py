import logging
from datetime import date

import pandas as pd

import luigi
from backend.config import get_config
from backend.repository import AWSFileRepository

log = logging.getLogger(__name__)

class DownloadCityStatsTask(luigi.Task):
    file_name = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(file_name)

    def run(self):
        
        city_stats_location = 'Phase 2 - Districts.csv'
        config = get_config()
        repository = AWSFileRepository(config['aws']['bucket production'])

        if repository.exists(city_stats_location):
            city_stats = repository.get_dataframe(city_stats_location)
            # TODO --> Edit implementations of RT and DT to accomodate all districts
            city_stats = city_stats[list(map(lambda x: x == "Mumbai", city_stats['district']))].reset_index(drop=True)
            # download to local fs
            city_stats.to_csv(self.output().path, index=False)     
        else:
            log.error("Missing City Stats Data")

    def complete(self):
        return self.output().exists()



