import logging
from datetime import date
import pandas as pd
import numpy as np 
import functools
import luigi
from backend.config import get_config
from backend.repository import AWSFileRepository
from backend.metrics.calculations import fourteen_day_avg_ratio

log = logging.getLogger(__name__)

class DownloadWardStatsTask(luigi.Task):
    file_name = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.file_name)

    def run(self):
        
        ward_stats_location = 'Phase 2 - Wards'
        config = get_config()
        repository = AWSFileRepository(config['aws']['bucket production'])

        if repository.exists(ward_stats_location):
            ward_stats = repository.get_dataframe(ward_stats_location)
            # -- sort by date
            ward_stats = ward_stats.sort_values(by = ['date'])

            # download to local fs
            ward_stats.to_csv(self.output().path, index=False)     
        else:
            log.error("Missing Ward Stats Data")

        return None

    def complete(self):
        return self.output().exists()
