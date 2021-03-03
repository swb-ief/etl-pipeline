import logging
from datetime import date

import pandas as pd

import luigi
from backend.config import get_config
from backend.repository import AWSFileRepository

log = logging.getLogger(__name__)

def critical_districts(data):
    # TODO!
    """
    1) Criteria for identifying cities/districts with (a) increasing Covid-19 Incidence:
    a. Daily new cases  >  100 
    b. New Cases in the last 14 days  >  New cases in the preceding 14 days
    AND
    2) Criteria for identifying cities with high incidence burden:
    a. Top 20 cities/districts with the highest incidence burden as of date in terms of cumulative cases
    """

    #? criteria 1 a 
    c1a = list(map(lambda x: x > 100, data['daily_new_cases']))
    #? criteria 1 b
    
    #? criteria 2: 

    return 

class DownloadCityStatsTask(luigi.Task):
    file_name = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.file_name)

    def run(self):
        
        city_stats_location = 'Phase 2 - Districts'
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



