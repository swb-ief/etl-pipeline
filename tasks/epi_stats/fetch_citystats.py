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

def rolling_avgratio(series, window, shift):
    week_avg = series.rolling(window='{}D'.format(str(window)), min_periods=7).mean()
    prev_avg = series.shift(periods=shift, freq='D').rolling(window='{}D'.format(str(window)), min_periods=7).mean()
    ratio = week_avg / prev_avg
    return ratio

def critical_districts(data):
    """
    1) Criteria for identifying cities/districts with (a) increasing Covid-19 Incidence:
    a. Daily new cases  >  100 
    b. New Cases in the last 14 days  >  New cases in the preceding 14 days
    AND
    2) Criteria for identifying cities with high incidence burden:
    a. Top 20 cities/districts with the highest incidence burden as of date in terms of cumulative cases
    """
    #? criteria 1 a 
    c1a = data['delta.confirmed'] > 100
    #? criteria 1 b
    daily_new_cases_14dratio = rolling_avgratio(data['delta.confirmed'], window=14, shift=14)
    c1b = daily_new_cases_14dratio > 1
    # latest date
    latest_crit = data['date'] == data['date'].max()
    # apply criteria
    criteria = list(map(lambda coll: all(coll), zip(c1a, c1b, latest_crit)))
    # critical cities
    critical_cities = data[criteria]
    # criteria 2a: highest 20 cumulative cases
    print(critical_cities)
    critical_cities = critical_cities['districts'].drop_duplicates().to_list()
    #? critical city data
    data_critical = data[data['districts'].isin(critical_cities)].reset_index(drop=True)

    return data_critical

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
            critical_city_stats = critical_districts(data=city_stats)
            city_stats = city_stats[list(map(lambda x: x == "Mumbai", city_stats['district']))].reset_index(drop=True)
            # download to local fs
            city_stats.to_csv(self.output().path, index=False)     
        else:
            log.error("Missing City Stats Data")
        
        # test 
        print(city_stats.head())
        print(city_stats.columns)

        new_data = critical_districts(data=city_stats)
        print(new_data.head())
        print(new_data.columns)

        return None

    def complete(self):
        return self.output().exists()



