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

def rolling_avgratio(data):
    window = 14
    shift = 14
    series = data['delta.confirmed']
    # re index 
    series = series.reindex(data['date'])
    week_avg = series.rolling(window=window, min_periods=7).mean()
    prev_avg = series.shift(periods=shift, freq='D').rolling(window=window, min_periods=7).mean()
    ratio = week_avg / prev_avg
    data['newcase_ratio'] = ratio

    return data

def critical_districts(data):
    """
    1) Criteria for identifying cities/districts with (a) increasing Covid-19 Incidence:
    a. Daily new cases  >  100 
    b. New Cases in the last 14 days  >  New cases in the preceding 14 days
    AND
    2) Criteria for identifying cities with high incidence burden:
    a. Top 20 cities/districts with the highest incidence burden as of date in terms of cumulative cases
    """
    # latest date
    data = data[data['date'] == data['date'].max()]

    #? criteria 1 a 
    c1a = data['delta.confirmed'] > 100
    #? criteria 1 b
    #daily_new_cases_14dratio = data.groupby(['district']).apply(rolling_avgratio)
    c1b = data['total.confirmed.14_day_ratio'] > 1
    
    # apply criteria
    criteria = list(map(lambda coll: all(coll), zip(c1a, c1b, latest_crit)))
    # critical cities, re criteria set 1
    critical_cities = data[criteria]
    
    # criteria 2a: highest 20 cumulative cases
    critical_cities = critical_cities.sort_values(by = ['total.confirmed']).reset_index(drop=True)
    critical_cities = critical_cities.head(20)

    critical_cities = critical_cities['district'].drop_duplicates().to_list()
    #? critical city data
    data_critical = data[data['district'].isin(critical_cities)].reset_index(drop=True)

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
            # -- sort by date
            city_stats = city_stats.sort_values(by = ['date'])
            city_stats = city_stats.groupby(['district']).apply(rolling_avgratio)

            print("mumbai data")
            print(city_stats[city_stats['district'] == "Mumbai"])

            # critical cities
            critical_city_stats = critical_districts(data=city_stats)
            city_stats = city_stats[list(map(lambda x: x == "Mumbai", city_stats['district']))].reset_index(drop=True)
            # download to local fs
            city_stats.to_csv(self.output().path, index=False)     
        else:
            log.error("Missing City Stats Data")
        
        # test 

        new_data = critical_districts(data=city_stats)
        print("critical cities")
        print(new_data.head())
        print(new_data.columns)

        return None

    def complete(self):
        return self.output().exists()



