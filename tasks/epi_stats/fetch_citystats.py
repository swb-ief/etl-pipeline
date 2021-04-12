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
    data_latest = data[data['date'] == data['date'].max()]

    #? criteria 1 a 
    c1a = data_latest['delta.confirmed'] > 100
    #? criteria 1 b
    data_latest['total.confirmed.14_day_ratio'] = np.where(abs(data_latest['total.confirmed.14_day_ratio'].values) == np.inf, np.nan, data_latest['total.confirmed.14_day_ratio'].values)
    c1b = data_latest['total.confirmed.14_day_ratio'] > 1
    
    # criteria 2
    top20_cutoff = data_latest["total.confirmed"].sort_values(ascending=False).iloc[20]
    c2 = data_latest['total.confirmed'] > top20_cutoff

    # apply criteria
    c1 = (c1a & c1b)
    criteria = (c1 | c2)
    # critical cities, re criteria set 1
    critical_cities = data_latest[criteria][['district', 'delta.confirmed']]
    critical_cities.sort_values(by=['delta.confirmed'], ascending=False, inplace=True)
    critical_cities_capped = critical_cities.head(20)
    #? critical city data
    data_critical = data[data['district'].isin(critical_cities_capped.district.drop_duplicates().to_list())].reset_index(drop=True)
    
    # for testing purposes
    data_critical = data_critical.head(2)

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

            # critical cities
            critical_city_stats = critical_districts(data=city_stats)
            # download to local fs
            critical_city_stats.to_csv(self.output().path, index=False)     
        else:
            log.error("Missing City Stats Data")
        
        # test 
        new_data = critical_districts(data=city_stats)
        print("critical cities")
        print(new_data)
        print(new_data.columns)

        return None

    def complete(self):
        return self.output().exists()



