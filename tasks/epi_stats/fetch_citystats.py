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
    
    
    n = 20  #################### TO n, ASSIGN NUMBER OF CITIES SATISFYING CRITERIA 1 TO BE PICKED #########
    criteria1 = (c1a & c1b)
    critical_cities_c1 = data_latest[criteria1][['district', 'delta.confirmed']]
    critical_cities_c1.sort_values(by=['delta.confirmed'], ascending=False, inplace=True)
    critical_cities_c1_capped = critical_cities_c1.head(n)    
    
    # criteria 2
    top20_cutoff = data_latest["total.confirmed"].sort_values(ascending=False).iloc[20]
    c2 = data_latest['total.confirmed'] > top20_cutoff
    critical_cities_c2 = data_latest[c2][['district', 'delta.confirmed']]
    
    critical_cities_list = list(set(critical_cities_c1_capped.district.drop_duplicates().to_list()) | set(critical_cities_c2.district.drop_duplicates().to_list()))

    #? critical city data (pick 2 for test phase)
    data_critical = data[data['district'].isin(critical_cities_list[0:2])].reset_index(drop=True)

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



