import logging
from datetime import date
import subprocess
import pandas as pd

import luigi
from backend.config import get_config
from backend.repository import AWSFileRepository
from tasks.epi_stats.calcRT_ward_task import CalcRTWardTask
# from tasks.epi_stats.calcDT_ward_task import CalcDTWardTask

log = logging.getLogger(__name__)

class UpdateEpiStatsWardsTask(luigi.Task):

    # local file paths
    local_rt_path = "/usr/data/epiestim_out_wards.csv"
    local_dt_path = "/usr/data/doubling_time_wards.csv"
    # aws s3 paths
    s3_rt_path = 'Phase2_RT_Wards'
    s3_dt_path = 'Phase2_DT_Wards'
    s3_wards_path = 'Phase 2 - Wards'
    s3_wards_rt_path = 'Phase 2 - Wards_RT-DT'

    def requires(self):
        return CalcRTWardTask(file_name = self.local_rt_path)#, CalcDTTask(file_name = self.local_dt_path)

    def run(self):
        config = get_config()
        repository = AWSFileRepository(config['aws']['bucket production'])

        # import master districts data
        all_wards = repository.get_dataframe(self.s3_wards_path)
        del all_wards['mean.RT']
        del all_wards['upper.RT']
        del all_wards['lower.RT']
        del all_wards['dt']
        
        # read RT results
        rt_results0 = pd.read_csv(self.local_rt_path, parse_dates=["date"])
        
        # specify column name containing Rt values
        rt_colname = ['mean', 'upper', 'lower']
        rt_results = rt_results0[['ward', 'date']+rt_colname]
        rt_results.columns = ['ward', 'date', 'mean.RT', 'upper.RT', 'lower.RT']

        # join rt data with all districts data
        all_wards = all_wards.merge(rt_results, on=['ward', 'date'], how='left')
    
        # read DT results
        dt_results0 = pd.read_csv(self.local_dt_path, parse_dates=["date"])
        # pick only the relevant columns
        dt_results = dt_results0[["ward", "date", "dt"]]
        all_wards = all_wards.merge(dt_results, on=['ward', 'date'], how='left')
    
        # push RT to Repo
        repository.store_dataframe(all_wards, self.s3_wards_rt_path, allow_create=True)

       
