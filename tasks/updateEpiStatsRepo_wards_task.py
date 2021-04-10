import logging
from datetime import date
import subprocess
import pandas as pd

import luigi
from backend.config import get_config
from backend.repository import AWSFileRepository
from tasks.epi_stats.calcRT_ward_task import CalcRTWardTask
from tasks.epi_stats.calcDT_ward_task import CalcDTWardTask

log = logging.getLogger(__name__)

class UpdateEpiStatsTask(luigi.Task):

    # local file paths
    local_rt_path = "/usr/data/epiestim_out_wards.csv"
    local_dt_path = "/usr/data/doubling_time_wards.csv"
    # aws s3 paths
    s3_rt_path = 'Phase2_RT_Wards'
    s3_dt_path = 'Phase2_DT_Wards'
    s3_wards_path = 'Phase 2 - Wards'
    s3_wards_rt_path = 'Phase 2 - Districts - With RT'

    def requires(self):
        return CalcRTTask(file_name = self.local_rt_path)#, CalcDTTask(file_name = self.local_dt_path)

    def run(self):
        config = get_config()
        repository = AWSFileRepository(config['aws']['bucket production'])

        # read RT results
        rt_results = pd.read_csv(self.local_rt_path, parse_dates=["date"])
        
        # specify column name containing Rt values
        rt_colname = 'median'
        
        # rename columns so as to allow join with master districts data
        rt_results.columns = ['district' if x=='city' else x for x in rt_results.columns]
        rt_results.columns = ['rt' if x==rt_colname else x for x in rt_results.columns]
        
        # import master districts data
        all_districts = repository.get_dataframe(self.s3_districts_path)
        
        # join rt data with all districts data
        all_districts = all_districts.merge(rt_results[['district', 'date', 'rt']], on=['district', 'date'], how='left')
    
        # push RT to Repo
        repository.store_dataframe(all_districts, self.s3_districts_rt_path, allow_create=True)

        # # read DT results
        # dt_results = pd.read_csv(self.local_dt_path)
        
        # # push DT to Repo
        # repository.store_dataframe(dt_results, self.s3_dt_path, allow_create=True)


    # def complete(self):
    #     # TODO --> palceholder for some kind of validation process,
    #     # TODO, similar to https://github.com/swb-ief/etl-pipeline/blob/master/tasks/update_dashboard_task.py
    #     return True



