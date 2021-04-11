import logging
from datetime import date
import subprocess
import pandas as pd

import luigi
from backend.config import get_config
from backend.repository import AWSFileRepository
from tasks.epi_stats.calcRT_task import CalcDistrictsRTTask
from tasks.epi_stats.calcDT_task import CalcDTTask

log = logging.getLogger(__name__)

class UpdateEpiStatsDistrictsTask(luigi.Task):

    # local file paths
    local_rt_path = "/usr/data/epiestim_out_districts.csv"
    local_dt_path = "/usr/data/doubling_time_districts.csv"

    # aws s3 paths
    s3_rt_path = 'Phase2_RT'
    s3_dt_path = 'Phase2_DT'
    s3_districts_path = 'Phase 2 - Districts'
    s3_districts_update_path = 'Phase 2 - Districts_RT-DT'

    def requires(self):
        return CalcDistrictsRTTask(file_name = self.local_rt_path)#, CalcDTTask(file_name = self.local_dt_path)

    def run(self):
        config = get_config()
        repository = AWSFileRepository(config['aws']['bucket production'])

        # import master districts data
        all_districts = repository.get_dataframe(self.s3_districts_path)

        # read RT results
        rt_results0 = pd.read_csv(self.local_rt_path, parse_dates=["date"])
        # specify column name containing Rt values
        rt_colname = ['mean', 'upper', 'lower']
        rt_results = rt_results0[['city', 'date']+rt_colname]
        rt_results.columns = ['district', 'date', 'mean.RT', 'upper.RT', 'lower.RT']
        # join rt data with all districts data
        all_districts = all_districts.merge(rt_results, on=['district', 'date'], how='left')
    
        # # read DT results
        # dt_results = pd.read_csv(self.local_dt_path)
        # join dt data with all districts data
        # all_districts = 

        # push RT/DT Critical Cities Updates to Repo
        repository.store_dataframe(all_districts, self.s3_districts_update_path, allow_create=True)




