import logging
from datetime import date
import subprocess
import pandas as pd

import luigi
from backend.config import get_config
from backend.repository import AWSFileRepository
from tasks.epi_stats.calcRT_task import CalcRTTask
from tasks.epi_stats.calcDT_task import CalcDTTask

log = logging.getLogger(__name__)

class UpdateEpiStatsTask(luigi.Task):

    # local file paths
    local_rt_path = "/usr/data/epiestim_out.csv"
    local_dt_path = "/usr/data/doubling_time.csv"
    # aws s3 paths
    s3_rt_path = 'Phase2_RT.csv'
    s3_dt_path = 'Phase2_DT.csv'

    def requires(self):
        return CalcRTTask(file_name = self.local_rt_path), CalcDTTask(file_name = self.local_dt_path)

    def run(self):
        config = get_config()
        repository = AWSFileRepository(config['aws']['bucket production'])

        # read RT results
        rt_results = pd.read_csv(self.local_rt_path)
        # push RT to Repo
        repository.store_dataframe(rt_results, self.s3_rt_path, allow_create=True)

        # read DT results
        dt_results = pd.read_csv(self.local_dt_path)
        # push DT to Repo
        repository.store_dataframe(dt_results, self.s3_dt_path, allow_create=True)


    # def complete(self):
    #     # TODO --> palceholder for some kind of validation process,
    #     # TODO, similar to https://github.com/swb-ief/etl-pipeline/blob/master/tasks/update_dashboard_task.py
    #     return True



