import logging
from datetime import date
import subprocess
import pandas as pd
import os

import luigi
from tasks.epi_stats.fetch_citystats import DownloadCityStatsTask
from R_scripts.dt_script import run_DT

log = logging.getLogger(__name__)

class CalcDTTask(luigi.Task):

    # city stats local file path
    citystats_path = '/usr/data/citystats.csv' 

    def requires(self):
        return DownloadCityStatsTask(file_name = self.citystats_path)

    file_name = luigi.Parameter()
    # /usr/data/doubling_time.csv

    def output(self):
        return luigi.LocalTarget(self.file_name)

    def run(self):
        # cwd 
        cwd = os.getcwd()
        # Run DT Calculation
        run_DT(loc_type='district')
        # subprocess.call("Rscript {}/R_scripts/dt_script.R".format(cwd), shell=True)
        # log.info("Ran DT Calculation")

