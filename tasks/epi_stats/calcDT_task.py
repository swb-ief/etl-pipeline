import logging
from datetime import date
import subprocess
import pandas as pd
import os

import luigi
from tasks.epi_stats.fetch_citystats import DownloadCityStatsTask

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
        print(os.listdir())
        # chdir
        os.chdir("{}/R_scripts".format(cwd))
        print("==== NEW WD")
        print(os.listdir())
        # Run DT Calculation
        subprocess.call("Rscript {}/Rt_calcs.R".format(cwd), shell=True)
        log.info("Ran DT Calculation")

    def complete(self):
        # checks if DT Calc ran successfully
        return self.output().exists()



