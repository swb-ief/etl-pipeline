import logging
from datetime import date
import subprocess
import pandas as pd

import luigi
from tasks.epi_stats.fetch_citystats import DownloadCityStatsTask

log = logging.getLogger(__name__)

class CalcDTTask(luigi.Task):

    # city stats local file path
    citystats_path = '/usr/data/citystats.csv'

    def requires(self):
        return DownloadFileTask(
            file_name = citystats_path)

    file_name = luigi.Parameter()
    # /usr/data/doubling_time.csv

    def output(self):
        return luigi.LocalTarget(file_name)

    def run(self):
        # Run DT Calculation
        subprocess.call("Rscript R_scripts/Rt_calcs.R", shell=False)

    def complete(self):
        # checks if DT Calc ran successfully
        return self.output().exists()



