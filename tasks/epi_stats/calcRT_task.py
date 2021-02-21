import logging
from datetime import date
import subprocess
import pandas as pd

import luigi
from tasks.epi_stats.fetch_citystats import DownloadCityStatsTask

log = logging.getLogger(__name__)

class CalcRTTask(luigi.Task):

    # city stats local file path
    citystats_path = '/usr/data/citystats.csv'

    def requires(self):
        return DownloadFileTask(
            file_name = citystats_path)

    file_name = luigi.Parameter()
    # "/usr/data/epiestim_out.csv"

    def output(self):
        return luigi.LocalTarget(self.file_name)

    def run(self):
        # Run RT Calculation
        subprocess.call("Rscript R_scripts/Epistim_rt_script.R", shell=False)
        log.info("Ran RT Calculation")

    def complete(self):
        # checks if RT Calc ran successfully
        return self.output().exists()


