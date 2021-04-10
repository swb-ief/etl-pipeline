import logging
from datetime import date
import subprocess
import pandas as pd
import os
import luigi
from tasks.epi_stats.fetch_wardstats import DownloadWardStatsTask

log = logging.getLogger(__name__)

class CalcRTWardTask(luigi.Task):

    # city stats local file path
    wardstats_path = '/usr/data/wardstats.csv'

    def requires(self):
        return DownloadWardStatsTask(file_name = self.wardstats_path)

    file_name = luigi.Parameter()
    # "/usr/data/epiestim_out.csv"

    def output(self):
        return luigi.LocalTarget(self.file_name)

    def run(self):
        # cwd 
        cwd = os.getcwd()

        # Run RT Calculation
        subprocess.call("Rscript {}/R_scripts/Epistim_rt_wards_script.R".format(cwd), shell=True)
        log.info("Ran RT Calculation for wards")
