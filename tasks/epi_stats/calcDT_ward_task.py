import logging
from datetime import date
import subprocess
import pandas as pd
import os

import luigi
from tasks.epi_stats.fetch_wardstats import DownloadWardStatsTask
from R_scripts.dt_script import run_DT

log = logging.getLogger(__name__)

class CalcDTWardTask(luigi.Task):

    # city stats local file path
    wardstats_path = '/usr/data/wardstats.csv'

    def requires(self):
        return DownloadWardStatsTask(file_name = self.wardstats_path)

    file_name = luigi.Parameter()
    # /usr/data/doubling_time.csv

    def output(self):
        return luigi.LocalTarget(self.file_name)

    def run(self):
        # cwd 
        cwd = os.getcwd()
        # Run DT Calculation
        run_DT(loc_type='ward')
