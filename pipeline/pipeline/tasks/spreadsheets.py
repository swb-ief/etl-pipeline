from datetime import date
from typing import Tuple
import tempfile


import luigi
import pandas
import gspread


from pipeline.dashboard_pdf_scrapper import (
    scrap_positive_wards_to_df,
    scrape_elderly_table,
    positive_breakdown_fix_dtypes,
    scrape_case_growth_to_df,
    scrape_elderly_table_df,
)

from .dropbox import dropbox_target, textio2binary
from .stopcoronavirus_mcgm_scrapping import DownloadMcgmDashboardPdfTask

from ..config import GSPREAD_CLIENT


def worksheet_as_df_by_url(sheet_url: str, worksheet_name: str) -> Tuple[gspread.Worksheet, pandas.DataFrame]:
    sheet = GSPREAD_CLIENT.open_by_url(sheet_url)
    worksheet = sheet.worksheet(worksheet_name)
    return worksheet, pandas.DataFrame(worksheet.get_all_records())


WORKSHEET_URL = 'https://docs.google.com/spreadsheets/d/1HeTZKEXtSYFDNKmVEcRmF573k2ZraDb6DzgCOSXI0f0/edit#gid=0'

class ExtractWardPositiveBreakdownGSheetTask(luigi.ExternalTask):
    date = luigi.DateParameter(default=date.today())
    page_index = luigi.IntParameter(default=20)
    response = None

    def run(self):
        pdf_input = yield DownloadMcgmDashboardPdfTask(date=self.date)
        worksheet, positive_breakdown_df = positive_breakdown_fix_dtypes(worksheet_as_df_by_url(WORKSHEET_URL, "positive-breakdown")) 
        with pdf_input.open(
            "r"
        ) as input_file, tempfile.NamedTemporaryFile("ab+") as named_tmp_file:
            named_tmp_file.write(textio2binary(input_file))
            scrap_df = scrap_positive_wards_to_df(
                named_tmp_file, page=self.page_index
            )
            scrap_df["downloaded_for"] = self.date.strftime('%Y-%m-%d')
            result_df = pandas.concat([positive_breakdown_df, scrap_df])
            self.response = worksheet.update([result_df.columns.values.tolist()] + result_df.values.tolist())
            # TODO: Check number of rows written vs scrap_df
            return True
        return False
    
    def complete(self):
        return self.response != None


class ExtractCaseGrowthTableGSheetTask(luigi.ExternalTask):
    date = luigi.DateParameter(default=date.today())
    page = luigi.IntParameter(default=25)
    response = None

    def run(self):
        pdf_input = yield DownloadMcgmDashboardPdfTask(date=self.date)
        worksheet, daily_case_growth_df = worksheet_as_df_by_url(WORKSHEET_URL, "daily-case-growth")

        with pdf_input.open(
            "r"
        ) as input_file, tempfile.NamedTemporaryFile("ab+") as named_tmp_file:
            named_tmp_file.write(textio2binary(input_file))
            scrap_df = scrape_case_growth_to_df(named_tmp_file.name, page=self.page)
            scrap_df["downloaded_for"] = self.date.strftime('%Y-%m-%d')
            result_df = pandas.concat([daily_case_growth_df, scrap_df])
            self.response = worksheet.update([result_df.columns.values.tolist()] + result_df.values.tolist())
            return True
        return False
    
    def complete(self):
        return self.response != None


class ExtractElderlyTableGSheetTask(luigi.Task):
    date = luigi.DateParameter(default=date.today())
    page = luigi.IntParameter(default=22)
    response = None
    
    def run(self):
        pdf_input = yield DownloadMcgmDashboardPdfTask(date=self.date)
        worksheet, elderly_df = worksheet_as_df_by_url(WORKSHEET_URL, "elderly")

        with pdf_input.open(
            "r"
        ) as input_file, tempfile.NamedTemporaryFile("ab+") as named_tmp_file:
            named_tmp_file.write(textio2binary(input_file))
            scrap_df = scrape_elderly_table_df(named_tmp_file.name, page=self.page)
            scrap_df["downloaded_for"] = self.date.strftime('%Y-%m-%d')
            result_df = pandas.concat([elderly_df, scrap_df])
            result_df = result_df.fillna('NA')
            self.response = worksheet.update([result_df.columns.values.tolist()] + result_df.values.tolist())
            return True
        return False

    def complete(self):
        return self.response != None