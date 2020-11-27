from datetime import date
import tempfile

import luigi
import requests

from .gcloud import gcs_target, textio2binary
from pipeline.dashboard_pdf_scrapper import (
    scrap_positive_wards_to_csv,
    scrape_case_growth_to_csv,
    scrape_elderly_table,
)


class DownloadMcgmDashboardPdfTask(luigi.Task):
    url = luigi.Parameter(
        default="http://stopcoronavirus.mcgm.gov.in/assets/docs/Dashboard.pdf"
    )
    date = luigi.DateParameter(default=date.today())

    def output(self):
        return gcs_target(f"/data/dashboard-pdf/{self.date}-mcgm.stopcoronavirus.pdf")

    def run(self):
        response = requests.get(self.url)
        with self.output().temporary_path() as output_path:
            with open(output_path, "wb") as output_tmp_file:
                output_tmp_file.write(response.content)


class ExtractWardPositiveBreakdownTask(luigi.Task):
    date = luigi.DateParameter(default=date.today())
    page_index = luigi.IntParameter(default=22)

    def requires(self):
        return DownloadMcgmDashboardPdfTask(date=self.date)

    def output(self):
        return gcs_target(
            f"/data/positive-wards/ward-positive-breakdown-{self.date}.csv"
        )

    def run(self):
        with self.output().open("w") as output_file, self.input().open(
            "r"
        ) as input_file, tempfile.NamedTemporaryFile("ab+") as named_tmp_file:
            named_tmp_file.write(textio2binary(input_file))
            scrap_positive_wards_to_csv(
                named_tmp_file, output_file, page=self.page_index
            )


class ExtractCaseGrowthTableTask(luigi.Task):
    date = luigi.DateParameter(default=date.today())
    page = luigi.IntParameter(default=25)

    def requires(self):
        return DownloadMcgmDashboardPdfTask(date=self.date)

    def output(self):
        return gcs_target(f"/data/dashboard-case-growth/growth-{self.date}.csv")

    def run(self):
        with self.output().open("w") as output_file, self.input().open(
            "r"
        ) as input_file, tempfile.NamedTemporaryFile("ab+") as named_tmp_file:
            named_tmp_file.write(textio2binary(input_file))
            scrape_case_growth_to_csv(named_tmp_file.name, output_file, page=self.page)


class ExtractElderlyTableTask(luigi.Task):
    date = luigi.DateParameter(default=date.today())
    page = luigi.IntParameter(default=22)

    def requires(self):
        return DownloadMcgmDashboardPdfTask(date=self.date)

    def output(self):
        return gcs_target(f"/data/dashboard-elderly/elderly-{self.date}.csv")

    def run(self):
        with self.output().open("w") as output_file, self.input().open(
            "r"
        ) as input_file, tempfile.NamedTemporaryFile("ab+") as named_tmp_file:
            named_tmp_file.write(textio2binary(input_file))
            scrape_elderly_table(named_tmp_file.name, output_file, page=self.page)


class ExtractDataFromPdfDashboardWrapper(luigi.WrapperTask):
    date = luigi.DateParameter(default=date.today())
    elderly_page = luigi.IntParameter(default=22)
    daily_case_growth_page = luigi.IntParameter(default=25)
    positive_breakdown_index = luigi.IntParameter(default=22)

    def requires(self):
        yield ExtractWardPositiveBreakdownTask(
            date=self.date, page_index=self.positive_breakdown_index
        )
        yield ExtractCaseGrowthTableTask(
            date=self.date, page=self.daily_case_growth_page
        )
        yield ExtractElderlyTableTask(date=self.date, page=self.elderly_page)


if __name__ == "__main__":
    luigi.run()
