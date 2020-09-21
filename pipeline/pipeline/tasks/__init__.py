import luigi

from .cities_metrics import CalculateCityMetricsTask
from .stopcoronavirus_mcgm_scrapping import ExtractDataFromPdfDashboardWrapper


class SWBPipelineWrapper(luigi.WrapperTask):
    date = luigi.DateParameter()
    start_date = luigi.DateParameter()
    states_and_districts = luigi.DictParameter()
    elderly_page = luigi.IntParameter(default=22)
    daily_case_growth_page = luigi.IntParameter(default=25)
    positive_breakdown_index = luigi.IntParameter(default=22)

    def requires(self):
        yield CalculateCityMetricsTask(
            date=self.date,
            start_date=self.start_date,
            states_and_districts=self.states_and_districts,
        )
        yield ExtractDataFromPdfDashboardWrapper(
            date=self.date,
            elderly_page=self.elderly_page,
            daily_case_growth_page=self.daily_case_growth_page,
            positive_breakdown_index=self.positive_breakdown_index,
        )
