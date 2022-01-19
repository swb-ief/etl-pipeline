import logging
from datetime import date

from backend.data import get_static_ward_data
from backend.data.utility import create_delta_cols, interpolate_values_generic

import luigi
import pandas as pd

from backend.repository import GSheetRepository, AWSFileRepository
from backend.config import get_config
from tasks.districts import FetchMumbaiOverallTask

log = logging.getLogger(__name__)


class FetchDistrictOverviewTask(luigi.Task):
    def requires(self):
        return {
            'Mumbai': FetchMumbaiOverallTask(),
        }

    def run(self):
        # TODO: it needs to partialy fail if one of its dependencies fails
        # as in it should continue processing all the other wards even if a few failed.

        overall_storage_location = 'raw_district_overview_stats'
        config = get_config()
        repository = AWSFileRepository(config['aws']['bucket production'])

        if repository.exists(overall_storage_location):
            overall_df = repository.get_dataframe(overall_storage_location)
            overall_df = overall_df.set_index(['date', 'metric', 'metric_type'])
        else:
            overall_df = None

        for district, overall_task in self.input().items():
            log.info(f'Processing: {district}')

            with overall_task.open('r') as json_file:
                overall_task = pd.read_csv(json_file, parse_dates=['date'])
                overall_task = overall_task.set_index(['date', 'metric', 'metric_type'])

            # This needs to support overwriting existing data as well as adding new data
            # TODO make a test for it
            if overall_df is None:
                overall_df = overall_task
            else:
                overall_df = overall_df.combine_first(overall_task)  # update old values and add new values

        # cleanup
        for task in self.input().values():
            task.remove()

        # store the raw data, no imputation done yet
        repository.store_dataframe(overall_df, overall_storage_location, allow_create=True, store_index=True)
        
        data_mini = overall_df.reset_index(drop=False)
        
        data_mini = data_mini.loc[data_mini['metric'].isin(['active.ccc1.facilities','active.ccc2.facilities','contact.traced.high.risk','contact.traced.low.risk',
                                                             'total.contact.traced','containment.zones.active.slums.chawls',
                                                             'containment.zones.active.micro.sealed.buildings','floors.sealed','bed.available.dchc.dch.ccc2',
                                                             'bed.occupied.dchc.dch.ccc2','bed.available.dchc.dch','bed.occupied.dchc.dch','bed.available.icu',
                                                             'bed.occupied.icu','bed.available.o2','bed.occupied.o2','bed.available.ventilator',
                                                             'bed.occupied.ventilator','active.critical','total.deaths','total.discharged',
                                                             'active.symptomatic','active.asymptomatic','total.tests','total.active','total.positive',
                                                             'currently.quarantined.home'])]

        data_mini['value'] = data_mini.apply(lambda x: x['past.24hrs'] if x['metric'] in ['contact.traced.high.risk', 'contact.traced.low.risk', 'total.contact.traced'] 
                                                       else (x['num.facilities'] if x['metric'] in ['active.ccc1.facilities', 'active.ccc2.facilities'] else x['count']), axis=1)


        data_mini2 = data_mini.pivot(index='date', columns='metric', values='value')

        # impute delta's atleast for Mumbai this is needed it only provides totals
        delta_needed_for = ['deaths', 'discharged', 'tests', 'positive']
        group_by_cols = []
        #try:
        data_mini2 = interpolate_values_generic(data_mini2, group_by_cols, delta_needed_for)
        data_mini2 = create_delta_cols(data_mini2, group_by_cols, delta_needed_for)
        #except:
        #    print("error in delta/interpolate")
        #    pass
        
        # add population
        data_mini2.to_csv(self.output().path, index=True)

    def output(self):
        return luigi.LocalTarget(f'district_overview_{date.today()}.csv')

    def complete(self):
        return self.output().exists()
