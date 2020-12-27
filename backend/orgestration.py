import itertools
import logging
from datetime import datetime

import pandas as pd
from pandas import isnull

from backend.metrics.calculations import calculate_all_metrics

log = logging.getLogger(__name__)


def update_data(
        covid19data: pd.DataFrame,
        states_and_districts: dict,
        hospitalizations: pd.DataFrame,
        start_date: datetime,
) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
    """
    :returns: city_stats, metrics, hospitalization
    """

    logging.info("Reading states and districts")
    # 2. Now, filter the entries that are in the YAML
    for state, districts in states_and_districts.items():
        json_keys = ["delta", "total"]
        log.info("Extracting data for State: {}".format(state))

        # 2.1 Filter parent dataframe by state
        districts_series = covid19data[state].apply(pd.Series)["districts"]
        districts_series = districts_series.apply(lambda x_: {} if isnull(x_) else x_)

        # 2.2 Create a DF with district-level columns
        state_df = pd.json_normalize(districts_series)

        for district in districts:
            # 2.3 Create a regular expression to filter the district in the YAML
            y = [".".join(list(p)) for p in itertools.product([district], json_keys)]
            reg = "|".join("^" + i for i in y)

            # 2.4 Filter district using RE
            city_stats = state_df.filter(regex=reg)
            city_stats.insert(1, "district", district)
            city_stats.insert(2, "state", state)

            # 2.5 set index for easy concat
            city_stats.index = covid19data.index
            city_stats.index.set_names(["date"], inplace=True)

            # 2.6 add genenric col names
            new_col = [
                col.replace("{}.".format(district), "") for col in list(city_stats.columns)
            ]
            city_stats.rename(
                dict(zip(list(city_stats.columns), new_col)), axis=1, inplace=True
            )

            # Calculate metrics
            log.info("calculating metrics for {}".format(district))
            metrics, hospitalizations_updated = calculate_all_metrics(
                start_date=start_date,
                city_stats=city_stats,
                hospitalizations=hospitalizations,
            )

            return city_stats, metrics, hospitalizations_updated
