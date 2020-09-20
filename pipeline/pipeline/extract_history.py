import itertools
import logging

import pandas as pd
from pandas import isnull


def extract_history_command(history_json_url, states_and_districts, output_file):
    """Extracts history records from the covid19india v4 data all API which should be defined by the --history-json-url argument."""
    logging.info(f"Fetching json")
    df = pd.read_json(history_json_url)
    df = df.T
    logging.info("Parsed JSON")
    all_df = None
    logging.info("Reading states and districts")
    # 2. Now, filter the entries that are in the YAML
    for state, districts in states_and_districts.items():
        json_keys = ["delta", "total"]
        logging.info("Extracting data for State: {}".format(state))

        # 2.1 Filter parent dataframe by state
        districts_series = df[state].apply(pd.Series)["districts"]
        districts_series = districts_series.apply(lambda x_: {} if isnull(x_) else x_)

        # 2.2 Create a DF with district-level columns
        state_df = pd.json_normalize(districts_series)

        for district in districts:

            # 2.3 Create a regular expression to filter the district in the YAML
            y = [".".join(list(p)) for p in itertools.product([district], json_keys)]
            reg = "|".join("^" + i for i in y)

            # 2.4 Filter district using RE
            dist_df = state_df.filter(regex=reg)
            dist_df.insert(1, "district", district)
            dist_df.insert(2, "state", state)

            # 2.5 set index for easy concat
            dist_df.index = [df.index, dist_df.pop("district"), dist_df.pop("state")]
            dist_df.index.set_names(["date", "district", "state"], inplace=True)

            # 2.6 add genenric col names
            new_col = [
                col.replace("{}.".format(district), "") for col in list(dist_df.columns)
            ]
            dist_df.rename(
                dict(zip(list(dist_df.columns), new_col)), axis=1, inplace=True
            )

            # 2.7 Append to a global DF
            if all_df is None:
                all_df = dist_df.copy()
            else:
                all_df = pd.concat([all_df, dist_df])

    logging.info("Writing data to output file")
    all_df.to_csv(output_file)
    logging.info("Output file created: {}".format(output_file))
