import requests
import json
import pandas as pd
from collections import defaultdict
from pandas import isnull
from pandas.io.json import json_normalize
import itertools
import yaml
import logging
import os

# file path info
logging.basicConfig(format="%(levelname)s - %(message)s", level=logging.WARN)
full_path = os.path.realpath(__file__)
path, filename = os.path.split(full_path)

try:

    logging.info("Reading YAML")
    with open(os.path.join(path, "parameters.yaml"), "r") as f:
        yaml_file = yaml.load(f, Loader=yaml.FullLoader)

    # assign defult values to fields.
    json_url = yaml_file.get(
        "history_json_url",
        "https://raw.githubusercontent.com/covid19india/api/gh-pages/v4/data-all.json",
    )
    states_and_districts = yaml_file.get("states_and_districts", [{"MH": ["Mumbai"]}])
    output_file = os.path.join(
        path, yaml_file.get("output_file", "output/city_stats.csv")
    )

    logging.info("Fetching JSON from URL")

    # 1. Convert the JSON to a DF
    df = pd.read_json(json_url)
    df = df.T

    logging.info("Parsed JSON")

    all_df = None

    logging.info("Reading states and districts")
    # 2. Now, filter the entries that are in the YAML
    for entry_ in states_and_districts:
        state = list(entry_.keys())[0]
        districts = list(entry_.values())[0]
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
    logging.info(all_df.columns)
except Exception as e:
    logging.exception("Error Occurred")
