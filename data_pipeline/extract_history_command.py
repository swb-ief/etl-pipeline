import itertools
import json
from collections import defaultdict

import requests
import click
import pandas as pd
from pandas import isnull
from pandas.io.json import json_normalize


@click.command()
@click.option(
    "--history-json-url",
    default="https://raw.githubusercontent.com/covid19india/api/gh-pages/v4/data-all.json",
    help="The URL for the covid19india v4 json",
)
@click.option(
    "--states-and-districts",
    type=(str, str),
    default=(("MH", "Mumbai")),
    multiple=True,
    help="A list of state and districts to extract data from.",
    show_default=True,
)
@click.option(
    "--output-file",
    default="city_stats.csv",
    help="The path of the new csv containing the cities stats from the v4 history json.",
)
def extract_history_command(history_json_url, states_and_districts, output_file):
    """Extracts history records from the covid19india v4 data all API which should be defined by the --history-json-url argument.
    """
    click.echo(f"Fetching json from {history_json_url}")
    df = pd.read_json(history_json_url)
    df = df.T
    click.echo("Parsed JSON")
    all_df = None
    click.echo("Reading states and districts")
    # 2. Now, filter the entries that are in the YAML
    for entry_ in states_and_districts:
        state = entry_[0]
        districts = entry_[1].split(",")
        json_keys = ["delta", "total"]
        click.echo("Extracting data for State: {}".format(state))

        # 2.1 Filter parent dataframe by state
        districts_series = df[state].apply(pd.Series)["districts"]
        districts_series = districts_series.apply(lambda x_: {} if isnull(x_) else x_)

        # 2.2 Create a DF with district-level columns
        state_df = pd.json_normalize(districts_series)

        # 2.3 Create a regular expression to filter the districts in the YAML
        y = [".".join(list(p)) for p in itertools.product(districts, json_keys)]
        reg = "|".join("^" + i for i in y)

        # 2.4 Filter districts using RE
        state_df = state_df.filter(regex=reg)
        state_df.index = df.index

        click.echo(f"Appending State data for {state} and districts {districts}")

        # 2.4 Append to a global DF
        if all_df is None:
            all_df = state_df.copy()
        else:
            all_df = all_df.join(state_df)

    click.echo("Writing data to output file")
    all_df.to_csv(output_file)
    click.echo("Output file created: {}".format(output_file))
    click.echo(all_df.columns)
    return all_df


if __name__ == "__main__":
    extract_history_command()
