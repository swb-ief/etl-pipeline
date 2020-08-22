#!/usr/bin/env python3

import os

import click

from spreadsheets_fetcher import fetcher, sheets_to_csv


@click.command()
@click.argument("spreadsheets_ids", nargs=-1, required=True)
@click.argument("output_dir", nargs=1, required=True)
def fetch_spreadsheets_and_echo(spreadsheets_ids, output_dir):
    """
    Download each id of SPREADSHEET_IDS and writes them as csv to an OUTPUT_DIR

    SPREADSHEET_IDS a list of spreadsheet ids shared using "anyone on the internet with this link can view"
    OUTPUT_DIR the path to which we will be writing the csv files.
    """
    if not os.path.isdir(output_dir):
        raise IOError(f"Output: {output_dir} is not a directory")

    values = [
        fetcher.fetch_spreadsheet(spreadsheet_id) for spreadsheet_id in spreadsheets_ids
    ]
    results = [sheets_to_csv.spreadsheet_values_to_csv(output_dir, value) for value in values]
    click.echo(results)


if __name__ == "__main__":
    fetch_spreadsheets_and_echo()
