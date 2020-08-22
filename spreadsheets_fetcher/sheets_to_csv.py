#!/usr/bin/env python3

import csv
import logging


def spreadsheet_values_to_csv(sheets):
    spreadsheet_id = sheets['spreadsheetId']
    logging.info(f"Parsing values from spreadsheet {spreadsheet_id}")
    csv_files = []
    for sheet_value_range in sheets['valueRanges']:
        value_range = sheet_value_range['range']
        file_name = f"{spreadsheet_id}_{value_range}.csv"
        logging.info(f"Writing csv file {file_name} for values for range {value_range}")
        with open(file_name, 'w') as csv_range_file:
            writer = csv.writer(csv_range_file)
            writer.writerows(sheet_value_range['values'])
        csv_files.append(file_name)

    return csv_files
