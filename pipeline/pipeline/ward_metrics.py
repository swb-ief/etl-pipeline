import gspread
import numpy
import pandas as pd
import os
from ward_data_computation import (
    ward_data_pre_processing,
    impute_ward_population_data,
    impute_missing_metrics,
)

GCS_TOKEN = None
DEFAULT_WORKSHEET_URL = "https://docs.google.com/spreadsheets/d/1HeTZKEXtSYFDNKmVEcRmF573k2ZraDb6DzgCOSXI0f0/edit#gid=0"

WORKSHEET_URL = os.getenv("SWB_WORKSHEET_URL", DEFAULT_WORKSHEET_URL)

try:
    GCS_TOKEN = gspread.service_account(
        filename=os.getenv(
            "GOOGLE_APPLICATION_CREDENTIALS", "~/.config/gspread/service_account.json"
        )
    )
except FileNotFoundError as e:
    logging.error(f"Unable to create gspread client #{e}")


def worksheet_as_df_by_url(
    sheet_url: str, worksheet_name: str
):  # -> Tuple[gspread.Worksheet, pandas.DataFrame]:
    sheet = GCS_TOKEN.open_by_url(sheet_url)
    worksheet = sheet.worksheet(worksheet_name)
    return worksheet, pd.DataFrame(worksheet.get_all_records())


_, df_ward_metrics = worksheet_as_df_by_url(WORKSHEET_URL, "positive-breakdown")
_, df_ward_population = worksheet_as_df_by_url(WORKSHEET_URL, "ward_population")
ward_metrics_worksheet, _ = worksheet_as_df_by_url(WORKSHEET_URL, "ward_metrics")

df_ward_metrics = ward_data_pre_processing(df_ward_metrics)
processed_ward_data_df = impute_ward_population_data(
    df_ward_metrics, df_ward_population
)
ward_data_with_metrics_and_imputation_df = impute_missing_metrics(
    processed_ward_data_df
)

# populate the googlesheets
ward_metrics_worksheet.update(
    [ward_data_with_metrics_and_imputation_df.columns.values.tolist()]
    + ward_data_with_metrics_and_imputation_df.values.tolist()
)
