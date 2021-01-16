import gspread
import numpy as np
import pandas as pd
import os
import logging

GSPREAD_CLIENT = None
#DEFAULT_WORKSHEET_URL = "https://docs.google.com/spreadsheets/d/1HeTZKEXtSYFDNKmVEcRmF573k2ZraDb6DzgCOSXI0f0/edit#gid=0"
DEFAULT_WORKSHEET_URL = "https://docs.google.com/spreadsheets/d/1fkPkvIowcd2hob3qvq20iPhVGowquqLygLDTJt3aPQU/edit#gid=1452119190"


WORKSHEET_URL = os.getenv("SWB_WORKSHEET_URL", DEFAULT_WORKSHEET_URL)

try:
    GSPREAD_CLIENT = gspread.service_account(
        filename=os.getenv(
            "GOOGLE_APPLICATION_CREDENTIALS", "~/.config/gspread/service_account.json"
        )
    )
except FileNotFoundError as e:
    logging.error(f"Unable to create gspread client #{e}")


def worksheet_as_df_by_url(sheet_url: str, worksheet_name: str):
    sheet = GSPREAD_CLIENT.open_by_url(sheet_url)
    worksheet = sheet.worksheet(worksheet_name)
    return worksheet, pd.DataFrame(worksheet.get_all_records())


rt_worksheet, Rt_df = worksheet_as_df_by_url(WORKSHEET_URL, "Rt")
dt_worksheet, dt_df = worksheet_as_df_by_url(WORKSHEET_URL, "doubling_time")

# Read the existing Rt out file by the R Script
new_Rt_df = pd.read_csv("/usr/data/epinow2_out.csv")
new_Rt_df["city"] = "Mumbai"

# TODO --> TEMPORARY UNTIL SCHEMA IN OUTPUT GOOGLE SHEET IS CHANGED
new_Rt_df["lower"] = np.nan
new_Rt_df["upper"] = np.nan

new_Rt_df = new_Rt_df.drop(['variable', 'strat'], 1)
print(new_Rt_df.columns)

cols = ['Unnamed: 0', 'date', 'type', 'median', 'lower', 'upper', 'mean', 'lower_95', 'upper_95', 'city']
new_Rt_df = new_Rt_df[cols]
new_Rt_df.columns = ['Unnamed: 0', 'date', 'type', 'median', 'lower', 'upper', 'mean.mean', 'CI_lower.mean', 'CI_upper.mean', 'city']
print(new_Rt_df)


# Read the existing doubling time numbers in days
# new_dt_df = pd.read_csv("/usr/data/dt.csv")
new_dt_df = pd.read_csv("/usr/data/doubling_time.csv")
new_dt_df["city"] = "Mumbai"
print(new_dt_df.columns)
# new_dt_df = new_dt_df.drop("r", 1)
cols = ['Unnamed: 0', 'date', 'dt', 'city']
new_dt_df = new_dt_df[cols]
new_dt_df.columns = ['Unnamed: 0', 'date', 'doubling.time', 'city']
print(new_dt_df)

# populate the googlesheets
rt_worksheet.update([new_Rt_df.columns.values.tolist()] + new_Rt_df.values.tolist())
dt_worksheet.update([new_dt_df.columns.values.tolist()] + new_dt_df.values.tolist())
