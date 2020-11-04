import gspread
import numpy
import pandas as pd
import os

GSPREAD_CLIENT = None
DEFAULT_WORKSHEET_URL = "https://docs.google.com/spreadsheets/d/1HeTZKEXtSYFDNKmVEcRmF573k2ZraDb6DzgCOSXI0f0/edit#gid=0"


WORKSHEET_URL = os.getenv("SWB_WORKSHEET_URL", DEFAULT_WORKSHEET_URL)

try:
    GSPREAD_CLIENT = gspread.service_account(
        filename=os.getenv(
            "GOOGLE_APPLICATION_CREDENTIALS", "~/.config/gspread/service_account.json"
        )
    )
except FileNotFoundError as e:
    logging.error(f"Unable to create gspread client #{e}")

def worksheet_as_df_by_url(sheet_url: str, worksheet_name: str): # -> Tuple[gspread.Worksheet, pandas.DataFrame]:
    sheet = GSPREAD_CLIENT.open_by_url(sheet_url)
    worksheet = sheet.worksheet(worksheet_name)
    return worksheet, pd.DataFrame(worksheet.get_all_records())
    
worksheet, Rt_df = worksheet_as_df_by_url(WORKSHEET_URL, "Rt")
 
new_Rt_df = pd.read_csv("/usr/data/epinow2_out.csv")
 
worksheet.update([new_Rt_df.columns.values.tolist()] + new_Rt_df.values.tolist())
 
 
