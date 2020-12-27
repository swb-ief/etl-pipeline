import gspread as gspread
import pandas as pd

import os
import logging

log = logging.getLogger(__name__)

_GSPREAD_CLIENT = None
_DEFAULT_WORKSHEET_URL = "https://docs.google.com/spreadsheets/d/1HeTZKEXtSYFDNKmVEcRmF573k2ZraDb6DzgCOSXI0f0/edit#gid=0"
WORKSHEET_URL = os.getenv("SWB_WORKSHEET_URL", _DEFAULT_WORKSHEET_URL)


def _get_client():
    """ Singleton instance of the google spreadsheet client"""
    global _GSPREAD_CLIENT

    if not _GSPREAD_CLIENT:
        try:
            _GSPREAD_CLIENT = gspread.service_account(
                filename=os.getenv(
                    "GOOGLE_APPLICATION_CREDENTIALS", "~/.config/gspread/service_account.json"
                )
            )
        except FileNotFoundError as e:
            log.error(f"Unable to create gspread client #{e}")

    return _GSPREAD_CLIENT


def get_worksheet(url: str, worksheet_name: str) -> gspread.Worksheet:
    sheet = _get_client().open_by_url(url)
    worksheet = sheet.worksheet(worksheet_name)
    return worksheet


def get_dataframe(worksheet: gspread.Worksheet) -> pd.DataFrame:
    return pd.DataFrame(worksheet.get_all_records())
