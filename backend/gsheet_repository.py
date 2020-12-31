import gspread
import pandas as pd
import numpy as np
import os
import logging

from backend.repository import Repository

log = logging.getLogger(__name__)


class GSheetRepository(Repository):
    default_worksheet = "https://docs.google.com/spreadsheets/d/1HeTZKEXtSYFDNKmVEcRmF573k2ZraDb6DzgCOSXI0f0/edit#gid=0"

    @classmethod
    def get_worksheet_url_from_env(cls):
        return os.getenv("SWB_WORKSHEET_URL", GSheetRepository.default_worksheet)

    def __init__(self, base_url: str):
        self.base_url = base_url
        self._gspread_client = None

    def _get_gspread_client(self):

        if self._gspread_client is None:
            try:
                self._gspread_client = gspread.service_account(
                    filename=os.getenv(
                        "GOOGLE_APPLICATION_CREDENTIALS", "~/.config/gspread/service_account.json"
                    )
                )
            except FileNotFoundError as e:
                log.error(f"Unable to create gspread client #{e}")

        return self._gspread_client

    def _get_worksheet(self, worksheet_name: str) -> gspread.Worksheet:
        sheet = self._get_gspread_client().open_by_url(self.base_url)
        worksheet = sheet.worksheet(worksheet_name)
        return worksheet

    @staticmethod
    def _df_to_cleaned_data(df):
        """ note it will convert int's to floats if it contains a field with an empty string.
        Because of the np.nan it will inject
        :remarks: '' to np.nan is a safe assumption the only string columns state/district/ward can't be empty
        """
        cleaned = [df.columns.values.tolist()] + df.replace("", np.nan).values.tolist()
        return cleaned

    def store_dataframe(self, df: pd.DataFrame, storage_name: str) -> None:
        worksheet = self._get_worksheet(storage_name)

        # create a list of lists with the first list the column names, followed by rows of data
        clean_data = self._df_to_cleaned_data(df)

        worksheet.update(values=clean_data)

    def get_dataframe(self, storage_name: str) -> pd.DataFrame:
        worksheet = self._get_worksheet(storage_name)
        return pd.DataFrame(worksheet.get_all_records())
