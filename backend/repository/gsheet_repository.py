import gspread
import pandas as pd
import numpy as np
import os
import logging

from backend.repository import Repository

log = logging.getLogger(__name__)


class GSheetRepository(Repository):
    def __init__(self, base_url: str):
        self.base_url = base_url
        self._gspread_client = None
        self._spreadsheet = None

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

    def _get_spreadsheet(self):
        if self._spreadsheet is None:
            self._spreadsheet = self._get_gspread_client().open_by_url(self.base_url)
        return self._spreadsheet

    def _get_worksheet(self, worksheet_name: str) -> gspread.Worksheet:
        sheet = self._get_spreadsheet()
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

    def store_dataframe(self, df: pd.DataFrame, storage_location: str, allow_create: bool) -> None:
        if not self.exists(storage_location):
            if allow_create:
                log.info(f'Created storage location {storage_location}')
                self.create_storage_location(storage_location)
            else:
                raise ValueError('Storage location does not exists, create it or call this with allow_create=True')

        worksheet = self._get_worksheet(storage_location)

        # create a list of lists with the first list the column names, followed by rows of data
        clean_data = self._df_to_cleaned_data(df)

        worksheet.update(values=clean_data)

    def get_dataframe(self, storage_location: str) -> pd.DataFrame:
        worksheet = self._get_worksheet(storage_location)
        return pd.DataFrame(worksheet.get_all_records())

    def exists(self, storage_location: str) -> bool:
        spreadsheet = self._get_spreadsheet()
        sheet_names = [sheet.title for sheet in spreadsheet]
        return storage_location in sheet_names

    def create_storage_location(self, storage_location: str) -> None:
        if self.exists(storage_location):
            log.warning(f'Storage location {storage_location} already exists')
            return

        _ = self._get_spreadsheet().add_worksheet(title=storage_location, rows=100, cols=20)

    def create_repository(self, repository_name: str, admin_email: str) -> str:
        log.info(f'Creating a new Google Sheet "{repository_name}" as a repository')
        client = self._get_gspread_client()

        spreadsheet = client.create(repository_name)
        log.info(f'Created sheet with id: {spreadsheet.id}')

        self.base_url = spreadsheet.url
        log.info(f'URL: {spreadsheet.url}')

        spreadsheet.share(admin_email, perm_type='user', role='owner')
        log.info(f'Assigned {admin_email} as owner')

        return spreadsheet.id
