import os

import gspread

from luigi.contrib.dropbox import DropboxClient

DROPBOX_TOKEN = os.getenv("SWB_DROPBOX_TOKEN")
dropbox_client = DropboxClient(DROPBOX_TOKEN)

GSPREAD_CLIENT = gspread.service_account()