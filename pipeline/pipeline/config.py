import os
import logging

import gspread

from luigi.contrib.gcs import GCSClient


GCS_TOKEN = None

try:
    GCS_TOKEN = gspread.service_account(
        filename=os.getenv(
            "GOOGLE_APPLICATION_CREDENTIALS", "~/.config/gspread/service_account.json"
        )
    )
except FileNotFoundError as e:
    logging.error(f"Unable to find gcloud client #{e}")


gcs_client = GCSClient(GCS_TOKEN)

DEFAULT_WORKSHEET_URL = "https://docs.google.com/spreadsheets/d/1HeTZKEXtSYFDNKmVEcRmF573k2ZraDb6DzgCOSXI0f0/edit#gid=0"


WORKSHEET_URL = os.getenv("SWB_WORKSHEET_URL", DEFAULT_WORKSHEET_URL)

