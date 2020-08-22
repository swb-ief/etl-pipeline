#!/usr/bin/env python3

import os

import requests

API_KEY = os.environ.get("SPREADSHEET_API_KEY")


def get_spreadsheet(spreadsheet_id):
    return requests.get(
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}",
        params={"key": API_KEY},
    )


def get_sheets(spreadsheet):
    spreadsheet_id = spreadsheet["spreadsheetId"]
    sheets_names = [sheet["properties"]["title"] for sheet in spreadsheet["sheets"]]
    return requests.get(
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values:batchGet",
        params={
            "key": API_KEY,
            "ranges": sheets_names,
            "valueRenderOption": "UNFORMATTED_VALUE",
        },
    )


def fetch_spreadsheet(spreadsheet_id):
    spreadsheet_response = get_spreadsheet(spreadsheet_id)
    spreadsheet_response.raise_for_status()
    sheets_response = get_sheets(spreadsheet_response.json())
    sheets_response.raise_for_status()
    return sheets_response.json()
