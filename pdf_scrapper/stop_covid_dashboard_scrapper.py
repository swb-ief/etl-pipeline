"""
This file just organizes what's inside the PDF_scrape_text.py module.

We assume that we are scrapping the following PDF:

- http://stopcoronavirus.mcgm.gov.in/assets/docs/Dashboard.pdf

Which is updated every now and then with updated information.

We intend to scrap the numbers for positives cases in the 
"Ward-wise breakdown of positive cases" where we assume that 
the elements to scrap are in the page 23 inside the bunding box at:
(x0: 295.2, top: 79.2, x1: 770.4, bottom: 504.0).

An ilustration on the are we scrapping can be seen in the image: ./section-to-scrap.png
"""

import re

import pandas as pd
import pdfplumber as pdfplumber


def read_pdf(source_file_path):
    return pdfplumber.open(source_file_path)


def extract_breakdown_positive_cases_date(positive_cases_page):
    return positive_cases_page.extract_text().split("\n")[1]


def extract_wards_data_from_page(positive_cases_page):
    top_left_x = 295.2  # points
    top_left_y = 79.2  # points
    size_width = 475.2  # points
    size_height = 424.8  # points
    bbox = (top_left_x, top_left_y, top_left_x + size_width, top_left_y + size_height)
    page_bbox = positive_cases_page.within_bbox(bbox)
    wards_lines = map(
        lambda ward_line: ward_line.split(), page_bbox.extract_text().split("\n")
    )
    result = [
        extract_ward_data_from_pdf_line(ward_pdf_line) for ward_pdf_line in wards_lines
    ]
    return result


def extract_ward_data_from_pdf_line(ward_pdf_line):
    return {
        "Ward_Name": ward_pdf_line[0],
        "Total_Positive_Cases": ward_pdf_line[1],
        "Total_Discharged": ward_pdf_line[2],
        "Total_Deaths": ward_pdf_line[3],
        "Total_Active": ward_pdf_line[4],
    }


def pdf_data_to_pandas_df(ward_pdf_info):
    ward_positive_df = pd.DataFrame(
        ward_pdf_info,
        columns=[
            "Ward_Name",
            "Total_Positive_Cases",
            "Total_Discharged",
            "Total_Deaths",
            "Total_Active",
        ],
    )
    return ward_positive_df


def export_df_to_file(df, output_ward_path):
    return df.to_csv(output_ward_path, index=False)


def scrap_pdf_to_csv(source_file_path, output_path, page=22):
    pdf = read_pdf(source_file_path)
    positive_cases_page = pdf.pages[page]
    date = extract_breakdown_positive_cases_date(positive_cases_page)
    ward_pdf_data = extract_wards_data_from_page(positive_cases_page)
    ward_positive_df = pdf_data_to_pandas_df(ward_pdf_data)
    date_from_pdf = re.sub("[^0-9A-z]|\s", "-", date.lower().strip())
    datetime_suffix = pd.Timestamp.utcnow().isoformat()
    csv_path = f"{output_path}/ward-positive_{date_from_pdf}_{datetime_suffix}.csv"
    export_df_to_file(ward_positive_df, csv_path)
    return date, csv_path
