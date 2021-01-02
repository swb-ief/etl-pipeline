import pdfplumber
from datetime import datetime
import pandas as pd


def _postive_breakdown_pdf_columns():
    return [
        "as_of",
        "Ward_Name",
        "Total_Positive_Cases",
        "Total_Discharged",
        "Total_Deaths",
        "Total_Active",
        "imputed",
    ]


def _read_pdf(source_file_path):
    return pdfplumber.open(source_file_path)


def _extract_breakdown_positive_cases_date(positive_cases_page):
    return positive_cases_page.extract_text().split("\n")[1]


def _extract_wards_data_from_page(positive_cases_page):
    top_left_x = 240.0  # 295.2  # points
    top_left_y = 79.2  # points
    size_width = 550  # 475.2  # points
    size_height = 424.8  # points

    bottom_right_x = 800
    bottom_right_y = 79.2 + 424.8

    bbox = (top_left_x, top_left_y, bottom_right_x, bottom_right_y)
    page_bbox = positive_cases_page.within_bbox(bbox)
    page_lines = page_bbox.extract_text().split("\n")
    wards_lines = map(
        lambda ward_line: ward_line.split(), page_lines
    )
    result = [
        _extract_ward_data_from_pdf_line(ward_pdf_line) for ward_pdf_line in wards_lines
    ]
    return result


def _extract_ward_data_from_pdf_line(ward_pdf_line):
    return {
        "Ward_Name": ward_pdf_line[0],
        "Total_Positive_Cases": ward_pdf_line[1],
        "Total_Discharged": ward_pdf_line[2],
        "Total_Deaths": ward_pdf_line[3],
        "Total_Active": ward_pdf_line[4],
    }


def _pdf_data_to_pandas_df(ward_pdf_info):
    ward_positive_df = pd.DataFrame(
        ward_pdf_info,
        columns=_postive_breakdown_pdf_columns(),
    )
    ward_positive_df["Total_Positive_Cases"] = pd.to_numeric(
        ward_positive_df["Total_Positive_Cases"]
    )
    ward_positive_df["Total_Deaths"] = pd.to_numeric(ward_positive_df["Total_Deaths"])
    ward_positive_df["Total_Discharged"] = pd.to_numeric(
        ward_positive_df["Total_Discharged"]
    )
    ward_positive_df["Total_Active"] = pd.to_numeric(ward_positive_df["Total_Active"])
    return ward_positive_df


def scrap_positive_wards_to_df(source_file_path, page=20):
    """
    :param page: page is 0 indexed
    :remarks:
    """
    pdf = _read_pdf(source_file_path)
    positive_cases_page = pdf.pages[page]
    breakdown_date_string = _extract_breakdown_positive_cases_date(positive_cases_page)
    ward_pdf_data = _extract_wards_data_from_page(positive_cases_page)
    ward_positive_df = _pdf_data_to_pandas_df(ward_pdf_data)
    breakdown_date = datetime.strptime(breakdown_date_string, "As of %b %d, %Y")
    ward_positive_df["as_of"] = breakdown_date.strftime("%Y-%m-%d")
    ward_positive_df["imputed"] = 0
    return ward_positive_df.copy()
