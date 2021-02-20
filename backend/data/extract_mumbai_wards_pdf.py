import pdfplumber
from datetime import datetime
import pandas as pd
import numpy as np


def _read_pdf(source_file_path):
    return pdfplumber.open(source_file_path)


def _extract_wards_data_from_page(positive_cases_pdf_page) -> pd.DataFrame:
    total_discharged_boundary = 618
    discharged_deaths_boundary = 650
    deaths_active_boundary = 700

    # switching to our naming convention
    boxes = {
        'ward': (240, 79.2, 350, 504),  # ward abbreviation
        'total.confirmed': (350, 79.2, total_discharged_boundary, 504),  # cases
        'total.recovered': (total_discharged_boundary, 79.2, discharged_deaths_boundary, 504),  # Discharged column
        'total.deceased': (discharged_deaths_boundary, 79.2, deaths_active_boundary, 504),  # deaths column
        'total.active': (deaths_active_boundary, 79.2, 800, 504),  # active column
    }

    data = dict()
    for key, box in boxes.items():
        raw_data = positive_cases_pdf_page.within_bbox(box).extract_text()
        data[key] = raw_data.split('\n')

    # In a similar way we could actually search for the correct page that
    # contains 'Ward-wise breakdown of positive cases' instead of hard coded page numbers
    date_box = (50, 70, 200, 120)
    raw_date = positive_cases_pdf_page.within_bbox(date_box).extract_text().strip()
    date = datetime.strptime(raw_date, '%b %d, %Y')

    df = pd.DataFrame(data)

    numeric_columns = ['total.confirmed', 'total.recovered', 'total.deceased', 'total.active']
    for column in numeric_columns:
        data[column] = pd.to_numeric(df[column])

    # not available in sheet, but making it consistent with states and districts
    df['total.other'] = 0
    df['total.tested'] = np.nan

    df['date'] = date
    df['district'] = 'Mumbai'
    df['state'] = 'MH'

    return df


def scrape_mumbai_pdf(source_file_path):
    """
    :param source_file_path: 
    :remarks:
    """
    pdf = _read_pdf(source_file_path)
    positive_cases_page = pdf.pages[20]  # TODO instead of hardcoded page, search for the correct page see notes above
    df = _extract_wards_data_from_page(positive_cases_page)

    return df
