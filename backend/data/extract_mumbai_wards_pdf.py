import pdfplumber
from datetime import datetime
import pandas as pd
import numpy as np

def _read_pdf(source_file_path):
    return pdfplumber.open(source_file_path)


def identify_wardnames_top_left(positive_cases_pdf_page, initial_bbox=(240, 80, 330, 150)):
    """
    Function to identify the top left corner of the wardnames box, to allow dynamic box definitions
    positive_cases_pdf_page - page containing data of active/recovered/discharged/deaths etc
    initial_bbox - initial box with which the search for top-left corner will start
    
    bbox definition - (x0, y0, x1, y1):
        x0 - distance of left side of bbox from left side of page
        y0 - distance of top side of bbox from top of page
        x1 - distance of right side of bbox from left side of page
        y1 - distance of bottom of bbox from top of page
        
    obtain visual debugging tips from https://github.com/jsvine/pdfplumber
    
    Contributor: aswinjayan94
    """
    
    x0 = initial_bbox[0]
    top = initial_bbox[1]
    
    checkbox = initial_bbox
    # extract all wards in initial_bbox
    checkdata = positive_cases_pdf_page.within_bbox(checkbox).extract_text()
    checklist = set(checkdata.split('\n')[:24])
    # check which wards extracted above belong to the list below (it's very likely that you'll find at least one of the wards specified below)
    checkagainstlist = set(['RC', 'KW', 'PN', 'RS', 'KE'])
    # Number of wards common to checklist and checkagainstlist
    initiallen = len(checklist.intersection(checkagainstlist))
    
    # keep adjusting the x coordinate of the initial_bbox until the list of extracted wards is different to initially extracted list
    while initiallen>0 and len(checklist.intersection(checkagainstlist))==initiallen:
        #print("All in initiallist Still present")
        previous_x0 = x0
        x0+=5
        checkbox = (x0, top, initial_bbox[2], initial_bbox[3])
        checkdata = positive_cases_pdf_page.within_bbox(checkbox).extract_text()
        checklist = set(checkdata.split('\n')[:24])
    
    # since number of wards has changed (as the while loop has broken), the new box has crossed data. reset it to previous coordinate
    x0 = previous_x0-5
    checkbox = (x0, top, initial_bbox[2], initial_bbox[3])
    checkdata = positive_cases_pdf_page.within_bbox(checkbox).extract_text()
    checklist = set(checkdata.split('\n')[:24])

    # keep adjusting the y coordinate of the initial_bbox until the list of extracted wards is different to initially extracted list
    while initiallen>0 and len(checklist.intersection(checkagainstlist))==initiallen:
        #print("All in initiallist Still present")
        previous_top = top
        top+=5
        checkbox = (x0, top, initial_bbox[2], initial_bbox[3])
        checkdata = positive_cases_pdf_page.within_bbox(checkbox).extract_text()
        checklist = set(checkdata.split('\n')[:24])
        
    # since number of wards has changed, the new box has crossed data. reset it to previous coordinate
    top = previous_top-5
    
    return (x0, top)
    

def _extract_wards_data_from_page(positive_cases_pdf_page) -> pd.DataFrame:
    total_discharged_boundary = 618
    discharged_deaths_boundary = 660
    deaths_active_boundary = 710
    
    # identify top left corner of district names
    x0, top = identify_wardnames_top_left(positive_cases_pdf_page)
    
    wardbox = (x0, top, x0+30, top+420)
    confirmedbox = (x0+30, top, total_discharged_boundary, top+420)
    recoveredbox = (total_discharged_boundary, top, discharged_deaths_boundary, top+420)
    deceasedbox = (discharged_deaths_boundary, top, deaths_active_boundary, top+420)
    activebox = (deaths_active_boundary, top, 800, top+420)

    # switching to our naming convention
    boxes = {
        'ward': wardbox,  # ward abbreviation
        'total.confirmed': confirmedbox,  # cases
        'total.recovered': recoveredbox,  # Discharged column
        'total.deceased': deceasedbox,  # deaths column
        'total.active': activebox,  # active column
    }

    data = dict()
    for key, box in boxes.items():
        raw_data = positive_cases_pdf_page.within_bbox(box).extract_text()
        # due to shifting sizes the totals rows sometimes gets included
        # because we know there are only 24 wards we can cut it of by limiting our selves to 24
        data[key] = raw_data.split('\n')[:24]

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


def find_ward_wise_breakdown_page(pdf):
    title_box = (0, 0, 800, 70)
    ward_page_title = 'Ward-wise breakdown of positive cases'

    for page in pdf.pages:
        title = page.within_bbox(title_box).extract_text()
        if title is not None and ward_page_title.lower() in title.lower():
            return page

    raise ValueError(f'PDF does not contain a page with {ward_page_title}')


def find_ward_wise_new_cases_page(pdf):
    title_box = (0, 0, 800, 70)
    ward_page_title = 'Ward-wise new cases'

    for page in pdf.pages:
        title = page.within_bbox(title_box).extract_text()
        if title is not None and ward_page_title.lower() in title.lower():
            return page

    raise ValueError(f'PDF does not contain a page with {ward_page_title}')
    
    
def find_sealed_building_breakdown_page(pdf):
    title_box = (0, 0, 800, 70)
    ward_page_title = 'Ward-wise Sealed Buildings (SBs)/Micro-containment zones'
    for page in pdf.pages:
        title = page.within_bbox(title_box).extract_text()
        if title is not None and ward_page_title.lower() in title.lower():
            return page
    raise ValueError(f'PDF does not contain a page with {ward_page_title}')
    
    
def find_sealed_floor_breakdown_page(pdf):
    title_box = (0, 0, 800, 70)
    ward_page_title = 'Ward-wise Sealed Floors (SFs)'
    for page in pdf.pages:
        title = page.within_bbox(title_box).extract_text()
        if title is not None and ward_page_title.lower() in title.lower():
            return page
    raise ValueError(f'PDF does not contain a page with {ward_page_title}')
    
    
def _extract__data_from_page_sealed(positive_cases_pdf_page, top_factor, column_name) -> pd.DataFrame:
    outer_boundary = 705
    
    # identify top left corner of district names
    x0, top = identify_wardnames_top_left(positive_cases_pdf_page, initial_bbox=(330, 80, 420, 200))
    
    wardbox = (x0, top-top_factor, x0+40, top+425)
    confirmedbox = (x0+40, top-top_factor, outer_boundary, top+425)
    # switching to our naming convention
    boxes = {
        'ward': wardbox,  # ward abbreviation
        column_name: confirmedbox
    }# cases
    data = dict()
    for key, box in boxes.items():
        raw_data = positive_cases_pdf_page.within_bbox(box).extract_text()
        # due to shifting sizes the totals rows sometimes gets included
        # because we know there are only 24 wards we can cut it of by limiting our selves to 24
        data[key] = raw_data.split('\n')[:24]
        # In a similar way we could actually search for the correct page that
    # contains 'Ward-wise breakdown of positive cases' instead of hard coded page numbers
    
    df = pd.DataFrame(data)
    numeric_columns = [column_name]
    for column in numeric_columns:
        data[column] = pd.to_numeric(df[column])
    return df


def scrape_mumbai_sealed_details_pdf(source_file_path):
    """
    :param source_file_path: 
    :remarks:
    """
    pdf = _read_pdf(source_file_path)
    
    return full_df


def scrape_mumbai_pdf(source_file_path):
    """
    :param source_file_path: 
    :remarks:
    """
    pdf = _read_pdf(source_file_path)

    positive_cases_pdf_page = find_ward_wise_breakdown_page(pdf)
    # new_cases_page = find_ward_wise_new_cases_page(pdf)
    df = _extract_wards_data_from_page(positive_cases_pdf_page)
    
    # sealed buildings/floors
    sealed_building_pdf_page = find_sealed_building_breakdown_page(pdf)
    building_df=_extract__data_from_page_sealed(sealed_building_pdf_page, 0, 'total.sealedbuildings')
    sealed_floor_pdf_page = find_sealed_floor_breakdown_page(pdf)
    floor_df=_extract__data_from_page_sealed(sealed_floor_pdf_page, 0, 'total.sealedfloors')
    
    # combine all data
    full_df=df.merge(building_df.merge(floor_df, how='outer',on='ward'), how='outer', on='ward')

    return full_df
