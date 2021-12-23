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
        
    visual debugging tips at https://github.com/jsvine/pdfplumber
    
    Contributors: aswinjayan94, bcbowers, nozziel
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
    
    
def find_page_general(pdf, title):
    title_box = (0, 0, 800, 70)
    page_title = title

    for page in pdf.pages:
        title = page.within_bbox(title_box).extract_text()
        if title is not None and page_title.lower() in title.lower():
            return page

    raise ValueError(f'PDF does not contain a page with {page_title}')

def _extract__data_from_page_general(positive_cases_pdf_page, top_factor, column_name) -> pd.DataFrame:
    outer_boundary = 720
    
    # identify top left corner of district names
    x0, top = identify_wardnames_top_left(positive_cases_pdf_page, initial_bbox=(350, 50, 425, 450))
    
    wardbox = (x0, top-top_factor, x0+50, top+425)
    confirmedbox = (x0+30, top-top_factor, outer_boundary, top+425)

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
    date_box = (50, 70, 200, 120)
    raw_date = positive_cases_pdf_page.within_bbox(date_box).extract_text().strip()
    date = datetime.strptime(raw_date, '%b %d, %Y')

    df = pd.DataFrame(data)

    numeric_columns = [column_name]
    for column in numeric_columns:
        data[column] = pd.to_numeric(df[column])

    return df

def _extract_data_from_page(positive_cases_pdf_page, x0, top, graph_name) -> pd.DataFrame:
    #total_discharged_boundary = 0
    #discharged_deaths_boundary = 660
    #deaths_active_boundary = 710
    
    # identify top left corner of district names
    #x0, top = identify_wardnames_top_left(positive_cases_pdf_page)
    

    if graph_name=='COVID19 Case Analysis' or graph_name=='COVID19 Bed Management':  
        metricbox = (x0, top, x0+140, top+325)
        countbox = (x0+140, top, x0+200, top+325)
    
    if graph_name=='Containment Measures':
        metricbox = (x0, top, x0+180, top+200)
        countbox = (x0+190, top, x0+245, top+200)
        
    if graph_name=='Quarantine Stats':
        metricbox = (x0, top, x0+150, top+75)
        countbox = (x0+180, top, x0+245, top+75)

    # switching to our naming convention
    boxes = {
        'metric': metricbox,  # ward abbreviation
        'count': countbox
    }# cases

    data = dict()
    for key, box in boxes.items():
        raw_data = positive_cases_pdf_page.within_bbox(box).extract_text()
        # due to shifting sizes the totals rows sometimes gets included
        # because we know there are only 24 wards we can cut it of by limiting our selves to 24
        data[key] = raw_data.split('\n')[:24]
        
    if graph_name=='COVID19 Bed Management':
        data['metric']=['Bed Capacity (DCHC+DCH+CCC2)',
              'Bed (DCHC+DCH+CCC2) Occupied',
              'Bed (DCHC+DCH+CCC2) Available',
              'DCH & DCHC Bed Capacity',
              'DCH & DCHC Bed Occupied',
              'DCH & DCHC Bed Available',
              'O2 Bed Capacity',
              'O2 Bed Occupied',
              'O2 Bed Available',
              'ICU Bed Capacity',
              'ICU Bed Occupied',
              'ICU Bed Available',
              'Ventilator Bed Capacity',
              'Ventilator Bed Occupied',
              'Ventilator Bed Available']
        
    if graph_name=='Containment Measures':
        data['metric']=['Active Containment Zones – Slums & Chawls',
              'Released Containment Zones – Slums & Chawls',
              'Active Sealed Buildings/micro-containment zones',
              'Released Sealed Buildings/micro-containment zones',
              'Active Sealed Floors']
        
    if graph_name=='Quarantine Stats':
        data['metric']=['Total Quarantine Completed',
              'Currently in Home Quarantine']

        # In a similar way we could actually search for the correct page that
    # contains 'Ward-wise breakdown of positive cases' instead of hard coded page numbers
    date_box = (770, 50, 875, 80)
    raw_date = positive_cases_pdf_page.within_bbox(date_box).extract_text().strip()
    date = datetime.strptime(raw_date, '%b %d, %Y')

    numeric_columns = ['count']
    
    for n in range(len(data['count'])):
        data['count'][n]=data['count'][n].replace(',','').replace(')','').replace('–','').lstrip().strip()
          
    for column in numeric_columns:
        data[column] = pd.to_numeric(data[column], errors='coerce')
    
    df = pd.DataFrame(data)

    df['date'] = date
    df['metric_type'] = graph_name

    return df


def get_text_coordinate(positive_cases_pdf_page, x0, y0, width, height, text):
    
    # check if original bbox has the required text
    if positive_cases_pdf_page.within_bbox((x0, y0, x0+width, y0+height)).extract_text()!=text:
        raise Exception("Specified initial bbox doesn't contain the specified text")
        
    x = x0
    y = y0    
    
    # keep shifting x coordinate of box until text is no longer captured in the bbox
    while positive_cases_pdf_page.within_bbox((x, y, x+width, y+height)).extract_text()==text:
        x+=5
    # final x coordinate
    x-=5
    
    # keep shifting y coordinate of box until text is no longer captured in the bbox
    while positive_cases_pdf_page.within_bbox((x, y, x+width, y+height)).extract_text()==text:
        y+=2
    # final y coordinate
    y-=2
    
    # return coordinates
    return x, y
    

def _extract_data_from_page_facilities(positive_cases_pdf_page, x0, top, graph_name) -> pd.DataFrame:
    # x0 490, top 70
    # get coordinates of top left of 'CCC1 Facilities' heading
    x, y = get_text_coordinate(positive_cases_pdf_page, x0, top, width=170, height=45, text='CCC1 Facilities')
        
    if graph_name=='CCC1 Facilities':  
        metricbox = (x-100, y+70, x-100 +80, y+70 +115)
        countbox2 = (x-100 +80, y+70, x-100 +80+50, y+70 +115)
        countbox3 = (x-100 +80+50, y+70, x-100 +80+50+80, y+70 +115)
        countbox4 = (x-100 +80+50+80, y+70, x-100 +80+50+80+70, y+70 +115)

    if graph_name=='CCC2 Facilities':  
        metricbox = (x-100, y+242, x-100 +80, y+242 +109)
        countbox2 = (x-100+80, y+242, x-100 +80+50, y+242 +109)
        countbox3 = (x-100 +80+50, y+242, x-100 +80+50+80, y+242 +109)
        countbox4 = (x-100 +80+50+80, y+242, x-100 +80+50+80+70, y+242 +109)
        

    # switching to our naming convention
    boxes = {
        'metric': metricbox,  # ward abbreviation
        'num.facilities': countbox2,
        'bed.capacity': countbox3,
        'occupancy': countbox4
    }# cases

    data = dict()
    for key, box in boxes.items():
        raw_data = positive_cases_pdf_page.within_bbox(box).extract_text()
        # due to shifting sizes the totals rows sometimes gets included
        # because we know there are only 24 wards we can cut it of by limiting our selves to 24
        data[key] = raw_data.split('\n')[:24]

    if graph_name=='CCC1 Facilities':
        data['metric']=['Total CCC1 Facilities', 
                        'Active CCC1 Facilities', 
                        'Buffer CCC1 Facilities', 
                        'Reserve CCC1 Facilities']
    if graph_name=='CCC2 Facilities':
        data['metric']=['Total CCC2 Facilities', 
                        'Active CCC2 Facilities', 
                        'Buffer CCC2 Facilities', 
                        'Reserve CCC2 Facilities']

        # In a similar way we could actually search for the correct page that
    # contains 'Ward-wise breakdown of positive cases' instead of hard coded page numbers
    date_box = (770, 50, 875, 80)
    raw_date = positive_cases_pdf_page.within_bbox(date_box).extract_text().strip()
    date = datetime.strptime(raw_date, '%b %d, %Y')

    data

    numeric_columns = ['num.facilities',
                        'bed.capacity',
                        'occupancy']
    for column in numeric_columns:
        for n in range(len(data[column])):
            data[column][n]=data[column][n].replace(',','').replace(')','').replace('–','').lstrip().strip()

    for column in numeric_columns:
        data[column] = pd.to_numeric(data[column], errors='coerce')

    df = pd.DataFrame(data)

    df['date'] = date
    df['metric_type'] = graph_name

    return df

def _extract_data_from_page_tracing(positive_cases_pdf_page, x0, top, graph_name) -> pd.DataFrame:

    metricbox = (x0, top, x0+100, top+80)
    countbox2 = (x0+120, top, x0+180, top+80)
    countbox3 = (x0+180, top, x0+250, top+80)

    # switching to our naming convention
    boxes = {
        'metric': metricbox,  # ward abbreviation
        'past.24hrs': countbox2,
        'cumulative': countbox3,
    }# cases

    data = dict()
    for key, box in boxes.items():
        raw_data = positive_cases_pdf_page.within_bbox(box).extract_text()
        # due to shifting sizes the totals rows sometimes gets included
        # because we know there are only 24 wards we can cut it of by limiting our selves to 24
        data[key] = raw_data.split('\n')[:24]

        # In a similar way we could actually search for the correct page that
    # contains 'Ward-wise breakdown of positive cases' instead of hard coded page numbers
    date_box = (770, 50, 875, 80)
    raw_date = positive_cases_pdf_page.within_bbox(date_box).extract_text().strip()
    date = datetime.strptime(raw_date, '%b %d, %Y')


    numeric_columns = ['past.24hrs',
                        'cumulative']
    for column in numeric_columns:
        for n in range(len(data[column])):
            data[column][n]=data[column][n].replace(',','').replace(')','').replace('–','').lstrip().strip()

    for column in numeric_columns:
        data[column] = pd.to_numeric(data[column], errors='coerce')

    df = pd.DataFrame(data)

    df['date'] = date
    df['metric_type'] = graph_name

    return df

def _extract_ward_positive_data(positive_cases_pdf_page,initial_bbox=(95, 450, 900, 470)) -> pd.DataFrame:
    
    wardbox = initial_bbox
    countbox1 = (initial_bbox[0],initial_bbox[1]+20,initial_bbox[2],initial_bbox[3]+20)
    countbox2 = (initial_bbox[0],initial_bbox[1]+30,initial_bbox[2],initial_bbox[3]+30)
    countbox3 = (initial_bbox[0],initial_bbox[1]+40,initial_bbox[2],initial_bbox[3]+40)

    # switching to our naming convention
    boxes = {
        'ward': wardbox,  # ward abbreviation
        'positive': countbox1,  # cases
        'days.to.double': countbox2,  # Discharged column
        'weekly.growth.rate': countbox3,  # deaths column
    }

    data = dict()
    for key, box in boxes.items():
        raw_data = positive_cases_pdf_page.within_bbox(box).extract_text()
        # due to shifting sizes the totals rows sometimes gets included
        # because we know there are only 24 wards we can cut it of by limiting our selves to 24
        data[key] = raw_data.split(' ')[:24]

        # In a similar way we could actually search for the correct page that
    # contains 'Ward-wise breakdown of positive cases' instead of hard coded page numbers
    date_box = (770, 50, 875, 80)
    raw_date = positive_cases_pdf_page.within_bbox(date_box).extract_text().strip()
    date = datetime.strptime(raw_date, '%b %d, %Y')


    numeric_columns = ['positive', 'days.to.double', 'weekly.growth.rate']
    for column in numeric_columns:
        for n in range(len(data[column])):
            data[column][n]=data[column][n].replace(',','').replace('%','').replace('–','').lstrip().strip()

    for column in numeric_columns:
        data[column] = pd.to_numeric(data[column], errors='coerce')
            
    df = pd.DataFrame(data)

    # not available in sheet, but making it consistent with states and districts
    #df['date'] = date
    #df['district'] = 'Mumbai'
    #df['state'] = 'MH'

    return df


def scrape_mumbai_pdf_overall(source_file_path):
    """
    :param source_file_path: 
    :remarks:
    """
    pdf = _read_pdf(source_file_path)

    tables_config = {
        "COVID19 Case Analysis":{'type':'one column',
        "x0": 10,
        "top": 125,
        },
        "COVID19 Bed Management":{'type':'one column',
        "x0": 210,
        "top": 125
        },
        "Containment Measures":{'type':'one column',
        "x0": 715,
        "top": 260,
        },
        "Quarantine Stats":{'type':'one column',
        "x0": 715,
        "top": 180,
        },
        "CCC1 Facilities":{'type':'facilities',
        "x0": 490,
        "top": 70,
        },
        "CCC2 Facilities":{'type':'facilities',
        "x0": 490,
        "top": 70,
        },
        "Contact Tracing":{'type':'tracing',
        "x0": 710,
        "top": 100,
        }
    }

    full_one_column=pd.DataFrame(columns=['metric','count','date','metric_type'])
    full_facilities=pd.DataFrame(columns=['metric','num.facilities','bed.capacity','occupancy','date','metric_type'])
    full_tracing=pd.DataFrame(columns=['metric','past.24hrs','cumulative','date','metric_type'])
    # sealed buildings/floors
            
    try:
        title='Mumbai COVID19 status at a glance'
        page=find_page_general(pdf,title)
        
        for key in tables_config:
            print(key)
            table_type=tables_config[key]['type']
            x0=tables_config[key]['x0']
            top=tables_config[key]['top']
            graph_name=key
            if table_type=='one column':
                df=_extract_data_from_page(page, x0, top, graph_name)
                full_one_column=full_one_column.append(df)
            if table_type=='facilities':
                df=_extract_data_from_page_facilities(page, x0, top, graph_name)
                full_facilities=full_facilities.append(df)
            if table_type=='tracing':
                df=_extract_data_from_page_tracing(page, x0, top, graph_name)
                full_tracing=full_tracing.append(df)

        full_df_2=pd.merge(full_one_column, full_facilities, how='outer', on=['metric', 'date', 'metric_type'])
        full_df_2=pd.merge(full_df_2, full_tracing, how='outer', on=['metric', 'date', 'metric_type'])

    except:
        
        full_df_2 = pd.DataFrame(columns = ['metric', 'count', 'date', 'metric_type', 'num.facilities', 
                                            'bed.capacity', 'occupancy', 'past.24hrs', 'cumulative'])
        
    
    return full_df_2

def scrape_mumbai_pdf(source_file_path):
    """
    :param source_file_path: 
    :remarks:
    """
    pdf = _read_pdf(source_file_path)

    pages_config = {
    "sealed_buildings":{
      "title": "Ward-wise Sealed Buildings (SBs)/Micro-containment zones",
      "top_factor": 10,
      "column_name": 'total.sealedbuildings',
    },
    "sealed_floors":{
      "title": 'Ward-wise Sealed Floors (SFs)',
      "top_factor": 0,
      "column_name": 'total.sealedfloors',
        }
    }

    positive_cases_pdf_page = find_ward_wise_breakdown_page(pdf)
    # new_cases_page = find_ward_wise_new_cases_page(pdf)
    
    full_df = pd.DataFrame()
    # sealed buildings/floors
    try:
        full_df = _extract_wards_data_from_page(positive_cases_pdf_page)
        for page in pages_config:
            pdf_page = find_page_general(pdf,pages_config[page]['title'])
            df = _extract__data_from_page_general(pdf_page, pages_config[page]['top_factor'], pages_config[page]['column_name'])

            full_df=full_df.merge(df, how='outer',on='ward')
        
        title='Mumbai COVID19 status at a glance'
        page=find_page_general(pdf,title)
        positive_df = _extract_ward_positive_data(page)

        full_df=full_df.merge(positive_df, how='outer',on='ward')

    except ValueError: # older versions of the PDF do not contain the sealed buildings/wards page in the format this code has been developed for
        full_df = full_df
        missing_columns = [x for x in ['ward', 'total.confirmed', 'total.recovered', 'total.deceased',
                                       'total.active', 'total.other', 'total.tested', 'date', 'district',
                                       'state', 'total.sealedbuildings', 'total.sealedfloors', 'positive',
                                       'days.to.double', 'weekly.growth.rate'] if x not in full_df.columns]
        for col in missing_columns:
            full_df[col]=None
    
    return full_df
