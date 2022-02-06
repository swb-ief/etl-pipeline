import pdfplumber
from datetime import datetime
import pandas as pd
import numpy as np
import re

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
    checkagainstlist = checklist
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
    
    wardbox = (x0, top-top_factor, x0+35, top+425)
    confirmedbox = (x0+35, top-top_factor, outer_boundary, top+425)

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
    

    if graph_name=='COVID19 Case Analysis':  
        x, y = get_text_coordinate(positive_cases_pdf_page, x0, top, 110, 40, 'Total Positive', xstep=5, ystep=2)
        metricbox = (x, y, x+138, y+310)
        countbox = (x+138, y, x+205, y+315)
    
    if graph_name=='COVID19 Bed Management':  
        x, y = get_text_coordinate(positive_cases_pdf_page, x0, top, 110, 25, 'Bed Capacity', xstep=5, ystep=2, exact_matching=False)
        metricbox = (x, y, x+125, y+315)
        countbox = (x+125, y, x+203, y+315)
    
    
    if graph_name=='Containment Measures':
        x, y = get_text_coordinate(positive_cases_pdf_page, x0, top, 160, 50, 'Active Containment Zones –\nSlums & Chawls', xstep=5, ystep=2, exact_matching=False)
        metricbox = (x, y, x+170, y+173)
        countbox = (x+170, y, x+215, y+173)
        
    if graph_name=='Quarantine Stats':
        x, y = get_text_coordinate(positive_cases_pdf_page, x0, top, 100, 35, 'Total Quarantine \nCompleted', xstep=5, ystep=2, exact_matching=False)
        metricbox = (x, y, x+140, y+70)
        countbox = (x+140, y, x+215, y+70)

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
        
    if graph_name=='COVID19 Case Analysis':
        for i, item in enumerate(data['metric']):
            if 'total positive' in item.lower():
                data['metric'][i]='total.positive'
            elif 'discharged' in item.lower():
                data['metric'][i]='total.discharged'
            elif '50yr' in item.lower():
                data['metric'][i]='deaths.above.50yrs'
            elif 'deaths' in item.lower():
                data['metric'][i]='total.deaths'
            elif 'active' in item.lower():
                data['metric'][i]='total.active'
            elif 'asymptom' in item.lower():
                data['metric'][i]='active.asymptomatic'
            elif 'symptom' in item.lower():
                data['metric'][i]='active.symptomatic'
            elif 'critical' in item.lower():
                data['metric'][i]='active.critical'
            elif 'tests' in item.lower():
                data['metric'][i]='total.tests'
            elif 'of positive' in item.lower():
                data['metric'][i]='percentage.positive'
                
                
        
    if graph_name=='COVID19 Bed Management':
        data['metric']=['bed.capacity.dchc.dch.ccc2',
              'bed.occupied.dchc.dch.ccc2',
              'bed.available.dchc.dch.ccc2',
              'bed.capacity.dchc.dch',
              'bed.occupied.dchc.dch',
              'bed.available.dchc.dch',
              'bed.capacity.o2',
              'bed.occupied.o2',
              'bed.available.o2',
              'bed.capacity.icu',
              'bed.occupied.icu',
              'bed.available.icu',
              'bed.capacity.ventilator',
              'bed.occupied.ventilator',
              'bed.available.ventilator']

                
    if graph_name=='Containment Measures':
        
        active_ind=0
        released_ind=0
        slums_ind=0
        micro_ind=0
        floors_ind=0
        
        for i, item in enumerate(data['metric']):
            if 'slums' in item.lower():
                slums_ind+=1
            if 'active' in item.lower():
                active_ind+=1
            if 'released' in item.lower():
                released_ind+=1
            if 'micro' in item.lower():
                micro_ind+=1
            if 'floors' in item.lower():
                floors_ind+=1
        
        new_metrics = [] 
        if active_ind>0 and slums_ind>0:
            new_metrics.append('containment.zones.active.slums.chawls')
        if released_ind>0 and slums_ind>0:
            new_metrics.append('containment.zones.released.slums.chawls')
        if active_ind>0 and micro_ind>0:
            new_metrics.append('containment.zones.active.micro.sealed.buildings')
        if released_ind>0 and micro_ind>0:
            new_metrics.append('containment.zones.released.micro.sealed.buildings')
        if floors_ind>0:
            new_metrics.append('floors.sealed')
        
        data['metric'] = new_metrics
    
    
    if graph_name=='Quarantine Stats':
        
        home_ind=0
        total_ind=0
        
        for i, item in enumerate(data['metric']):
            if 'home' in item.lower():
                home_ind+=1
            if 'total' in item.lower():
                total_ind+=1
        
        new_metrics = [] 
        if total_ind>0:
            new_metrics.append('total.quarantine.completed')
        if home_ind>0:
            new_metrics.append('currently.quarantined.home')
        
        data['metric'] = new_metrics

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


def get_text_coordinate(positive_cases_pdf_page, x0, y0, width, height, text, xstep=5, ystep=2, exact_matching=True):
    
    # check if original bbox has the required text
    def minimatcher(text1, text2, exmatch=exact_matching):
        if exact_matching:
            return text1==text2
        return bool(re.search(re.sub("[^0-9a-zA-Z\s]+", '', re.escape(text2.lower())), re.sub("[^0-9a-zA-Z\s]+", '', re.escape(text1.lower()))))
        
    if not minimatcher(positive_cases_pdf_page.within_bbox((x0, y0, x0+width, y0+height)).extract_text(), text, exact_matching):
        raise Exception("Specified initial bbox doesn't contain the specified text")
        
    x = x0
    y = y0    
    
    # keep shifting x coordinate of box until text is no longer captured in the bbox
    while minimatcher(positive_cases_pdf_page.within_bbox((x, y, x+width, y+height)).extract_text(), text, exact_matching):
        x+=xstep
    # final x coordinate
    x-=xstep
    
    # keep shifting y coordinate of box until text is no longer captured in the bbox
    while minimatcher(positive_cases_pdf_page.within_bbox((x, y, x+width, y+height)).extract_text(), text, exact_matching):
        y+=ystep
    # final y coordinate
    y-=ystep
    
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
        data[key] = raw_data.split('\n')
        
    data['metric'] = [x for x in data['metric'] if re.search('total|active|buffer|reserve', x.lower())]
        
    for i in range(len(data['metric'])):
        if 'total' in data['metric'][i].lower():
            data['metric'][i] = 'total.' + ".".join(graph_name.lower().split())
        if 'active' in data['metric'][i].lower():
            data['metric'][i] = 'active.' + ".".join(graph_name.lower().split())
        if 'buffer' in data['metric'][i].lower():
            data['metric'][i] = 'buffer.' + ".".join(graph_name.lower().split())
        if 'reserve' in data['metric'][i].lower():
            data['metric'][i] = 'reserve.' + ".".join(graph_name.lower().split())

        # In a similar way we could actually search for the correct page that
    # contains 'Ward-wise breakdown of positive cases' instead of hard coded page numbers
    date_box = (770, 50, 875, 80)
    raw_date = positive_cases_pdf_page.within_bbox(date_box).extract_text().strip()
    date = datetime.strptime(raw_date, '%b %d, %Y')

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

    for i in range(len(data['metric'])):
        if 'total' in data['metric'][i].lower():
            data['metric'][i] = 'total.contact.traced'
        if 'high' in data['metric'][i].lower():
            data['metric'][i] = 'contact.traced.high.risk'
        if 'low' in data['metric'][i].lower():
            data['metric'][i] = 'contact.traced.low.risk'
            
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

def _extract_ward_positive_data(positive_cases_pdf_page, wards_corner_initial=(10, 450)) -> pd.DataFrame:
    
    x, y = get_text_coordinate(positive_cases_pdf_page, wards_corner_initial[0], wards_corner_initial[1], width=40, height=18, text='Wards', xstep=1, ystep=1)
    
    wardbox = (x +75, y -3, x +72+818, y -3+16)
    countbox1 = (x +75, y -3+16, x +72+818, y -3+16+16)
    countbox2 = (x +75, y -3+16+16-1, x +72+818, y -3+16+16+16)
    countbox3 = (x +75, y -3+16+16+16-1, x +72+818, y -3+16+16+16+16)
    
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
    df['date'] = date

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
        "top": 115,
        },
        "COVID19 Bed Management":{'type':'one column',
        "x0": 220,
        "top": 120
        },
        "Containment Measures":{'type':'one column',
        "x0": 730,
        "top": 270,
        },
        "Quarantine Stats":{'type':'one column',
        "x0": 735,
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
            
    title='Mumbai COVID19 status at a glance'
    page=find_page_general(pdf,title)
    
    full_df_2 = pd.DataFrame(columns = ['metric', 'count', 'date', 'metric_type', 'num.facilities', 
                                        'bed.capacity', 'occupancy', 'past.24hrs', 'cumulative'])
    
    full_df_2 = full_df_2.set_index(['date', 'metric', 'metric_type'])
    
    for key in tables_config:
        try:
            print(key)
            table_type=tables_config[key]['type']
            x0=tables_config[key]['x0']
            top=tables_config[key]['top']
            graph_name=key
            if table_type=='one column':
                df=_extract_data_from_page(page, x0, top, graph_name)
                full_df_2=full_df_2.combine_first(df.set_index(['date', 'metric', 'metric_type']))
            if table_type=='facilities':
                df=_extract_data_from_page_facilities(page, x0, top, graph_name)
                full_df_2=full_df_2.combine_first(df.set_index(['date', 'metric', 'metric_type']))
            if table_type=='tracing':
                df=_extract_data_from_page_tracing(page, x0, top, graph_name)
                full_df_2=full_df_2.combine_first(df.set_index(['date', 'metric', 'metric_type']))
        except:
            print('Skipping '+key)
            
    full_df_final = full_df_2.reset_index(drop=False)
    
    return full_df_final

def scrape_mumbai_pdf(source_file_path):
    """
    :param source_file_path: 
    :remarks:
    """
    pdf = _read_pdf(source_file_path)

    pages_config = {
    "sealed_buildings":{
      "title": "Ward-wise Sealed Buildings",
      "top_factor": 10,
      "column_name": 'total.sealedbuildings',
    },
    "sealed_floors":{
      "title": 'Ward-wise Sealed Floors',
      "top_factor": 0,
      "column_name": 'total.sealedfloors',
        }
    }

    positive_cases_pdf_page = find_ward_wise_breakdown_page(pdf)
    # new_cases_page = find_ward_wise_new_cases_page(pdf)
    
    full_df = pd.DataFrame(columns = ['ward', 'total.confirmed', 'total.recovered', 'total.deceased',
                                       'total.active', 'total.other', 'total.tested', 'date', 'district',
                                       'state', 'total.sealedbuildings', 'total.sealedfloors', 'positive',
                                       'days.to.double', 'weekly.growth.rate'])
    
    full_df = full_df.set_index(['date', 'state', 'district', 'ward'])
    # sealed buildings/floors
    try:
        df1 = _extract_wards_data_from_page(positive_cases_pdf_page)
        full_df = full_df.combine_first(df1.set_index(['date', 'state', 'district', 'ward']))
    except: # older versions of the PDF do not contain the sealed buildings/wards page in the format this code has been developed for
        print("Skipping extraction of main stats")
    
    for page in pages_config:
        try:
            pdf_page = find_page_general(pdf,pages_config[page]['title'])
            df = _extract__data_from_page_general(pdf_page, pages_config[page]['top_factor'], pages_config[page]['column_name'])
            df['state']='MH'
            df['district']='Mumbai'
            df['date'] = full_df.index[0][0]
            full_df = full_df.combine_first(df.set_index(['date', 'state', 'district', 'ward']))
        except:
            print("Skipping extraction of sealed building/floors")
    
    try:
        title='Mumbai COVID19 status at a glance'
        page=find_page_general(pdf,title)
        positive_df = _extract_ward_positive_data(page) 
        positive_df['state']='MH'
        positive_df['district']='Mumbai'
        full_df = full_df.combine_first(positive_df.set_index(['date', 'state', 'district', 'ward']))
    except:
        print("Skipping extraction of page 2 ward-wise stats")
        
    full_df_final = full_df.reset_index(drop=False)
        
    return full_df_final
