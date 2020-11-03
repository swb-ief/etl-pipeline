import gspread
import numpy
import pandas


from pipeline.config import GSPREAD_CLIENT, WORKSHEET_URL

def worksheet_as_df_by_url(sheet_url: str, worksheet_name: str) -> Tuple[gspread.Worksheet, pandas.DataFrame]:
    sheet = GSPREAD_CLIENT.open_by_url(sheet_url)
    worksheet = sheet.worksheet(worksheet_name)
    return worksheet, pandas.DataFrame(worksheet.get_all_records())
    
 worksheet, Rt_df = worksheet_as_df_by_url(WORKSHEET_URL, "Rt")
 
 new_Rt_df = pd.read_csv("/usr/data/epinow2_out.csv")
 
 worksheet.update([new_Rt_df.columns.values.tolist()] + new_Rt_df.values.tolist())
 
 
