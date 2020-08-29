
### Set up Pandas framework
import pandas as pd

ward_positive_df = pd.DataFrame(columns= ["Ward_Name", "Total_Positive_Cases", "Total_Discharged", "Total_Deaths", "Total_Active"])

### Pdfpluber: This works and shows that pulling text is a good idea, but need to try line by line
# Source: https://towardsdatascience.com/how-to-extract-text-from-pdf-245482a96de7
import pdfplumber as pdfplumber
# r'C:\Users\Atiyasha Kaur\Documents\SWB\Covid-19 Dashboard for Large Cities\Dashboard_ward_08_26_2020.pdf'
#r'C:\Users\Atiyasha Kaur\Documents\SWB\Covid-19 Dashboard for Large Cities\Dashboard - 8.21.20.pdf'
source_file_path = r'C:\Users\Atiyasha Kaur\Documents\SWB\Covid-19 Dashboard for Large Cities\Dashboard - 8.24.20.pdf'

pdf = pdfplumber.open(source_file_path)
ward_positive_pdf = pdf.pages[21]
ward_positive_pdf_line = ward_positive_pdf.extract_text().split('\n')
#print(ward_positive_pdf_line)
date_string = ward_positive_pdf_line[1]
print(ward_positive_pdf_line[2] + '\n' + ward_positive_pdf_line[3] + '\n' + ward_positive_pdf_line[6] + '\n' +
      ward_positive_pdf_line[8] + '\n' + ward_positive_pdf_line[10] + '\n' + ward_positive_pdf_line[12]
      + '\n'+ ward_positive_pdf_line[14] + '\n' + ward_positive_pdf_line[16] + '\n' + ward_positive_pdf_line[19]
      + '\n' + ward_positive_pdf_line[20] + '\n' + ward_positive_pdf_line[22] + '\n' + ward_positive_pdf_line[24]
      + '\n' + ward_positive_pdf_line[26] + '\n' + ward_positive_pdf_line[28] + '\n' + ward_positive_pdf_line[30]
      + '\n' + ward_positive_pdf_line[31] + '\n' + ward_positive_pdf_line[32] + '\n' + ward_positive_pdf_line[33]
      + '\n' + ward_positive_pdf_line[36] + '\n' + ward_positive_pdf_line[37] + '\n' + ward_positive_pdf_line[39]
      + '\n' + ward_positive_pdf_line[40] + '\n' + ward_positive_pdf_line[41] + '\n' + ward_positive_pdf_line[42])

KE_ward = ward_positive_pdf_line[2].split(' ')
PN_ward = ward_positive_pdf_line[3].split(' ')
GN_ward = ward_positive_pdf_line[6].split(' ')
KW_ward = ward_positive_pdf_line[8].split(' ')
S_ward = ward_positive_pdf_line[10].split(' ')
RC_ward = ward_positive_pdf_line[12].split(' ')
N_ward = ward_positive_pdf_line[14].split(' ')
RS_ward = ward_positive_pdf_line[16].split(' ')
FS_ward = ward_positive_pdf_line[19].split(' ')
GS_ward = ward_positive_pdf_line[20].split(' ')
L_ward = ward_positive_pdf_line[22].split(' ')
T_ward = ward_positive_pdf_line[24].split(' ')
D_ward = ward_positive_pdf_line[26].split(' ')
E_ward = ward_positive_pdf_line[28].split(' ')
FN_ward = ward_positive_pdf_line[30].split(' ')
ME_ward = ward_positive_pdf_line[31].split(' ')
HE_ward = ward_positive_pdf_line[32].split(' ')
PS_ward = ward_positive_pdf_line[33].split(' ')
MW_ward = ward_positive_pdf_line[36].split(' ')
HW_ward = ward_positive_pdf_line[37].split(' ')
RN_ward = ward_positive_pdf_line[39].split(' ')
A_ward = ward_positive_pdf_line[40].split(' ')
C_ward = ward_positive_pdf_line[41].split(' ')
B_ward = ward_positive_pdf_line[42].split(' ')

ward_positive_df = ward_positive_df.append({"Ward_Name": KE_ward[0], "Total_Positive_Cases": KE_ward[1],
                                            "Total_Discharged": KE_ward[2], "Total_Deaths": KE_ward[3],
                                            "Total_Active": KE_ward[4]}, ignore_index=True)
ward_positive_df = ward_positive_df.append({"Ward_Name": PN_ward[0], "Total_Positive_Cases": PN_ward[1],
                                            "Total_Discharged": PN_ward[2], "Total_Deaths": PN_ward[3],
                                            "Total_Active": PN_ward[4]}, ignore_index=True)
ward_positive_df = ward_positive_df.append({"Ward_Name": GN_ward[0], "Total_Positive_Cases": GN_ward[1],
                                            "Total_Discharged": GN_ward[2], "Total_Deaths": GN_ward[3],
                                            "Total_Active": GN_ward[4]}, ignore_index=True)
ward_positive_df = ward_positive_df.append({"Ward_Name": KW_ward[1], "Total_Positive_Cases": KW_ward[2],
                                            "Total_Discharged": KW_ward[3], "Total_Deaths": KW_ward[4],
                                            "Total_Active": KW_ward[5]}, ignore_index=True)
ward_positive_df = ward_positive_df.append({"Ward_Name": S_ward[0], "Total_Positive_Cases": S_ward[1],
                                            "Total_Discharged": S_ward[2], "Total_Deaths": S_ward[3],
                                            "Total_Active": S_ward[4]}, ignore_index=True)
ward_positive_df = ward_positive_df.append({"Ward_Name": RC_ward[0], "Total_Positive_Cases": RC_ward[1],
                                            "Total_Discharged": RC_ward[2], "Total_Deaths": RC_ward[3],
                                            "Total_Active": RC_ward[4]}, ignore_index=True)
ward_positive_df = ward_positive_df.append({"Ward_Name": N_ward[1], "Total_Positive_Cases": N_ward[2],
                                            "Total_Discharged": N_ward[3], "Total_Deaths": N_ward[4],
                                            "Total_Active": N_ward[5]}, ignore_index=True)
ward_positive_df = ward_positive_df.append({"Ward_Name": RS_ward[0], "Total_Positive_Cases": RS_ward[1],
                                            "Total_Discharged": RS_ward[2], "Total_Deaths": RS_ward[3],
                                            "Total_Active": RS_ward[4]}, ignore_index=True)
ward_positive_df = ward_positive_df.append({"Ward_Name": FS_ward[0], "Total_Positive_Cases": FS_ward[1],
                                            "Total_Discharged": FS_ward[2], "Total_Deaths": FS_ward[3],
                                            "Total_Active": FS_ward[4]}, ignore_index=True)
ward_positive_df = ward_positive_df.append({"Ward_Name": GS_ward[1], "Total_Positive_Cases": GS_ward[2],
                                            "Total_Discharged": GS_ward[3], "Total_Deaths": GS_ward[4],
                                            "Total_Active": GS_ward[5]}, ignore_index=True)
ward_positive_df = ward_positive_df.append({"Ward_Name": L_ward[0], "Total_Positive_Cases": L_ward[1],
                                            "Total_Discharged": L_ward[2], "Total_Deaths": L_ward[3],
                                            "Total_Active": L_ward[4]}, ignore_index=True)
ward_positive_df = ward_positive_df.append({"Ward_Name": T_ward[0], "Total_Positive_Cases": T_ward[1],
                                            "Total_Discharged": T_ward[2], "Total_Deaths": T_ward[3],
                                            "Total_Active": T_ward[4]}, ignore_index=True)
ward_positive_df = ward_positive_df.append({"Ward_Name": D_ward[1], "Total_Positive_Cases": D_ward[2],
                                            "Total_Discharged": D_ward[3], "Total_Deaths": D_ward[4],
                                            "Total_Active": D_ward[5]}, ignore_index=True)
ward_positive_df = ward_positive_df.append({"Ward_Name": E_ward[1], "Total_Positive_Cases": E_ward[2],
                                            "Total_Discharged": E_ward[3], "Total_Deaths": E_ward[4],
                                            "Total_Active": E_ward[5]}, ignore_index=True)
ward_positive_df = ward_positive_df.append({"Ward_Name": FN_ward[0], "Total_Positive_Cases": FN_ward[1],
                                            "Total_Discharged": FN_ward[2], "Total_Deaths": FN_ward[3],
                                            "Total_Active": FN_ward[4]}, ignore_index=True)
ward_positive_df = ward_positive_df.append({"Ward_Name": ME_ward[3], "Total_Positive_Cases": ME_ward[4],
                                            "Total_Discharged": ME_ward[5], "Total_Deaths": ME_ward[6],
                                            "Total_Active": ME_ward[7]}, ignore_index=True)
ward_positive_df = ward_positive_df.append({"Ward_Name": HE_ward[1], "Total_Positive_Cases": HE_ward[2],
                                            "Total_Discharged": HE_ward[3], "Total_Deaths": HE_ward[4],
                                            "Total_Active": HE_ward[5]}, ignore_index=True)
ward_positive_df = ward_positive_df.append({"Ward_Name": PS_ward[0], "Total_Positive_Cases": PS_ward[1],
                                            "Total_Discharged": PS_ward[2], "Total_Deaths": PS_ward[3],
                                            "Total_Active": PS_ward[4]}, ignore_index=True)
ward_positive_df = ward_positive_df.append({"Ward_Name": MW_ward[1], "Total_Positive_Cases": MW_ward[2],
                                            "Total_Discharged": MW_ward[3], "Total_Deaths": MW_ward[4],
                                            "Total_Active": MW_ward[5]}, ignore_index=True)
ward_positive_df = ward_positive_df.append({"Ward_Name": HW_ward[0], "Total_Positive_Cases": HW_ward[1],
                                            "Total_Discharged": HW_ward[2], "Total_Deaths": HW_ward[3],
                                            "Total_Active": HW_ward[4]}, ignore_index=True)
ward_positive_df = ward_positive_df.append({"Ward_Name": RN_ward[1], "Total_Positive_Cases": RN_ward[2],
                                            "Total_Discharged": RN_ward[3], "Total_Deaths": RN_ward[4],
                                            "Total_Active": RN_ward[5]}, ignore_index=True)
ward_positive_df = ward_positive_df.append({"Ward_Name": A_ward[2], "Total_Positive_Cases": A_ward[3],
                                            "Total_Discharged": A_ward[4], "Total_Deaths": A_ward[5],
                                            "Total_Active": A_ward[6]}, ignore_index=True)
ward_positive_df = ward_positive_df.append({"Ward_Name": C_ward[0], "Total_Positive_Cases": C_ward[1],
                                            "Total_Discharged": C_ward[2], "Total_Deaths": C_ward[3],
                                            "Total_Active": C_ward[4]}, ignore_index=True)
ward_positive_df = ward_positive_df.append({"Ward_Name": B_ward[1], "Total_Positive_Cases": B_ward[2],
                                            "Total_Discharged": B_ward[3], "Total_Deaths": B_ward[4],
                                            "Total_Active": B_ward[5]}, ignore_index=True)               
print(ward_positive_df)

### Export dataframe to CSV
## Source: https://towardsdatascience.com/how-to-export-pandas-dataframe-to-csv-

export_file_path = r'C:\Users\Atiyasha Kaur\Documents\SWB\Covid-19 Dashboard for Large Cities\ward_positive8.24.20.csv'
ward_positive_df.to_csv(export_file_path, index=False)







### Reading text data line by line  **Not the method ultimately used**
## Source: https://www.codespeedy.com/read-pdf-file-in-python-line-by-line/
import PyPDF2

# Creating a pdf file object
pdfFileObj = open(source_file_path, 'rb')

# Creating a pdf reader object
pdfReader = PyPDF2.PdfFileReader(pdfFileObj)

# Creating a page object
pageNum = 22
page = pdfReader.getPage(pageNum-1)

# Extracting text from page and splitting it into chunks of lines
# Data stored in list line_text
line_text = page.extractText().split(" ")
#print(line_text)




