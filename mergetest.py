import pandas as pd
from zipfile import ZipFile
import PySimpleGUI as sg
#May need to install!
import time
import os

import matplotlib.pyplot as plt
import seaborn as sns

#TODO: Add state codes

def open_zip(zipname, full=False):
    """Extracts a given zip file and inner merges the contents into a dataframe."""
    
    if full:
        # If full file path (C:/...), use that path directly. Otherwise (if
        # being run in same directory as "MSU - HCRIS Project", 
        # use the default path)
        zipfile = zipname
    else:
        zipfile = f"MSU - HCRIS Project/{zipname}.ZIP"
    
    with ZipFile(zipfile, "r") as zipf:
        
        # Generates the names of the contents of the zip file from the given
        # name of the zip file.
        fname = zipfile[zipfile.rfind("/")+1:-4].replace("FY", "_")      
        
        # Opens the CSV files as Pandas dataframes and gives the columns the
        # appropriate names.
        alpha = pd.read_csv(zipf.open(f"{fname}_ALPHA.CSV"), names=
                            ["RPT_REC_NUM","WKSHT_CD","LINE_NUM","CLMN_NUM","ALPHNMRC_ITM_TXT"])
        
        nmrc = pd.read_csv(zipf.open(f"{fname}_NMRC.CSV"), names=
                           ["RPT_REC_NUM","WKSHT_CD","LINE_NUM","CLMN_NUM","ITM_VAL_NUM"])
        
        rpt = pd.read_csv(zipf.open(f"{fname}_RPT.CSV"), names=
                          ["RPT_REC_NUM","PRVDR_CTRL_TYPE_CD","PRVDR_NUM","NPI",
                           "RPT_STUS_CD","FY_BGN_DT","FY_END_DT","PROC_DT",
                           "INITL_RPT_SW","LAST_RPT_SW","TRNSMTL_NUM","FI_NUM",
                           "ADR_VNDR_CD","FI_CREAT_DT","UTIL_CD","NPR_DT",
                           "SPEC_IND","FI_RCPT_DT"])
        
        # TODO: Rollup table is currently unused, so is not loaded.
        
        return rpt, alpha, nmrc
#%%
# test
def reformat(table):
    """Reformats a dataframe to drop the worksheet code column and unflatten
    the rows of the table."""
    table = table.drop(["WKSHT_CD"], axis=1)
    # Sets three (row) indices on the table, then unstacks the CLMN_NUM index
    # into separate columns. The resulting table has multiindexed rows
    # (RPT_REC_NUM, then LINE_NUM), and columns given by the value of CLMN_NUM.
    return table.set_index(["RPT_REC_NUM", "LINE_NUM", "CLMN_NUM"]).unstack()

def get_worksheet(code, alpha, nmrc):
    """Selects entries from the alpha and nmrc tables corresponding to a given
    worksheet code"""
    alpha = alpha[alpha["WKSHT_CD"] == code]
    nmrc = nmrc[nmrc["WKSHT_CD"] == code]
    # Reformats both tables and joins them together.
    joined = reformat(alpha).join(reformat(nmrc), how="outer")
    
    # The resulting joined table has a unneeded multiindex, so it is dropped.
    joined.columns = joined.columns.droplevel()
    
    return joined



def select(df, rows, cols):
    """Returns the slice of a joined dataframe corresponding to all RPT_REC_NUMs
    at a given row and column. Also drops the unneeded RPT_REC_NUM multiindex."""
    # slice(None) instructs df.loc to select all of the uppper index.
    return df.loc[(slice(None), rows), cols]


def get_medicaid_utilization(alpha, nmrc):
    """Constructs medicaid utilization as outlined by company."""
    joined = get_worksheet("S300001", alpha, nmrc)
    
    # droplevel(1)'s are needed in all cases where row single row is selected.
    # TODO: detect this case and roll into select function.
    to_columns = select(joined,(200, 1400, 3200), '00700').unstack(level=1)\
        .join(select(joined, 1400, '00800'), how="outer").droplevel(1).fillna(0)
    to_columns["medicaid_utilization"] = (to_columns[200] + to_columns[1400] + to_columns[3200]) / to_columns["00800"]
    
    return to_columns

def get_ssi_percentage(alpha, nmrc):
    """Gets SSI percentage from field specified by company."""
    joined = get_worksheet("E00A18A", alpha, nmrc)
    return select(joined, 3000, "00100").droplevel(1)

def get_geography(alpha, nmrc):
    joined = get_worksheet("S200001", alpha, nmrc)
    out = select(joined, 2600, "00100").droplevel(1)
    out.name = "GEO"
    return out

def get_states_codes(rpt):
    prv_num = rpt["PRVDR_NUM"] 
    prv_num = prv_num.to_frame()   # series to df  
    prv_num['init_char'] = (prv_num["PRVDR_NUM"] / 10000).astype(int)
       
    dict = {1: 'Alabama', 2: 'Alaska', 3: 'Arizona', 4: 'Arkansas', 5: 'California', 55: 'California', 75: 'California', 6: 'Colorado', 7: 'Connecticut', 8: 'Delaware', 9: 'District of Columbia', 10: 'Florida', 68: 'Florida', 69: 'Florida', 11: 'Georgia', 12: 'Hawaii', 13: 'Idaho', 14: 'Illinois', 78: 'Illinois', 15: 'Indiana', 16: 'Iowa', 76: 'Iowa', 17: 'Kansas', 70: 'Kansas', 18: 'Kentucky', 19: 'Louisiana', 71: 'Louisiana', 20: 'Maine', 21: 'Maryland', 80: 'Maryland', 22: 'Massachusetts', 30: 'New Hampshire', 31: 'New Jersey', 32: 'New Mexico', 33: 'New York', 34: 'North Carolina', 35: 'North Dakota', 36: 'Ohio', 72: 'Ohio', 37: 'Oklahoma', 38: 'Oregon', 39: 'Pennsylvania', 73: 'Pennsylvania', 40: 'Puerto Rico', 41: 'Rhode Island', 42: 'South Carolina', 43: 'South Dakota', 44: 'Tennessee', 45: 'Texas', 67: 'Texas', 74: 'Texas', 46: 'Utah', 47: 'Vermont', 48: 'Virgin Islands', 49: 'Virginia', 50: 'Washington', 51: 'West Virginia', 23: 'Michigan', 24: 'Minnesota', 77: 'Minnesota', 25: 'Mississippi', 26: 'Missouri', 27: 'Montana', 28: 'Nebraska', 29: 'Nevada', 52: 'Wisconsin', 53: 'Wyoming', 56: 'Canada', 59: 'Mexico', 64: 'American Samoa', 65: 'Guam', 66: 'Commonwealth of the Northern Marianas Islands '}
    prv_num['states'] = prv_num['init_char'].map(dict)   
    joined  = prv_num[['PRVDR_NUM', 'states']]
    return joined

def get_s_fields(alpha, nmrc):
    """Gets fields from Worksheet S specified by company."""
    joined = get_worksheet("S100000", alpha, nmrc)
    
    # Since the goal is to select and join many individual fields, use
    # pd.concat since each object is a series, not a dataframe.
    ctc = select(joined, 100, "00100").droplevel(1)
    medicaid_charges = select(joined, 600, "00100").droplevel(1)
    medicaid_cost = select(joined, 700, "00100").droplevel(1)
    charity_charges = select(joined, 2000, "00300").droplevel(1)
    charity_cost = select(joined, 2300, "00300").droplevel(1)
    total_unreimbursed_uncompensated = select(joined, 3100, "00100").droplevel(1)
    # TODO: Verify that this concat works as expected (outer join).
    out = pd.concat([ctc, medicaid_charges, medicaid_cost, 
                     charity_charges, charity_cost,
                     total_unreimbursed_uncompensated], axis=1)
    out.columns = ["CST_TO_CHG", "MED_CHG", "MED_CST",
                   "CHAR_CHG", "CHAR_CST", "TOT_UNR_UNC"]
    
    return out

def display(string, window):
    """Prints action to console or GUI depending on whether GUI window object
    is passed into function."""
    if window:
        window.update(string)
    else:
        print(string)
    
#TODO: Make this function compatible with run_GUI()
def make_summarized_data(zipname, window=None, response=None):
    """Constructs the summarized data in the format specified by the company."""
    
    if response:
        rpt, alpha, nmrc = response
    else:
        display("Opening files...", window)
        rpt, alpha, nmrc = open_zip(zipname)
    # Gets provider numbers and fiscal year end dates.
    id_cols = rpt[["RPT_REC_NUM", "PRVDR_NUM", "FY_END_DT"]].set_index("RPT_REC_NUM")

    display("Getting Medicaid utilization...",window)
    medicaid = get_medicaid_utilization(alpha, nmrc)
    
    display("Getting SSI percentage...", window)
    ssi = get_ssi_percentage(alpha, nmrc)
    
    display("Getting S100000 worksheet fields...", window)
    s_wksht = get_s_fields(alpha, nmrc)
    
    display("Getting geography...", window)
    geo = get_geography(alpha, nmrc)
    
    display("Getting states info...", window)
    states = get_states_codes(rpt)
    
    display("Joining fields...", window)
    summarized = id_cols.join([medicaid, ssi, s_wksht, geo], how="outer")
    # add states
    summarized = summarized.merge(states, on = 'PRVDR_NUM', how = 'left')
    summarized = summarized.drop_duplicates()

    summarized.columns = ["PRVDR_NUM", 
                          "FY_END_DT", 
                          "HMO", 
                          "TOT_HOSP", 
                          "LAB_DEL_DAYS", 
                          "TOT_HOSP", 
                          "MED_UTIL", 
                          "SSI_PER",
                          "CST_TO_CHG",
                          "MED_CHG",
                          "MED_CST",
                          "CHAR_CHG",
                          "CHAR_CST",
                          "TOT_UNR_UNC", 
                          "URBAN_RURAL",
                          "STATES"]
    # Replaces all NaN values with zeroes.
    # TODO: verify that this is the intended behavior.
    summarized.fillna(0, inplace=True)
    summarized["DSH_PAT_PER"] = summarized["MED_UTIL"] + summarized["SSI_PER"]
    
    return summarized
#%%

def run_GUI():
    sg.theme('DarkAmber')
    
    # Creates layout of GUI with given button keys
    layout = [
                [sg.Text("Zip file with data:    "), sg.FileBrowse(key="in_filename")],
                [sg.Text("Output file location: "), sg.FolderBrowse(key="out_location")],
                [sg.Text("Output file name: "), sg.InputText(key="out_filename"), sg.Text(".csv")],
                [],
                [sg.Button('Run'), sg.Button('Cancel')],
                [],
                [sg.Text("Working on: "), sg.Text("", key="working_on")],
            ]
        
  
    # Instantiates window
    window = sg.Window('Generate Summarized Cost Report Data', layout, finalize=True)
    
    # Loops until window is closed or cancelled
    while True:
        # Reads button events
        event, values = window.read()
        
        # If user closes window or presses cancel
        if event == sg.WIN_CLOSED or event == 'Cancel': 
            break
        
        # Opening the zip files takes long enough that the computer declares the
        # window unresponsive. window.perform_long_operation ensures that this
        # does not occur. The result of the long operation (the opened files)
        # are stored in the values dictionary at the given key.
        
        # When run button is pressed
        elif event == 'Run':
            window["working_on"].update("Opening files...")
            window.perform_long_operation(lambda : open_zip(values["in_filename"],
                                    full=True), 'FILES OPENED')
            
        # The event 'FILES OPENED' occurs once the files are opened, as above.
        elif event == "FILES OPENED":
            
            summarized = make_summarized_data(None, window["working_on"], values[event])
            # Saves the summarized dataframe to the desired location.
            summarized.to_csv(f"{values['out_location']}/{values['out_filename']}.csv")
            window["working_on"].update("Finished!")
            
        time.sleep(0.1)
        
    window.close()