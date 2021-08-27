#!/usr/bin/env python3

"""WATenementsMonitor downloads recent WA mining tenement data and notifies of 
changes to any holdings.
"""

__author__ = "James Keillor"
__version__ = "0.1"


import requests
from datetime import datetime, timedelta, date
import os
from os.path import exists
from zipfile import ZipFile
import pandas as pd
import pytz



# set global varibales

# set current and previous weekday dates
timezone = pytz.timezone('Australia/Perth')
currDate = datetime.now(timezone).strftime('%Y-%m-%d')
prevWeekDay = str(datetime.now(timezone) - timedelta(days=[3,1,1,1,1,1,2][date.today().weekday()]))[:10]

print(currDate)
print(prevWeekDay)

# set tenementsZIP variables
dirPath = os.path.dirname(os.path.abspath(__file__))
tenementsZIP = f"{dirPath}/data/tenements.zip"
extractPath = f"{dirPath}/data/"
tenementsCurrent = f"{dirPath}/data/tenements-{currDate}.csv"
tenementsPrevious = f"{dirPath}/data/tenements-{prevWeekDay}.csv"


# Function to download and extract WA tenements data from DMP
def get_new_data():
    """Retrieves most recent tenements data.
    """
    
    tenementsFileURL = "https://dasc.dmp.wa.gov.au/dasc/download/file/2053"
    print("Downloading new data.")
    # get file and write bytes to file
    req = requests.get(tenementsFileURL)
    if req.status_code != 200:
        print(f"Error! Status code {req.status_code} recieved when attempting to download data.")
    else:
        open(tenementsZIP, 'wb').write(req.content)
        # todo - verify integrity of download

    # extracts tenements shape file and renames to include download currDate
    with ZipFile(tenementsZIP, 'r') as zip_ref:
        zip_ref.extract(member="CurrentTenements.csv", path=extractPath)
    os.rename(f"{dirPath}/data/CurrentTenements.csv", tenementsCurrent)
    os.remove(tenementsZIP)

def load_data_to_frame(currCSV, prevCSV):
    """Takes two csv inputs, cleans data and loads to dataframes

    Args:
        currCSV (path): path to most recent csv
        prevCSV (path): path to second most recent csv
    """
    global dfCurrent, dfPrevious
    # columns not used for comparison
    wasteColumns = ['ADDR1', 'ADDR2', 'ADDR3',
                    'ADDR4', 'ADDR5', 'ADDR6',
                    'ADDR7', 'ADDR8', 'ADDR9',
                     'STARTDATE', 'STARTTIME',
                     'ENDDATE', 'ENDTIME', 'GRANTDATE',
                     'GRANTTIME', 'UNIT_OF_MEASURE', 'EXTRACT_DATE',
                     'COMBINED_REPORTING_NO', 'ALL_HOLDERS']

    print("Loading data to frames.")
    dfCurrent = pd.read_csv(currCSV)
    dfPrevious = pd.read_csv(prevCSV, encoding='latin1')

    for frame in dfCurrent, dfPrevious:
        frame.drop(columns=wasteColumns, inplace=True, axis=1)

    return dfCurrent, dfPrevious

def compare_data(dfCurrent, dfPrevious):

    print("Comparing data.")
    dfCompare = pd.merge(dfCurrent, dfPrevious, how='outer', indicator=True)
    dfChanges = dfCompare.loc[dfCompare['_merge'] != 'both']

    dfChanges.to_csv(f"{extractPath}tenementChanges-{currDate}.csv")


def main():
    
    # if running on weekday
    if date.today().weekday() <= 4:
        # check if file already extracted 
        if exists(tenementsCurrent):
            load_data_to_frame(tenementsCurrent, tenementsPrevious)
            compare_data(dfCurrent, dfPrevious)
        else:
        # if no data, download and compare
            get_new_data()
            load_data_to_frame(tenementsCurrent, tenementsPrevious)
            compare_data(dfCurrent, dfPrevious)
    else:
        load_data_to_frame(tenementsCurrent, tenementsPrevious)
        compare_data(dfCurrent, dfPrevious)

main()