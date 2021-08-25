import requests
from datetime import datetime
import os
from zipfile import ZipFile

# import shapefile


# Function to download and extract WA tenements data from DMP
def get_new_data():
    
    # get current directory
    dirPath = os.path.dirname(os.path.abspath(__file__))
    # file URL from DMP
    tenementsFileURL = "https://dasc.dmp.wa.gov.au/dasc/download/file/2056"
    # set date variable for filenaming
    date = datetime.today().strftime('%Y-%m-%d')
    #set filename and path as variable
    filename = f"{dirPath}/data/tenements-{date}.zip"

    # get file and write bytes to file
    r = requests.get(tenementsFileURL)
    open(filename, 'wb').write(r.content)

    # extracts tenements shape file and renames to include download date
    with ZipFile(filename, 'r') as zip_ref:
        zip_ref.extract(member="CurrentTenements.shp", path=f"{dirPath}/data/")
    os.rename(f"{dirPath}/data/CurrentTenements.shp", f"{dirPath}/data/tenements-{date}.shp")
    os.remove(filename)

get_new_data()