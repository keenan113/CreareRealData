"""
2020-02-20

Creare - Eric Desjardins

This is an end-to-end pipeline script to download data from the WeatherCitizen server, convert it into hourly human-readable CSV files, and then process those files into MADIS readable Obs files. 

The script creates two folders in the directory the script it run from; one for the CSVs, one for the Obs files.

The only parameters the user will need to set are in lines 21-30. 

Two additional python files are needed to run this script: weathercitizen.py and ConvertCreareData_Pipe.py

"""

import weathercitizen
import ConvertCreareData_Pipe as CCD
import pandas as pd
import os

# set params

uuid = '2r2p9wgz' # add device uuid here
start_time = '2019-06-20 08:00:00' # UTC +4 (EST)
end_time = '2019-06-20 16:00:00' # UTC +4 (EST)
box = [[-73, 40], [-72, 41]] 
poi = ["Pressure","Temperature"]

# fetch the data 

items = weathercitizen.get(collection=["sensors", "geosensors"], start_time=start_time, end_time=end_time)
#items = weathercitizen.get(collection=["sensors", "geosensors"], uuid=uuid, start_time=start_time, end_time=end_time, box=box)

# Clean Up Data and Organize 

data = weathercitizen.to_dataframe(items)
data = CCD.cleanup(data)

# Break up into hourly df's and CSVs

if not os.path.exists('hourly_data_csv'): #create acculmulation folder if one does not already exist
    os.makedirs('hourly_data_csv')

curr_hour = pd.to_datetime(start_time, utc = True)
curr_hour = curr_hour.replace(minute=0,second=0,microsecond=0)
data['time'] = pd.DatetimeIndex(data['time'])
    
while curr_hour < pd.to_datetime(end_time, utc = True): #sort
        
    subD = data.loc[data['time']>curr_hour]
    subD = subD.loc[data['time']<curr_hour + pd.Timedelta(hours=1)]
        
    subD.to_csv("hourly_data_csv\WC_Obs_{0:%Y-%m-%d_%H%M%S}.csv".format(curr_hour))
        
    curr_hour = curr_hour + pd.Timedelta(hours=1)

#Take hourly files and convert

if not os.path.exists('ProcessedObsSeqFiles'): #create acculmulation folder if one does not already exist
    os.makedirs('ProcessedObsSeqFiles')
    
for file in os.listdir('hourly_data_csv'):
    
    path = os.path.join(os.getcwd(),'hourly_data_csv',file)
    
    #args = [path,poi]
    
    CCD.makeOBSfile(path,poi)
    
