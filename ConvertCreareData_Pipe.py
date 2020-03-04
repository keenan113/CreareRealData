"""	
        This is a modified version of the python script Brian Colle sent over on 2020/01/31. It's 
essentially a module that feeds into the "Pipeline.py" script. 

	Works for pressure and temperature as of now. They can be added by specifying a 'keyword' 
and indicating the csv file observation name that the keyword should match. Define this in 
CreareKeyWord global dictionary. We pass this keyword to a dictionary to get the DART kind index 
(in function LookupDARTkind).

	ex. keyword = Pressure
		CreareKeyWord = pressure
		DARTkeyword = MARINE_SFC_ALTIMETER

Issues:
	Currently, not using elevation data. Looks like a lot of values in altitude array are negative.
This is ok if for assimlation for now if we assume they are surface obs and increase error.

EDIT: 02/21/2020 by Keenan Fryer
	Needed to subtract 1 from num_obs variable so that last line of obs file correctly identifies that it's the last observation. Line 91
	Swapped order of seconds and days that are printed to the obs_seq files. Line 102
	In conv2secs function, "date-from" was incorrectly specified as 1970, DART package works from 1601. Line 148

"""

import xarray as xr
import numpy as np
import pandas as pd
from datetime import datetime,timedelta
import os,sys,shutil
import argparse

CreareKeyWord = {'Pressure':'pressure','Temperature':'ble.ambient_temperature'}

def makeOBSfile(path,pio):
	#Bypassing the "HandleArgs" function to make it easier to pass arguments in a script
	infile = path
	outdir = os.path.join(os.getcwd(),'ProcessedObsSeqFiles')
	ObsToProcess = pio

	#Gets us a datetime for the entire obs_seq file
	indate = datetime.strptime(os.path.basename(infile),'WC_Obs_%Y-%m-%d_%H%M%S.csv')

	# Load in Weather Citizen Data
	master_d_in = pd.read_csv(infile,encoding='cp1252',parse_dates=[89]) # convert string to timestamp
	master_d_in.sort_values(by='time',inplace=True)
	master_d_in['pressure'] = master_d_in['pressure']*100

	# Doin' a little QC
	for i in range(len(master_d_in)):
		ob = master_d_in.loc[i]
		if ob['loc_accuracy'] > 50 or ob['post_process.usgs_elevation'] < -10: # Filter out entries with poor positional accuracy and nonsense elevation
			master_d_in = master_d_in.drop([i])

	#Specify output filename.
	outFN = (os.path.join(outdir,"obs_seq_{0:%Y-%m-%d_%H%M%S}.out".format(indate)))

	num_obs_types = len(ObsToProcess) #number of obsevation types (pressure, temperature, windspeed, etc
	num_total_obs = 0
	#Open the output file. We will start by writing actual observations and worry about header later
	with open(outFN,'w') as outfi:

		#Loop over Command line specified variables.
		for var in ObsToProcess:
			#drop all nan values for this variable
			d_in = master_d_in.dropna(axis=0,how='any',subset=[CreareKeyWord[var]])

			#Get Gregorian day and seconds
			times_days,times_seconds = conv2secs(d_in['time'].values)			
			#Read in location information
			lat = d_in['lat'].values
			lon = d_in['lon'].values
			radlat = np.radians(lat)
			radlon = np.radians(lon)
			altitude = d_in['altitude'].values

			#Read in variable specific values
			var2conv = d_in[CreareKeyWord[var]].values

			#How many observations of this kind..
			num_obs = len(var2conv)

			for i in range(0,num_obs):

				#Write each observation into a linked list.
				#TODO: Write this info as a multiline string instead of seperate print statements
				outfi.write(" OBS            {0}\n".format(i+1))
				outfi.write("   {0:.11f}\n".format(var2conv[i])) 
				outfi.write("   {0:.14f}\n".format(1))
				if i == 0:
					outfi.write("        {0}           {1}          {2}\n".format(-1,i+2,-1))
				elif i==num_obs-1:
					outfi.write("        {0}           {1}          {2}\n".format(i,-1,-1))
				else:
					outfi.write("        {0}           {1}          {2}\n".format(i,i+2,-1))

				outfi.write("obdef\n".format())
				outfi.write("loc3d\n".format())
				#Instead of putting altitude information in--may be best to set them as surface obs and specify a higher error value
				outfi.write("     {0:.14f}        {1:.14f}         0.000000000000000     -1\n".format(radlon[i],radlat[i]))
				outfi.write("kind\n")
				outfi.write("           {0}\n".format(LookupDARTkind(var)[0]))
				outfi.write(" {0}     {1}\n".format(times_seconds[i],times_days[i]))
				outfi.write("   2.56000000000000\n".format())
			num_total_obs += num_obs

	#Now, we have full observation information (how many vars, how many obs of each var)
	#Next read  in written file, and add header information to top.
	with open(outFN, 'r') as f:
		f_obs_seq = f.read()

	with open(outFN,'w') as outfile:

		outfile.write(" obs_sequence\n")
		outfile.write("obs_kind_definitions\n")
		outfile.write("       {0}\n".format(num_obs_types))
		for var in ObsToProcess:
			outfile.write("       {0} {1}\n".format(LookupDARTkind(var)[0],LookupDARTkind(var)[1]))


		outfile.write("  num_copies:            1  num_qc:            1\n")
		outfile.write("  num_obs:         {0}  max_num_obs:         {0}\n".format(num_total_obs))

		outfile.write("MADIS observation\n")
		outfile.write("Data QC\n")

		outfile.write("  first:            1  last:       {0}\n".format(num_total_obs))

		#Write rest of obs_seq file to header file
		outfile.write(f_obs_seq)

def LookupDARTkind(observationType):
	DARTInfo = {
					"MARINE_SFC_U_WIND_COMPONENT":      [25,   "MARINE_SFC_U_WIND_COMPONENT"],
                  	"MARINE_SFC_V_WIND_COMPONENT":      [26,   "MARINE_SFC_V_WIND_COMPONENT"],
                  	"MARINE_SFC_TEMPERATURE":           [27,   "MARINE_SFC_TEMPERATURE"],
                  	"MARINE_SFC_TEMPERATURE":           [27,   "MARINE_SFC_TEMPERATURE"],
                  	"MARINE_SFC_SPECIFIC_HUMIDITY":     [28,   "MARINE_SFC_SPECIFIC_HUMIDITY"],
                    "U_WIND":      						[25,   "MARINE_SFC_U_WIND_COMPONENT"],
                    "V_WIND":      						[26,   "MARINE_SFC_V_WIND_COMPONENT"],
                    "Temperature":          			[27,   "MARINE_SFC_TEMPERATURE"],
                    "SPECIFIC_HUMIDITY":    			[28,   "MARINE_SFC_SPECIFIC_HUMIDITY"],
					"Pressure":							[3,"MARINE_SFC_ALTIMETER"]
				}

	return DARTInfo[observationType]

def conv2secs(dftime):
	stime = datetime(1601,1,1,0,0)
	stringtimes = [str(t)[0:19] for t in dftime]
	obtime = [datetime.strptime(t,'%Y-%m-%d %H:%M:%S') for t in stringtimes]
	timedeltas = [t - stime for t in obtime]
	# totsecs = [(t.days*24*60*60)+t.seconds for t in timedeltas]
	#daysecs = [(t.days,t.seconds) for t in timedeltas]
	sec = [(t.seconds) for t in timedeltas]
	day = [(t.days) for t in timedeltas]

	return day,sec

def HandleArgs(argv):
	parser = argparse.ArgumentParser(description='Converts csv data to obs_seq files ready for assimilation using DART.',epilog='e.g. python ConvertCreareData.py ./SensorData_2019_06_20.csv Pressure Temperature')
	parser.add_argument('infilename',action='store',metavar='TopPath',help='Working directory')
	parser.add_argument('vars',action='store',nargs='+',metavar='ObsTypes',help='Variables to add to obs_seq file. Can list multiple vars.',choices=['Pressure','Temperature','U','V'])
	parser.add_argument('-out',action='store',metavar='NewDirectory',help='Optional special directory name')
	# parser.add_argument('start_time',action='store',nargs=1,metavar='YYYYMMDDHH',help='the first time to be copied')
	# parser.add_argument('end_time',action='store',nargs=1,metavar='YYYYMMDDHH',help='the last time to be copied')
	parser.add_argument('-o','--overwrite',action='store_true',help='if used, will overwrite contents of destination directory')

	args = parser.parse_args(argv)
	infile = os.path.abspath(args.infilename)
	ObsTypes = args.vars
	
	if not os.path.exists('ProcessedObsSeqFiles'): #create acculmulation folder if one does not already exist
		os.makedirs('ProcessedObsSeqFiles')	

	outdir = os.path.join(os.getcwd(),'ProcessedObsSeqFiles')

	return infile,outdir,ObsTypes

def cleanup(data): # This function cleans up the raw WeatherCitizen data into a clean human-readable spreadsheet. The Obs files are then created from those spreadsheets.
	data = data.dropna(subset=['geometry.coordinates'])
	data = data.sort_values(by='properties.time') # organize by record time

	data =  data.reset_index(drop=True) # reset index

	for col in data.columns: # Remove "property" from the column headers
		if col[0:4] == "prop":
			nn = col[11:]
			data = data.rename(columns={col: nn})

	data = data.drop(['_etag', '_updated', 'geometry.type', 'type', 'cpu_average', 'cpu_current'], 1) # drop unneccesary columns

	for col in data.columns: # Remove "ble_map" columns (they are pointless)
		if col[0:7] == "ble_map":
			data = data.drop(col, 1)

	data['lat'] = '' # Convert geometry.coordinates to lat and lon
	data['lon'] = ''

	for i in range(0, len(data)):
		pair = data.iloc[i]['geometry.coordinates']
		data.at[i, 'lat'] = pair[1]
		data.at[i, 'lon'] = pair[0]
	data = data.drop(['geometry.coordinates'], 1)

	names =  ['accelerometer', 'gravity', 'gyroscope', 'linear_acceleration', 'magnetic_field', 'orientation'] # Break out accelerometer into x,y,z
	for name in names:
		data[name + '_x'] = ''
		data[name + '_y'] = ''
		data[name + '_z'] = ''


		for i in range(0, len(data)):
			stuff = data.iloc[i][name]
			if type(stuff) is list and len(stuff) == 3: 
				data.at[i, name + '_x'] = stuff[0]
				data.at[i, name + '_y'] = stuff[1]
				data.at[i, name + '_z'] = stuff[2]
		data = data.drop([name], 1)

	for col in data.columns: # Eliminate duplicate columns from different BLE devices
		if col[0:3] == 'ble':
			for i in range(5, len(col)):
				if col[i] == '.':
					param = col[i+1:]
					data = data.rename(columns={col: 'ble.' + param})    

	return data

if __name__ == '__main__':
	makeOBSfile(sys.argv[1::])
