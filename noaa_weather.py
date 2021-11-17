#TL - Thomas Logue

import ftplib
import os
import datetime
import glob
import gzip
import sys
import time
import pandas as pd
import multiprocessing as mp
from random import randrange
from io import StringIO
from tqdm import tqdm

pd.options.mode.chained_assignment = None #supresses a warning - TL
cpu_count = mp.cpu_count()




current_year = datetime.datetime.today().year

'''
check if weather data folder needed for processing exists in path
if it doesn't exist, the folders are created - TL
noaa_weather/raw
noaa_weather/csv
'''
def check_exists(path):
	if os.path.isdir(path + 'noaa_weather/') == False:
		os.makedirs(path + 'noaa_weather/raw')
		os.makedirs(path + 'noaa_weather/csv')
		return 'paths created'
	elif os.path.isdir(path + 'noaa_weather/raw') == False:
		os.makedirs(path + 'noaa_weather/raw')
		return 'raw path created'
	elif os.path.isdir(path + 'noaa_weather/csv') == False:
		os.makedirs(path + 'noaa_weather/csv')
		return 'csv path created'
	else:
		return 'Paths Exists'

#gets up to date list of all stations -TL
def get_isd_history(path):
	if os.path.isdir(path + 'noaa_weather') == False:
		os.makedirs(path + 'noaa_weather')
	file = 'isd-history.csv'
	ftp = ftplib.FTP('ftp.ncdc.noaa.gov')
	ftp.login("anonymous", "")
	ftp.cwd('pub/data/noaa/')
	file_name = os.path.join(path + 'noaa_weather/', file)
	lf = open(file_name, "wb")
	ftp.retrbinary("RETR " + file, lf.write)
	lf.close()



#all in one code for downloading, processing and validating. - TL
def read_weather(path, start_year=2010, end_year=current_year, states=['CA', 'TX', 'UT', 'NV', 'CO', 'OR', 'WA', 'AZ', 'ID']):
	print(check_exists(path))
	mp_download(start_year=start_year, end_year=end_year, path=path, states=states)
	mp_convert_csv(start_year=start_year, end_year=end_year, path=path)
	######################
	#need to add validation function - TL
	######################


def download_year(args):
	#args arr from the multiprocessing code - TL
	year = args[0]
	path = args[1]
	states = args[2]
	year = str(year)

	'''
	sleep is a random amount of seconds before the download begins, according to a ranndom document
	I found online there are 5 servers behind a load balancer, and each server allows for 5 connections
	If you space out the connections, you are more likely to connect to a different server. This didn't really
	work in theory, max connections I could get was 5 each at 2.5 Mbps - TL.
	'''

	sleep = randrange(5) + randrange(5) + randrange(10)
	time.sleep(sleep) #pauses code for set amount of time - TL

	#Connect to FTP
	ftp = ftplib.FTP('ftp.ncdc.noaa.gov')
	ftp.login("anonymous", "") # Random website says to use your email address, haven't tried - TL
	ftp.cwd('pub/data/noaa/' +year)
	ftp_files = ftp.nlst() #list all files

	df = pd.read_csv(path +'noaa_weather/isd-history.csv', low_memory=False)
	df_ca = df.loc[df['STATE'].isin(states)] #gets df of stations within the states we specify - TL

	#Joins USAF and WBAN column, to create a filename column -TL
	df_ca['FILE NAME'] = df_ca['USAF'].astype(str) + '-' + df_ca['WBAN'].astype(str)
	#create list of all filenames we need-TL
	files_down = list(df_ca['FILE NAME'] + '-' + year +'.gz')

	#list of files that are possible to download -TL
	download_files = list(set(files_down) & set(ftp_files))

	#add year to path - TL
	if os.path.isdir(path + 'noaa_weather/raw/'+year) == False:
		os.makedirs(path + 'noaa_weather/raw/'+year)

	#downloads and saves file - TL
	for file in tqdm(download_files, desc = year):
		file_name = os.path.join(path+"noaa_weather/raw/"+year+"/", file)
		lf = open(file_name, "wb")
		ftp.retrbinary("RETR " + file, lf.write)
	lf.close()
	print('Downloaded Files')







#Program to convert .gz files to csv -TL	
def convert_to_csv(args):

	#args arr from the multiprocessing code - TL
	year = args[0]
	path = args[1]
	files =args[2]
	year = str(year)
	print(path)


	#error handleing
	errors = pd.DataFrame(columns = ['File', 'Error']) 
	errors.to_csv(path +'noaa_weather/csv/'+year+'error_log.csv', index = False)

	'''
	The below code iterates over all the .gz files in the directory (by year) and then
	loads it into a dateframe, and then during the iteration each dataframe is concatonated
	to the last createing one big dataframe (this can be memory intensive), after you are left
	with a massive dataframe with two columns.
	'''

	######################
	#may want to consider using SQL server, or having sql option -TL
	#example below for mysql
	#from sqlalchemy import create_engine
	#engine = create_engine("mysql+pymysql://uname:passwrd@IP_or_FQDN/dbname")
	#df.to_sql(con=engine, name = 'dbname', if_exists='append')
	######################

	#Empty dataframe -TL
	df = pd.DataFrame(columns =['Data'])
	#get all files in directory - TL
	files = glob.glob(path+'noaa_weather/raw/'+year+"/*")
	#gzip will decrompress - TL
	for file in tqdm(files, desc=year):
		try:
			with gzip.open(file, 'rb') as f:
				file_content = f.read()
				f.close()
				s=str(file_content,'utf-8') #for pandas to read - TL
				data = StringIO(s) #for pandas to read - TL
				df_open = pd.read_csv(data, low_memory= False, header=None, names= ['Data'])
				df = pd.concat([df, df_open])
		except Exception as e:
			error_dict = {'File' : file, 'Error': e}
			temp_error = pd.read_csv(path +'noaa_weather/csv/'+year+'error_log.csv') 
			temp_error = temp_error.append(error_dict, ignore_index = True)
			temp_error.to_csv(path +'noaa_weather/csv/'+year+'error_log.csv', index = False)


	'''
	The below code creates the columns by the positional information provided by NOAA, 
	see isd-format-document.pdf. This does not include anything after Position 105, which may 
	need to be added later for infomation such as precipitation, snow, etc. -TL

	'''
	df['TVC'] = df['Data'].str[0:4]
	df['USAF'] = df['Data'].str[4:10]
	df['WBAN'] = df['Data'].str[10:15]
	df['DATE'] = df['Data'].str[15:23]
	df['TIME'] = df['Data'].str[23:27]
	df['SOURCE'] = df['Data'].str[27:28]
	df['LAT'] = df['Data'].str[28:34]
	df['LONG'] = df['Data'].str[34:41]
	df['TYPE'] = df['Data'].str[41:46]
	df['ELEV'] = df['Data'].str[46:51]
	df['FWSID'] = df['Data'].str[51:56]
	df['MPOQC'] = df['Data'].str[56:60]
	df['WIND_ANGLE'] = df['Data'].str[60:63]
	df['WIND_QC'] = df['Data'].str[63:64]
	df['WIND_TYPE'] = df['Data'].str[64:65]
	df['WIND_SPEED'] = df['Data'].str[65:69]
	df['WIND_SPEED_QC'] = df['Data'].str[69:70]
	df['SKY_OBS'] = df['Data'].str[70:75]
	df['SKY_OBS_QC'] = df['Data'].str[75:76]
	df['SKY_OBS_CIEL'] = df['Data'].str[76:77]
	df['SKY_CAVOK'] = df['Data'].str[77:78]
	df['VISIBILITY_DIST'] = df['Data'].str[78:84]
	df['VISIBILITY_QC'] = df['Data'].str[84:85]
	df['VISIBILITY_VAR'] = df['Data'].str[85:86]
	df['VISIBILITY_QUALITY_VAR'] = df['Data'].str[86:87]
	df['AIR_TEMP'] = df['Data'].str[87:92]
	df['AIR_TEMP_QC'] = df['Data'].str[92:93]
	df['AIR_TEMP_DEW'] = df['Data'].str[93:98]
	df['AIR_TEMP_DEW_QC'] = df['Data'].str[98:99]
	df['ATM_PRESSURE'] = df['Data'].str[99:104]
	df['ATM_PRESSURE_QC'] = df['Data'].str[104:105]

	'''
	The  below code converts date and time to datatime objects, and asjusts the scale of Lat, Long,
	Wind Speed, air_temp, dew point, and pressure. 
	'''

	df['DATE'] = pd.to_datetime(df['DATE'], format='%Y%m%d').dt.date
	df['TIME'] = df['TIME'].astype(str).str.zfill(4) #needed for time converstion, adds leading 0 so 56 becomes 0056.
	df['TIME'] = pd.to_datetime(df['TIME'], format='%H%M').dt.time

	df['LAT'] = df['LAT'].astype('int64')/1000
	df['LONG'] = df['LONG'].astype('int64')/1000

	df['WIND_SPEED'] = df['WIND_SPEED'].astype('int64')/10
	df['AIR_TEMP'] = df['AIR_TEMP'].astype('int64')/10
	df['AIR_TEMP_DEW'] = df['AIR_TEMP_DEW'].astype('int64')/10
	df['ATM_PRESSURE'] = df['ATM_PRESSURE'].astype('int64')/10



	if files == 'large':
		print("Saving Large File")
		df.to_csv(path+"noaa_weather/csv/large"+year+".csv", index = False)
	elif files == 'small':
		del df['Data']
		print("Saving Small File")
		df.to_csv(path+"noaa_weather/csv/small"+year+".csv", index = False)
	else:
		print("Saving Large File")
		df.to_csv(path+"noaa_weather/csv/large"+year+".csv", index = False)
		del df['Data']
		print("Saving Small File")
		df.to_csv(path+"noaa_weather/csv/small"+year+".csv", index = False)





#multiprocessing download code code - TL

def mp_download(start_year=2000, end_year=2021, path='C:/', states=['CA', 'NV', 'OR', 'AZ'], connections = 5):
	check_exists(path)
	get_isd_history(path)

	pool = mp.Pool(connections) # max concurrent connections is 5 -TL
	for _ in tqdm(pool.imap_unordered(download_year, [(year, path, states) for year in range(start_year, end_year)]), total = len(range(start_year, end_year)), desc= 'Total Progress'):
		pass
	pool.close()



'''
arg note for the below:
cores: the number of cores while converting to csv, the process is memory intesive
depending on the amount of states downloaded recomend 1 core with 5+ states and 16gb of memory.

files:
	large: a csv containing the data field and split positional columns
	small: a csv containing only the split position columns
	both: two csvs created small and large.
'''

def mp_convert_csv(start_year, end_year, path = 'C:/', cores = 1, files = 'both'): 

	######################
	#need to add memory management -TL
	######################
	check_exists(path)
	pool = mp.Pool(cores) # set to one becasue 16 gb not enough for more cores -TL
	for _ in tqdm(pool.imap_unordered(convert_to_csv, [(year, path, files) for year in range(start_year, end_year)]), desc= 'Total Progress'):
		pass
	pool.close()



