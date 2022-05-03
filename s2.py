import random
import string
import os
import glob
from sqlalchemy.pool import NullPool
import numpy as np
from pathlib import Path
import pandas as pd
import platform
import time
from random import randrange
import multiprocessing as mp
import subprocess
from sqlalchemy import create_engine, text
import rioxarray as xr
import boto3
import h5py
import shutil



# need to better clean/ organize tihs code


# Class for sql related events
class sql:
	def __init__(self, mode, user, password, endpoint, schema, table, instance_id, num_processes, update=True):
		self.mode = mode
		self.user = user
		self.password = password
		self.endpoint = endpoint
		self.schema = schema
		self.table = table
		self.instance_id = instance_id
		self.num_processes = num_processes
		self.update = update

	# connnects to sql database and gets granule id and url of item to process

	def get_urls(self):
		engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.endpoint}/{self.schema}", poolclass=NullPool)
		temp_table = ( ''.join(random.choice(string.ascii_lowercase) for i in range(15))) 
		
		connection = engine.raw_connection()
		try:
			cursor_obj = connection.cursor()
			cursor_obj.execute(f'CREATE TEMPORARY TABLE {temp_table} SELECT {self.table}.index FROM {self.schema}.{self.table} WHERE IN_PROGRESS = 0 AND PROCESSED = 0 ORDER BY RAND() LIMIT {self.num_processes}')
			if self.update == True:
				cursor_obj.execute(f'UPDATE {self.schema}.{self.table} SET IN_PROGRESS = 1, INSTANCE_ID = "{self.instance_id}" WHERE {self.schema}.{self.table}.index IN (SELECT {temp_table}.index FROM {temp_table})')
			cursor_obj.execute(f'SELECT BASE_URL, GRANULE_ID FROM {self.schema}.{self.table} WHERE {self.schema}.{self.table}.index IN (SELECT {temp_table}.index FROM {temp_table})')
			results = cursor_obj.fetchall()
			cursor_obj.execute(f'DROP TABLE {temp_table}')
			cursor_obj.close()
		finally:
			connection.close()

		self.results = results
		
		return self.results

# gets a list of split graules
	def get_split_granules(self):
		engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.endpoint}/{self.schema}", poolclass=NullPool)
		temp_table = ( ''.join(random.choice(string.ascii_lowercase) for i in range(15))) 
		
		connection = engine.raw_connection()
		try:
			cursor_obj = connection.cursor()
			cursor_obj.execute(f'CREATE TEMPORARY TABLE {temp_table} SELECT {self.table}.index FROM {self.schema}.{self.table} WHERE IN_PROGRESS_2 = 0 AND PROCESSED_2 = 0 ORDER BY RAND() LIMIT {self.num_processes}')
			if self.update == True:
				cursor_obj.execute(f'UPDATE {self.schema}.{self.table} SET IN_PROGRESS_2 = 1 WHERE {self.schema}.{self.table}.index IN (SELECT {temp_table}.index FROM {temp_table})')
			cursor_obj.execute(f'SELECT {self.table}.index, GRANULE_ID FROM {self.schema}.{self.table} WHERE {self.schema}.{self.table}.index IN (SELECT {temp_table}.index FROM {temp_table})')
			results = cursor_obj.fetchall()
			cursor_obj.execute(f'DROP TABLE {temp_table}')
			cursor_obj.close()
		finally:
			connection.close()

		self.results = results
		
		return self.results

	# gets the next tile in the sequnce, the sensing time, and the MGRS to be added as an atribute to the hdf5 file
	def get_metadata(self, granule_id):

		engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.endpoint}/{self.schema}", poolclass=NullPool)
		connection = engine.raw_connection()
		try:
			cursor_obj = connection.cursor()
			cursor_obj.execute(f'SELECT SENSING_TIME, NEXT_TILE, MGRS_TILE FROM {self.schema}.{self.table} WHERE {self.table}.GRANULE_ID = "{granule_id}"')
			results = cursor_obj.fetchall()
			cursor_obj.close()
		finally:
			connection.close()	

		self.sensing_time = str(results[0][0]) # why did I make this a string (I think it came is at dt object)
		self.next_tile = results[0][1]
		self.mgrs = results[0][2]

		return self.sensing_time, self.next_tile, self.mgrs

# simply gets the weather as seen in the sl code
	def get_weather(self):
		engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.endpoint}/{self.schema}", poolclass=NullPool)
		connection = engine.raw_connection()
		try:
			cursor_obj = connection.cursor()
			cursor_obj.execute(f'''SELECT WIND_ANGLE, WIND_SPEED, AIR_TEMP, ATM_PRESSURE 
									FROM dev.weather 
									WHERE MGRS = "{self.mgrs}" 
									AND DATE = DATE("{self.sensing_time}") 
									AND TIME BETWEEN "18:00" 
									AND "23:59"''')
			results = cursor_obj.fetchall()
			cursor_obj.close()
		finally:
			connection.close()	

		#turns tuple into array then transposes so that all weather data is together rather than row by row
		# so instead of [[wind_speed, air_temp], [wind_speed, air_temp]]
		# it is [[wind_speed, wind_speed],[air_temp, air_temp]]
		weather = np.asarray(results, dtype = 'float32').transpose() 

		return weather


	# updates the database to add how long L1C / L2A took to process
	def update_status(self, process_time, granule_id):
		stmt = f'UPDATE {self.table} SET PROCESS_TIME = :total_time, PROCESSED = 1 WHERE {self.table}.GRANULE_ID  = :granule_id'

		values = {
			'total_time': process_time,
			'granule_id': granule_id,
		}
		engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.endpoint}/{self.schema}", poolclass=NullPool)
		with engine.begin() as conn:
			conn.execute(text(stmt), values)

	def update_split_status(self, process_time, index):
		stmt = f'UPDATE {self.table} SET PROCESS_TIME_2 = :total_time, PROCESSED_2 = 1 WHERE {self.table}.index  = :index'

		values = {
			'total_time': process_time,
			'index': index,
		}
		engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.endpoint}/{self.schema}", poolclass=NullPool)
		with engine.begin() as conn:
			conn.execute(text(stmt), values)

	def update_split(self, table, granule_id_pos, b12_count, next_tile):
		# stmt = f'UPDATE {self.table} SET PROCESS_TIME = :total_time, PROCESSED = 1 WHERE {self.table}.GRANULE_ID  = :granule_id'
		stmt = f'INSERT INTO {table} VALUES (:granule_id_pos, :b12_count, :next_tile)'

		values = {
			'granule_id_pos': granule_id_pos,
			'b12_count': b12_count,
			'next_tile': next_tile
		}

		engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.endpoint}/{self.schema}", poolclass=NullPool)
		with engine.begin() as conn:
			conn.execute(text(stmt), values)


	def update_split_df(self, table, df):

		engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.endpoint}/{self.schema}", poolclass=NullPool)

		df.to_sql(table, engine, if_exists='append', index = False)


# Class for downloading / opening stuff
class download:
	def __init__(self, path, results=None):
		self.path = path
		self.results = results
		self.platform = platform.system()

	# downloads entire safe folder
	def safe(self):
		with open("paths.txt", 'w') as file:
			for row in self.results:
				file.write(row[0]+'\n')
		if self.platform == 'Windows': # need to specify powershell to run cat command
			os.system(f'powershell.exe cat paths.txt | gsutil -m cp -r -I {self.path}')
		else:
			os.system(f'cat paths.txt | gsutil -m cp -r -I {self.path}')

	# downloads, B04, B8A, and B12 specifically, in future should add functionality to specify
	# resoltion, and more bands like so: L2A(self, result, resolution = 20, bands = ['B04', 'B8A', 'B12'])
	def L2A(self, result):
		#sleep = randrange(5)
		#time.sleep(sleep)
		base_url = result[0]
		granule_id = result[1]

		if self.platform == 'Windows':
			l2a_path = f'{self.path}\{granule_id}'
		else:
			l2a_path = f'{self.path}/{granule_id}'

		Path(l2a_path).mkdir(parents=True, exist_ok=True)

		base_url_granule = base_url+'/GRANULE/'+granule_id

		for type in ['B04', 'B8A', 'B12']:
			# using suprocess becasue gsutil creates a tempfile that other processes cannot open when using multiprocessing
			#print(['gsutil', 'cp', '-r', f'{base_url_granule}/IMG_DATA/R20m/*{type}*', f'{l2a_path}'])
			#print(base_url_granule)
			#print(l2a_path)
			p = subprocess.Popen(['gsutil', 'cp', '-r', f'{base_url_granule}/IMG_DATA/R20m/*{type}*', f'{l2a_path}'], cwd = l2a_path)

			p.wait()
			p.kill()
		
		return l2a_path, granule_id

	def L2A_h5(self, bucket):
		os.system(f'aws s3 cp {bucket}/L2A {self.path}/L2A --recursive')
	
	# gets weather
	def weather(self, years= [2021, 2022], states= ['CA', 'AZ']):
		import pandas as pd

		#download history
		os.system(f'aws s3 cp s3://noaa-isd-pds/isd-history.csv {self.path}/noaa_weather')

		#subset for states that are needed
		df = pd.read_csv(self.path + '/noaa_weather/isd-history.csv', low_memory=False)
		df = df.loc[df['STATE'].isin(states)]

		df['BEGIN'] = pd.to_datetime(df['BEGIN'], format='%Y%m%d')
		df['END'] = pd.to_datetime(df['END'], format='%Y%m%d')


		df = df[df['BEGIN'].dt.year <= min(years)]
		df = df[df['END'].dt.year >= min(years)]

		# Joins USAF and WBAN column, to create a filename column
		df['FILE NAME'] = df['USAF'].astype(str) + df['WBAN'].astype(str) + '.csv'

		files =  df['FILE NAME'].unique().tolist()
		includes = [f"--inclclude {include}" for include in files]
		include_str = ' '.join(includes)


		for year in years:
			for file in files:
				#s3_command = f"aws s3 cp s3://noaa-global-hourly-pds/{year} {self.path}/year --recursive --exclude '*' {include_str}"
				s3_command = f"aws s3 cp s3://noaa-global-hourly-pds/{year}/{file} {self.path}/noaa_weather/{year}/{file}"
				os.system(s3_command)


#Class for processing stuff
class process:
	
	def __init__(self, path, thresh, bucket = None):
		self.path = path
		self.thresh = thresh
		self.B12 = None
		self.NDVI = None
		self.B12_images = None
		self.NDVI_images = None
		self.granule_id = None
		self.bucket = bucket # looks like s3://firedev/L2A
		self.B12_split = None
		self.NDVI_split = None
		self.B12_type = "bool"
		self.platform = platform.system()
		
	#function that runs the sen2cor algorithim to atmospherically correct tiles
	def convert_L1C(self, sen2cor_path):
		# sleep for no more than 2 min
		sleep = randrange(20) + randrange(20) + randrange(20) + randrange(60)
		time.sleep(sleep)
		# make paths for processing
		make_dir_path = glob.glob(f'{self.path}/GRANULE/L1C*/')[0]
		Path(f'{self.path}/HTML').mkdir(parents=True, exist_ok=True)
		Path(f'{self.path}/AUX_DATA').mkdir(parents=True, exist_ok=True)
		Path(f'{make_dir_path}/HTML').mkdir(parents=True, exist_ok=True)
		Path(f'{make_dir_path}/AUX_DATA').mkdir(parents=True, exist_ok=True)

		# linux /home/ec2-user/Sen2Cor-02.05.05-Linux64/bin/L2A_Process
		# windows C:\Sen2Cor-02.05.05-win64\L2A_Process.bat

		os.system(f'{sen2cor_path} --resolution=20 "{self.path}"')
		
		short_path = self.path[-20:]

		if self.platform == 'Windows':
			current_path = self.path.split('\\')[:-1]
			self.path = glob.glob(f'{current_path[0]}\\{current_path[1]}\\*L2A*{short_path}\\GRANULE\\*\\IMG_DATA\\R20m')[0]
		else:
			current_path = self.path.split('/')[:-1]
			self.path = glob.glob(f'{current_path[0]}/{current_path[1]}/*L2A*{short_path}/GRANULE/*/IMG_DATA/R20m')[0]
			

		
	# function that creates the B12 product (binary encodes) for neural network
	def create_bool_B12(self):

		#convert active burn pixels to 1 and everything else to 0
		self.B12[self.B12<self.thresh]=0
		self.B12[self.B12>self.thresh]=1

		#convert to 0,1 boolean to save space, (was uint 16)
		self.B12 = self.B12.astype('bool')

	# function that only opens B12 for later savings to HDF5 file
	def save_B12(self):
		#print(self.path)
		#add type book or not to save HDF% properly
		self.B12_type = 'float32'
		B12_Path = glob.glob(f'{self.path}/*_B12*.jp2')[0]
		self.B12 = xr.open_rasterio(B12_Path)[0].values

		self.B12 = self.B12.astype('float32')

	# function that creates NDVI product for neural network
	def create_NDVI(self):
		L2A_Red_Path = glob.glob(f'{self.path}/*_B04*.jp2')[0]
		L2A_NIR_Path = glob.glob(f'{self.path}/*_B8A*.jp2')[0]	

		Red = xr.open_rasterio(L2A_Red_Path)[0].values
		NIR = xr.open_rasterio(L2A_NIR_Path)[0].values

		# suppress warning reguarding divide by 0
		np.seterr(divide='ignore', invalid='ignore')
		self.NDVI = (NIR.astype(float)-Red.astype(float))/(NIR.astype(float)+Red.astype(float))
		
		# convert nan to 0 (this necessary due to the divide by 0 issue)
		self.NDVI = np.nan_to_num(self.NDVI, nan=0, posinf=0, neginf=0)

		# if no datatype is specified, thihs will be stored as float 64 and appears to double storage usage
		self.NDVI = self.NDVI.astype('float32')

# for processing on HPC and not in AWS
	def open_local_h5(self):

		with h5py.File(f'{self.path}', "r") as f:
			self.B12 = f['B12'][:]
			self.NDVI = f['NDVI'][:]
			self.weather = f['WEATHER'][:]
############### temp method, issues with atm pressure that need to get addressed #############
			try:
				self.wind_angle = np.mean(np.nan_to_num(self.weather[0], nan=0.0, posinf=0.0, neginf=0.0))
			except:
				self.wind_angle = 99999
				pass
			try:
				self.wind_speed = np.mean(np.nan_to_num(self.weather[1], nan=0.0, posinf=0.0, neginf=0.0))
			except:
				self.wind_speed = 99999
				pass
			try:	
				self.air_temp = np.nanmean(self.weather[2])
			except:
				self.air_temp = 99999
				pass
			try:
				self.atm_pressure = np.nanmean(self.weather[3])
			except:
				self.atm_pressure = 99999
				pass

			self.granule_id = f.attrs['granule_id']
			self.sensing_time =f.attrs['sensing_time']
			self.next_tile = f.attrs['next_tile']
			self.mgrs = f.attrs['mgrs']


		

	# function that combines the B12 and NDVI into a one file and saves it locally
	def save_hdf5(self, granule_id, weather, sensing_time, next_tile, mgrs):

		self.granule_id = granule_id

		if self.platform == 'Windows':
			hf = h5py.File(f'{self.path}\{granule_id}.h5', 'w')
		else:
			
######################### fix path #######################################
			#hf = h5py.File(f'{self.path}/{granule_id}.h5', 'w')
			hf = h5py.File(f'/data03/home/tjlogue/tiles/ca/{granule_id}.h5', 'w')

		if self.B12_type == "bool":
			hf.create_dataset('B12', data=self.B12, dtype ='bool',  compression="lzf") # lzf best for low overhead and good results, only for python
		else:
			hf.create_dataset('B12', data=self.B12, dtype ='float32',  compression="lzf")
	
		hf.create_dataset('NDVI', data=self.NDVI, dtype = 'float32', compression="lzf")

		hf.create_dataset('WEATHER', data=weather, dtype = 'float32', compression="lzf")
		
		hf.attrs['granule_id'] = granule_id
		hf.attrs['sensing_time'] = sensing_time
		hf.attrs['next_tile'] = next_tile
		hf.attrs['mgrs'] = mgrs

		hf.close()

	def save_hdf5_split(self, sql_conn, table):
		df = None
		for pos, B12 in enumerate(self.B12_split):

			b12_count = B12.sum()
			#print(b12_count)		

			#if self.platform == 'Windows':
			#	hf = h5py.File(f'{self.path}\{self.granule_id}.h5', 'w')
			#else:

######################### fix path #######################################
			hf = h5py.File(f'/data03/home/tjlogue/split/ca/{self.granule_id}_{pos}.h5', 'w')


			hf.create_dataset('B12', data=B12, dtype ='bool',  compression="lzf") # lzf best for low overhead and good results, only for python
			hf.create_dataset('NDVI', data=self.NDVI_split[pos], dtype = 'float32', compression="lzf")


			hf.attrs['wind_angle'] = self.wind_angle
			hf.attrs['wind_speed'] = self.wind_speed
			hf.attrs['air_temp'] = self.air_temp
			hf.attrs['atm_pressure'] = self.atm_pressure

			hf.attrs['b12_count'] = b12_count

			
			hf.attrs['granule_id'] = self.granule_id
			hf.attrs['sensing_time'] = self.sensing_time
			hf.attrs['next_tile'] = self.next_tile
			hf.attrs['mgrs'] = self.mgrs

			hf.close()

			#print(f'save time: {ef-sf}')


			#sql_conn.update_split(table, f'{self.granule_id}_{pos}', b12_count, self.next_tile)
			# using df is faster than individial inserts 1 second in total vs 1 second per tiles = 25 seconds
			if df is not None:
				df1 = pd.DataFrame([[f'{self.granule_id}_{pos}', b12_count, self.next_tile]],
										columns = ['granule_id_pos', 'b12_count', 'next_tile' ])
				df = pd.concat([df, df1])
									
			else:
				df = pd.DataFrame([[f'{self.granule_id}_{pos}', b12_count, self.next_tile]],
										columns = ['granule_id_pos', 'b12_count', 'next_tile' ])
			#print(df.head())
		sql_conn.update_split_df(table, df)

		#print(f'Insert time: {end-start}')

	# function for viewing tiles after they are processed and prepared for nueral network
	def view(self, item="NDVI"): #view B12, NDVI, NDVI_split, B12_split
		if item == "NDVI":
			return self.NDVI
		elif item == "B12":
			return self.B12
		elif item == "NDVI_split":
			return self.NDVI_split
		if item == "B12_split":
			return self.B12_split

	#function that sends locally saved .h5 files to s3 for later use
	def to_s3(self):		
			#send to bucket
			os.system(f'aws s3 cp "{self.path}/{self.granule_id}.h5" {self.bucket}/{self.granule_id}.h5')

	def py_to_s3(self, key_id = 'key_id', key= 'key', token = 'token'):		

	#send to bucket using python only, needed for Cal Poyly HPC since we cannot install S3
		session = boto3.Session()
		s3 = session.resource('s3')
		
		s3.Bucket(self.bucket).upload_file(f'{self.path}/{self.granule_id}.h5', f'tiles/{self.granule_id}.h5')

	# fucntion that splits tiles into 4 equal sizes
	def split_tiles(self):

		# will keep at 25 images for now
		M = self.B12.shape[0]//5
		N = self.B12.shape[1]//5
		self.B12_split = [self.B12[x:x+M,y:y+N] for x in range(0,self.B12.shape[0],M) for y in range(0,self.B12.shape[1],N)]

		M = self.NDVI.shape[0]//5
		N = self.NDVI.shape[1]//5
		self.NDVI_split = [self.NDVI[x:x+M,y:y+N] for x in range(0,self.NDVI.shape[0],M) for y in range(0,self.NDVI.shape[1],N)]



# process single .safe folder
def process_L1C(args):
	path, thresh, bucket, sen2cor_path, sql_conn = args
	process_start = time.time()
	if platform.system() == "Windows":
		split_path = glob.glob(f'{path}\GRANULE\*')[0].split('\\')
	else:
		split_path = glob.glob(f'{path}/GRANULE/*')[0].split('/')
	granule_id = split_path[-1] 
	p = process(path, thresh, bucket)
	p.convert_L1C(sen2cor_path)
	p.create_bool_B12()
	p.create_NDVI()
	sensing_time, next_tile, mgrs = sql_conn.get_metadata(granule_id)
	weather = sql_conn.get_weather() # need to calculate average?
	p.save_hdf5(granule_id, weather, sensing_time, next_tile, mgrs)
	p.to_s3()
	process_end = time.time()
	total_time = round((process_end - process_start) /60, 2)
	sql_conn.update_status(total_time, granule_id)

# download and process an L2A product
def process_L2A(args):
	sleep = randrange(10)
	time.sleep(sleep)
	result, path, thresh, bucket, sql_conn = args
	process_start = time.time()
	l2a_path, granule_id = download(path).L2A(result)
	p = process(l2a_path, thresh, bucket)
	p.save_B12()
	p.create_NDVI()
	sensing_time, next_tile, mgrs = sql_conn.get_metadata(granule_id)
	weather = sql_conn.get_weather()
	p.save_hdf5(granule_id, weather, sensing_time, next_tile, mgrs)
	#p.py_to_s3(key, key_id)
	process_end = time.time()
	total_time = round((process_end - process_start) /60, 2)
	sql_conn.update_status(total_time, granule_id)
	shutil.rmtree(l2a_path)

#split L2A
def split_L2A(args):
	sleep = randrange(5)
	time.sleep(sleep)
	result, path, thresh, sql_conn, = args
	index = result[0]
	granule_id = result[1]
	print(granule_id)
	process_start = time.time()
	l2a_path  = f'{path}/{granule_id}.h5'
	p = process(l2a_path, thresh)
	p.open_local_h5()
	p.create_bool_B12()
	p.split_tiles()

	p.save_hdf5_split(sql_conn, table = 'prod_l2a_ca_split')

	#p.py_to_s3(key, key_id)
	process_end = time.time()
	total_time = round((process_end - process_start) /60, 2)
	sql_conn.update_split_status(total_time, index)






# function to multiprocess L1C
def mp_process_L1C(path, thresh, bucket, sen2cor_path, sql_conn, num_cores):
	# download files to happen outside this function.
	if platform.system() == 'Windows':
		process_paths = glob.glob(f'{path}\\*.SAFE')
	else:
		process_paths = glob.glob(f'{path}/*.SAFE')
	pool = mp.Pool(num_cores)
	stuff_to_pass = [thresh, bucket, sen2cor_path, sql_conn]
	pool.map(process_L1C, [(path, *stuff_to_pass) for path in process_paths])
	pool.close()


# function to multiprocess L2A
def mp_process_L2A(results, path, thresh, bucket, sql_conn, num_cores):
	# get results to happen outside this function.
	pool = mp.Pool(num_cores)
	stuff_to_pass = [path, thresh, bucket, sql_conn]
	pool.map(process_L2A, [(result, *stuff_to_pass) for result in results])
	pool.close()

# function to multiprocess split L2A
def mp_process_split_L2A(results, path, thresh, sql_conn, num_cores):
	# get results to happen outside this function.
	pool = mp.Pool(num_cores)
	stuff_to_pass = [path, thresh, sql_conn]
	pool.map(split_L2A, [(result, *stuff_to_pass) for result in results])
	pool.close()




############# need to incorperate this somewhere ###############
def copy(granule):
	gran_file = f'{granule}.h5'
	#next_file = f'{next}.h5'

	print(gran_file)


	shutil.copy(f'/data03/home/tjlogue/split/ca/{gran_file}', f'/data03/home/tjlogue/next_tile/ca/{gran_file}')




def append_next_tile(data):
	try:
		granule, next_tile = data


		gran_file = f'/data03/home/tjlogue/split/ca/{granule}.h5'
		next_file = f'/data03/home/tjlogue/split/ca/{next_tile}.h5'

		gran_dest = f'/data03/home/tjlogue/next_tile/ca/{granule}.h5'

		shutil.copy(gran_file, gran_dest)

		gran_h5 = h5py.File(gran_dest, 'a')
		next_h5 = h5py.File(next_file, 'r')

		NEXT =  next_h5['B12'][:]

		gran_h5.create_dataset('NEXT', data=NEXT, dtype ='bool',  compression="lzf")

		gran_h5.close()
	except:
		#print(granule)
		pass




files = glob.glob('/data03/home/tjlogue/next_tile/ca/*')


def upload_tile(data):
		granule_id = data.split('/')[-1]
		
		session = boto3.Session()
		s3 = session.resource('s3')
		
		s3.Bucket('fire-prod').upload_file(data, f'nn/ca/{granule_id}')


