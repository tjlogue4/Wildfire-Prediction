from sqlalchemy import create_engine, text
import random
import string
import os
import glob
import xarray as xr
import numpy as np
from pathlib import Path
import h5py
import platform
import time
from random import randrange
import multiprocessing as mp
import subprocess




# Class for sql related events
class sql:
	def __init__(self, level, mode, user, password, endpoint, schema, table, instance_id, num_processes, update=True):
		self.purpose = level
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
		engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.endpoint}/{self.schema}")
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


	def get_metadata(self, granule_id):

		engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.endpoint}/{self.schema}")
		connection = engine.raw_connection()
		try:
			cursor_obj = connection.cursor()
			cursor_obj.execute(f'SELECT SENSING_TIME, UNIQUE_ID, NEXT_TILE, MGRS FROM {self.schema}.{self.table} WHERE {self.table}.GRANULE_ID = "{granule_id}"')
			results = cursor_obj.fetchall()
			cursor_obj.close()
		finally:
			connection.close()	

		self.sensing_time = str(results[0][0])
		self.unique_id = results[0][1]
		self.next_tile = results[0][2]
		self.mgrs = results[0][3]

		return self.sensing_time, self.unique_id, self.next_tile, self.mgrs


	def get_weather(self):
		engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.endpoint}/{self.schema}")
		connection = engine.raw_connection()
		try:
			cursor_obj = connection.cursor()
			cursor_obj.execute(f'''SELECT WIND_ANGLE, WIND_SPEED, AIR_TEMP, ATM_PRESSURE 
									FROM dev.weather 
									WHERE MGRS = "{self.mgrs}" 
									AND DATE = "{self.sensing_time}" 
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


	#updates the database to add how L1C/ l2A took to process
	def update_status(self, process_time, granule_id):
		stmt = f'UPDATE {self.table} SET PROCESS_TIME = :total_time, PROCESSED = 1 WHERE {self.table}.GRANULE_ID  = :granule_id'

		values = {
			'total_time': process_time,
			'granule_id': granule_id,
		}
		engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.endpoint}/{self.schema}")
		with engine.begin() as conn:
			conn.execute(text(stmt), values)

# Class for downloading stuff
class download:
	def __init__(self, path):
		self.path = path
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
			p = subprocess.Popen(['gsutil', 'cp', '-r', f'{base_url_granule}/IMG_DATA/R20m/*{type}*', f'{l2a_path}'], cwd = l2a_path, shell = True)

			p.wait()
		
		return l2a_path, granule_id

	def L2A_h5(self, bucket):
		os.system(f'aws s3 cp {bucket}/L2A {self.path}/L2A --recursive')
	
	# may be omiited later if decide to use SQL1
	def weather_h5(self, bucket):
		os.system(f'aws s3 cp {bucket}/weather {self.path}/weather --recursive')
	


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
	def create_B12(self):
		B12_Path = glob.glob(f'{self.path}/*_B12*.jp2')[0]
		self.B12 = xr.open_rasterio(B12_Path)[0].values

		#convert active burn pixels to 1 and everything else to 0
		self.B12[self.B12<self.thresh]=0
		self.B12[self.B12>self.thresh]=1

		#convert to 0,1 boolean to save space, (was uint 16)
		self.B12 = self.B12.astype('bool')

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

	# function that combines the B12 and NDVI into a one file and saves it locally
	def save_hdf5(self, granule_id, weather, sensing_time, unique_id, next_tile, mgrs):
			self.granule_id = granule_id

			if self.platform == 'Windows':
				hf = h5py.File(f'{self.path}\{granule_id}.h5', 'w')
			else:
				hf = h5py.File(f'{self.path}/{granule_id}.h5', 'w')
			hf.create_dataset('B12', data=self.B12, dtype ='bool',  compression="lzf") # lzf best for low overhead and good results, only for python
			hf.create_dataset('NDVI', data=self.NDVI, dtype = 'float32', compression="lzf")
			hf.create_dataset('WEATHER', data=weather, dtype = 'float32', compression="lzf")

			hf.attrs['granule_id'] = granule_id
			hf.attrs['sensing_time'] = sensing_time
			hf.attrs['unique_id'] = unique_id
			hf.attrs['next_tile'] = next_tile
			hf.attrs['mgrs'] = mgrs

			hf.close()

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


	# fucntion that splits tiles into 4 equal sizes
	def split_tiles(self):

		# will keep at 4 images for now
		M = self.B12.shape[0]//2
		N = self.B12.shape[1]//2
		self.B12_split = [self.B12[x:x+M,y:y+N] for x in range(0,self.B12.shape[0],M) for y in range(0,self.B12.shape[1],N)]

		M = self.NDVI.shape[0]//2
		N = self.NDVI.shape[1]//2
		self.NDVI_split = [self.NDVI[x:x+M,y:y+N] for x in range(0,self.NDVI.shape[0],M) for y in range(0,self.NDVI.shape[1],N)]



# process single .safe folder
def process_L1C(path, thresh, bucket, sen2cor_path, sql_conn):
	process_start = time.time()
	if platform.system() == "Windows":
		split_path = glob.glob(f'{path}\GRANULE\*')[0].split('\\')
	else:
		split_path = glob.glob(f'{path}/GRANULE/*')[0].split('/')
	granule_id = split_path[-1] 
	p = process(path, thresh, bucket)
	p.convert_L1C(sen2cor_path)
	p.create_B12()
	p.create_NDVI()
	sensing_time, unique_id, next_tile, mgrs = sql_conn.get_metadata(granule_id)
	weather = sql_conn.get_weather()
	p.save_hdf5(granule_id, weather, sensing_time, unique_id, next_tile, mgrs)
	p.to_s3()
	process_end = time.time()
	total_time = round((process_end - process_start) /60, 2)
	sql_conn.update_status(total_time, granule_id)

# download and process an L2A product
def process_L2A(args):
	sleep = randrange(5)
	time.sleep(sleep)
	result, path, thresh, bucket, sql_conn = args
	process_start = time.time()
	l2a_path, granule_id = download(path).L2A(result)
	p = process(l2a_path, thresh, bucket)
	p.create_B12()
	p.create_NDVI()
	sensing_time, unique_id, next_tile, mgrs = sql_conn.get_metadata(granule_id)
	weather = sql_conn.get_weather()
	p.save_hdf5(granule_id, weather, sensing_time, unique_id, next_tile, mgrs)
	p.to_s3()
	process_end = time.time()
	total_time = round((process_end - process_start) /60, 2)
	sql_conn.update_status(total_time, granule_id)



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


	
