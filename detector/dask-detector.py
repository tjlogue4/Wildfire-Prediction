from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
import random
import string
import numpy as np
import time
import platform
import os
import subprocess
from pathlib import Path
import glob
import multiprocessing as mp
import xarray
from random import randrange
import gc

from dask_jobqueue import SLURMCluster
from dask.distributed import Client
from dask import compute, delayed

cluster = SLURMCluster(cores=30,
                    processes=30,
                    memory="90GB",
                    queue ='compute'
)
cluster.scale(jobs=10)
#cluster.adapt(maximum_jobs=15) #doesn't seem to work


client = Client(cluster)

DBMODE= 'dev'
USER = 'hpc-user'
PASSWORD = '-'
DEV = '-.us-east-1.rds.amazonaws.com'
PROD = '-'
LEVEL = 'L2A'
BUCKET = '-'

ENDPOINT = DEV
SCHEMA = 'dev'
if LEVEL == "L1C":
	TABLE = 'detect_l1c'
else:
	TABLE = 'detect_l2a'

INSTANCE_ID = f'cpp-hpc-{platform.node()}'

#NUM_PROCESSES = 30 

NUM_RECORDS = 1000
os.environ["OMP_NUM_THREADS"] = "1" #force xarray to use single core

PATH = '/dev/shm' #basically a ramdisk
#PATH = '/tmp'

#connect to sql
def get_urls():
	engine = create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{ENDPOINT}/{SCHEMA}", poolclass=NullPool)
	temp_table = ( ''.join(random.choice(string.ascii_lowercase) for i in range(15))) 
	
	connection = engine.raw_connection()
	try:
		cursor_obj = connection.cursor()
		cursor_obj.execute(f'CREATE TEMPORARY TABLE {temp_table} SELECT {TABLE}.index FROM {SCHEMA}.{TABLE} WHERE IN_PROGRESS = 0 AND PROCESSED = 0 ORDER BY RAND() LIMIT {NUM_RECORDS}')
		cursor_obj.execute(f'UPDATE {SCHEMA}.{TABLE} SET IN_PROGRESS = 1, INSTANCE_ID = "{INSTANCE_ID}" WHERE {SCHEMA}.{TABLE}.index IN (SELECT {temp_table}.index FROM {temp_table})')
		cursor_obj.execute(f'SELECT {TABLE}.index, BASE_URL, GRANULE_ID FROM {SCHEMA}.{TABLE} WHERE {SCHEMA}.{TABLE}.index IN (SELECT {temp_table}.index FROM {temp_table})')
		results = cursor_obj.fetchall()
		cursor_obj.execute(f'DROP TABLE {temp_table}')
		cursor_obj.close()
	finally:
		connection.close()

	
	return results


def update_status(index, process_time, granule_id, t08, t09, t10, t11, t12, t13, t14, t15):
	stmt = f'UPDATE {TABLE} SET PROCESS_TIME = :total_time, PROCESSED = 1, T08 = :T08, T09= :T09, T10 = :T10, T11 = :T11, T12 = :T12, T13 = :T13, T14 = :T14, T15 = :T15 WHERE {TABLE}.index  = :index'

	values = {
		'total_time': process_time,
		'index': index,
		'T08': t08,
		'T09': t09,
		'T10': t10,
		'T11': t11,
		'T12': t12,
		'T13': t13,
		'T14': t14,
		'T15': t15
	}
	engine = create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{ENDPOINT}/{SCHEMA}", poolclass=NullPool)
	with engine.begin() as conn:
		conn.execute(text(stmt), values)
		print(f'Insert {granule_id}')	


def L2A(path, result):
	index = result[0]
	base_url = result[1]
	granule_id = result[2]

	l2a_path = f'{path}/{granule_id}'

	Path(l2a_path).mkdir(parents=True, exist_ok=True)
	

	base_url_granule = base_url+'/GRANULE/'+granule_id


	# using suprocess becasue gsutil creates a tempfile that other processes cannot open when using multiprocessing
	p = subprocess.Popen(['gsutil', 'cp', '-r', f'{base_url_granule}/IMG_DATA/R20m/*B12*', f'{l2a_path}'], cwd = l2a_path)

	p.wait()
	p.kill() #should prvent remaining zombie/ sleeping processes after code is complete
	
	return index, l2a_path, granule_id


def test_b12(l2a_path):
	B12_Path = glob.glob(f'{l2a_path}/*_B12*.jp2')[0]
	B12 = xarray.open_rasterio(B12_Path)[0].values

	# convert active burn pixels to 1 and everything else to 0
    # the idea here is to see what type of B12 values are captures during a wildfire
	t08 = np.where(B12 >= 8000, 1, 0).sum()
	t09 = np.where(B12 >= 9000, 1, 0).sum()
	t10 = np.where(B12 >= 10000, 1, 0).sum()
	t11 = np.where(B12 >= 11000, 1, 0).sum()
	t12 = np.where(B12 >= 12000, 1, 0).sum()
	t13 = np.where(B12 >= 13000, 1, 0).sum()
	t14 = np.where(B12 >= 14000, 1, 0).sum()
	t15 = np.where(B12 >= 15000, 1, 0).sum()

	os.system(f'rm {l2a_path} -r')
 

	return t08, t09, t10, t11, t12, t13, t14, t15

def detect_L2A(result):
	try: # add try/ except to prevent dask from failing all nodes if there is an issue with one
		process_start = time.time()
		index, l2a_path, granule_id = L2A(PATH, result)
		t08, t09, t10, t11, t12, t13, t14, t15 = test_b12(l2a_path)
		process_end = time.time()
		total_time = round((process_end - process_start) /60, 2)
		update_status(index, total_time, granule_id, t08, t09, t10, t11, t12, t13, t14, t15)
	except:
		pass

	

while True:
	results = get_urls()
	if results == (): # () is returned when no data is selected#
		break
	else:
		output = []
		for result in results:
            # need to better convet this to fully utilize dask, see notebook example
			a = delayed(detect_L2A)(result)
			output.append(a)
		compute(*output)


