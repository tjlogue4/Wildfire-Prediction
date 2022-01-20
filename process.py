import s2
import os
import multiprocessing as mp
from random import randrange
import time


#sleep for no more than a min
sleep = randrange(20) + randrange(20) + randrange(20)
time.sleep(sleep)

DBMODE= os.environ.get('DBMODE')
USER = os.environ.get('DBUSERNAME')
PASSWORD = os.environ.get('DBPASSWORD')
DEV = os.environ.get('DBDEV')
PROD = os.environ.get('DBPROD')
GSACCESS = os.environ.get('GSACCESS')
GSKEY = os.environ.get('GSKEY')
LEVEL = os.environ.get('LEVEL')
BUCKET = os.environ.get('BUCKET')


#assuming we are using 32 cores m5d.8xlarge us-east-1d
NUM_PROCESSES = mp.cpu_count()-2 #save some for overhead

THRESH = 12000 #B12 Threshold, may incorperate this into bash script for easier fine tuning

# AWS instance ID for tracking purposes
INSTANCE_ID = os.popen('cat /sys/devices/virtual/dmi/id/board_asset_tag').read().strip('\n')[2:]

#processing path
PATH = '/home/ec2-user/nvme'

sen2cor_path = "/home/ec2-user/Sen2Cor-02.05.05-Linux64/bin/L2A_Process"

#determine which database to use, production or development
if DBMODE == 'prod':
	ENDPOINT = PROD
	SCHEMA = 'prod'
	if LEVEL == "L1C":
		TABLE = 'prod_l1c'
	else:
		TABLE = 'prod_l2a'
else:
	ENDPOINT = DEV
	SCHEMA = 'dev'
	if LEVEL == "L1C":
		TABLE = 'dev_l1c'
	else:
		TABLE = 'dev_l2a'
	



# only needed on linux machines, this is for gsutil auth, no luck using bash for this
os.system(f"yes $'{GSACCESS}\n{GSKEY}' | gsutil config -a")




if __name__ ==  '__main__':
	if LEVEL == "L1C":
		while True:
			sql_conn = s2.sql(user = USER, password = PASSWORD, endpoint =ENDPOINT, schema = SCHEMA, table = TABLE, instance_id = INSTANCE_ID, num_processes= NUM_PROCESSES, update = True)
			results = sql_conn.get_urls()
			if results == (): # () is returned when no data is selected
				break
			else:
				s2.download(PATH, results).safe()
				s2.mp_process_L1C(path= PATH, thresh = THRESH, bucket = BUCKET, sen2cor_path = sen2cor_path, sql_conn=sql_conn, num_cores=NUM_PROCESSES)

	else:
		while True:
			sql_conn = s2.sql(user = USER, password = PASSWORD, endpoint =ENDPOINT, schema = SCHEMA, table = TABLE, instance_id = INSTANCE_ID, num_processes= NUM_PROCESSES, update = True)
			results = sql_conn.get_urls()
			if results == (): # () is returned when no data is selected
				break
			else:
				s2.mp_process_L2A(results, path= PATH, thresh = THRESH, bucket = BUCKET, sql_conn=sql_conn, num_cores=NUM_PROCESSES)
