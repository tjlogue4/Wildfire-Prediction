#!/bin/bash


mkdir /home/ec2-user/nvme
mkdir /home/ec2-user/google
mkdir /home/ec2-user/aws
mkdir /home/ec2-user/python
mkdir /home/ec2-user/sen2cor

# this might not be necessary
chmod 777 /home/ec2-user/temp
chmod 777 /home/ec2-user/google
chmod 777 /home/ec2-user/aws
chmod 777 /home/ec2-user/python
chmod 777 /home/ec2-user/sen2cor


# get sen2cor program
curl "http://step.esa.int/thirdparties/sen2cor/2.5.5/Sen2Cor-02.05.05-Linux64.run" --output /home/ec2-user/sen2cor/Sen2Cor-02.05.05-Linux64.run
chmod 777 /home/ec2-user/sen2cor/Sen2Cor-02.05.05-Linux64.run

# install sen2cor as ec2-user rather than as root, this will prevent sen2cor from installing in the root dir
su - ec2-user -c "/home/ec2-user/sen2cor/Sen2Cor-02.05.05-Linux64.run"

#install aws cli
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" --output "/home/ec2-user/aws/awscliv2.zip"
unzip /home/ec2-user/aws/awscliv2.zip
/home/ec2-user/aws/install

#get the process.py file and s2.py file
aws s3 cp "s3://bucket/process.py" "/home/ec2-user/python/" # --recursive
aws s3 cp "s3://bucket/s2.py" "/home/ec2-user/python/"

yum -y install htop

#install all packages needed for processing
pip3 install pandas sqlalchemy pymysql cryptography xarray rasterio h5py


# set env variables
echo export DBMODE="dev" >> /etc/profile
echo export LEVEL="L1C" >> /etc/profile # not needed

echo export DBDEV="[DEV DATABASE].us-east-1.rds.amazonaws.com" >> /etc/profile
echo export DBPROD="[PROD DATABASE].us-east-1.rds.amazonaws.com" >> /etc/profile

echo export DBUSERNAME="[USER NAME]" >> /etc/profile
echo export DBPASSWORD="[PASSWORD]" >> /etc/profile
echo export BUCKET="[S3 BUCKET]" >> /etc/profile #  bucket where processed data will be sent


# google cloud hmac keys
echo export GSACCESS="[HMAC ACCESS ID]" >> /etc/profile
echo export GSKEY="[HMAC SECRET]" >> /etc/profile

# get gsutil this kinda works, still have run it again in python
curl "https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-367.0.0-linux-x86_64.tar.gz" --output /home/ec2-user/google/google-cloud-sdk-367.0.0-linux-x86_64.tar.gz
tar -xf /home/ec2-user/google/google-cloud-sdk-367.0.0-linux-x86_64.tar.gz --directory /home/ec2-user/google/
echo yes $'N\nY\n ' | /home/ec2-user/google/google-cloud-sdk/install.sh

# some more stuff to make gsutil work
echo "if [ -f '/home/ec2-user/google/google-cloud-sdk/path.bash.inc' ]; then . '/home/ec2-user/google/google-cloud-sdk/path.bash.inc'; fi" >> /home/ec2-user/.bashrc
echo "if [ -f '/home/ec2-user/google/google-cloud-sdk/completion.bash.inc' ]; then . '/home/ec2-user/google/google-cloud-sdk/completion.bash.inc'; fi" >> /home/ec2-user/.bashrc



# create software raid array to take advantage of ec2 with nvme storage
# most of these steps are from https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/raid-config.html
echo "mdadm --create --verbose /dev/md0 --level=0 --name=MY_RAID --raid-devices=2 /dev/nvme1n1 /dev/nvme2n1" >> /etc/rc.local
echo "sleep 20s" >> /etc/rc.local # give time for raid array to initialize
echo "mkfs.ext4 -L MY_RAID /dev/md0" >> /etc/rc.local
echo "dracut -H -f /boot/initramfs-$(uname -r).img $(uname -r)" >> /etc/rc.local
echo "sudo mount LABEL=MY_RAID /home/ec2-user/nvme" >> /etc/rc.local
echo "mkdir /home/ec2-user/nvme/processed" >> /etc/rc.local
echo "chmod -R 777 /home/ec2-user/nvme" >> /etc/rc.local
echo "chmod -R 777 /home/ec2-user/python" >> /etc/rc.local

# run python file on next restart
echo "su - ec2-user -c '/usr/bin/python3 /home/ec2-user/python/process.py'" >> /etc/rc.local

sudo chmod +x /etc/rc.d/rc.local

reboot now







