#!/bin/bash
#load in tensorflow for first time while doing other tasks
python3.9 -c "import tensorflow as tf" & 
yum -y update
yum -y install htop

#mount fsx for lustre
mkdir /home/ec2-user/fsx
amazon-linux-extras install -y lustre2.10
mount -t lustre -o noatime,flock fs-057bf8371d0feb376.fsx.us-east-1.amazonaws.com@tcp:/waxq3bev /home/ec2-user/fsx
chmod go+rw /home/ec2-user/fsx
