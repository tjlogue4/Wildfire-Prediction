# Predicting Wildfires

This repository was created during my masters program with the goal of using sentinel-2 satalite imagry to create a risk map for a wildfire occuring at a 10m scale based on NDVI. We also attempted to predict a fire occuring near a weather station using historical weather and logistic regression. Although this was a group project, all of the code in this repository was written by me.


## Databases:
Due to the sheer amount of data that needed to be processed I created SQL databases to keep track of the data.
* [Database Builder L1C & L2A.ipynb](https://github.com/tjlogue4/Wildfire-Prediction/blob/main/Database%20Builder%20L1C%20%26%20L2A.ipynb)
* [Database Builder Weather.ipynb](https://github.com/tjlogue4/Wildfire-Prediction/blob/main/Database%20Builder%20Weather.ipynb)

## Exploration:
Some notebooks exploring and visualizing the data.
* [Exploration and Cleanup of Weather Data.ipynb](https://github.com/tjlogue4/Wildfire-Prediction/blob/main/Exploration%20and%20Cleanup%20of%20Weather%20Data.ipynb)
* [Exploration of Sentinel-2.ipynb](https://github.com/tjlogue4/Wildfire-Prediction/blob/main/Exploration%20of%20Sentinel-2.ipynb) or try [Exploration of Sentinel-2.ipynb at nbviewer.org](https://nbviewer.org/github/tjlogue4/Wildfire-Prediction/blob/main/Exploration%20of%20Sentinel-2.ipynb)
* [Logistic Regression.ipynb](https://github.com/tjlogue4/Wildfire-Prediction/blob/main/Logistic%20Regression.ipynb)

## Processing and Deep Learning
General Processing
* [noaa_weather.py](https://github.com/tjlogue4/Wildfire-Prediction/blob/main/noaa_weather.py)
* [process.py](https://github.com/tjlogue4/Wildfire-Prediction/blob/main/process.py)
* [s2.py](https://github.com/tjlogue4/Wildfire-Prediction/blob/main/s2.py)
* [Atmospheric Correction](https://github.com/tjlogue4/Wildfire-Prediction/blob/main/process_l1c.py)

Models and model testing
* [Training SSIM](https://github.com/tjlogue4/Wildfire-Prediction/blob/main/training_ssim.py)
* [Training Binary Cross Entropy](https://github.com/tjlogue4/Wildfire-Prediction/blob/main/training_binary_crossentropy.py)
* [Model Evaluation](https://github.com/tjlogue4/Wildfire-Prediction/blob/main/eval%20models.ipynb)
* [Earth Engine Processing/ Prep](https://github.com/tjlogue4/Wildfire-Prediction/blob/main/process_for_ee.ipynb)


## Bash Files
* [ec2-user-data.sh](https://github.com/tjlogue4/Wildfire-Prediction/blob/main/ec2-user-data.sh)
* [Slurm CPP HPC](https://github.com/tjlogue4/Wildfire-Prediction/blob/main/l1c.sh)
* [Mount FSx for Lustre](https://github.com/tjlogue4/Wildfire-Prediction/blob/main/ec2-user-data-fsx-lustre.sh)