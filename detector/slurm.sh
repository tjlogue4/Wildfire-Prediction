#!/bin/bash

#SBATCH --job-name=cpu-detect
#SBATCH --nodes=10
#SBATCH --ntasks=10
#SBATCH --mem=90G
#SBATCH --cpus-per-task=32
#SBATCH --partition=compute


srun conda run -n fires3.7 python detector.py