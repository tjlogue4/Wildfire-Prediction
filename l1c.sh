#!/bin/bash

#SBATCH --job-name=L1C_to_L2A
#SBATCH --nodes=10
#SBATCH --ntasks=10
#SBATCH --mem=120G
#SBATCH --cpus-per-task=32
#SBATCH --partition=compute


srun conda run -n fires3.7 python process_l1c.py



