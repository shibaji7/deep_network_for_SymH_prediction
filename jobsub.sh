#!/bin/bash
#PBS -l nodes=1:ppn=10
#PBS -l walltime=10:00:00
#PBS -l procs=120
#PBS -q normal_q
#PBS -A solarflare
#PBS -W group_list=newriver
#PBS -M shibaji7@vt.edu
#PBS -m bea
cd $PBS_O_WORKDIR
module purge
module load Anaconda
source activate Deep
export PYTHONUSERBASE=/home/shibaji7/deep_network_for_SymH_prediction/
python main.py
exit;
