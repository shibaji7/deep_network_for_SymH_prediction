#!/bin/bash
#PBS -l nodes=1:ppn=8
#PBS -l walltime=20:00:00
#PBS -q normal_q
#PBS -A solarflare
#PBS -W group_list=newriver
#PBS -M shibaji7@vt.edu
#PBS -m bea
python main.py
exit;
