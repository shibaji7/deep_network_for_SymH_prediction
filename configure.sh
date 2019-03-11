module purge
module load Anaconda
conda create -n Deep python=2.7
source activate Deep
conda list --export > requirements.txt
while read requirement; do conda install --yes $requirement; done < requirements.txt
conda install pytables
