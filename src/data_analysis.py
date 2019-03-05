#!/usr/bin/env python

"""data_analysis.py: data analysis is dedicated for pre-model data analysis and it also consists of plotting methods."""

__author__ = "Chakraborty, S."
__copyright__ = "Copyright 2019, Space@VT"
__credits__ = []
__license__ = "MIT"
__version__ = "1.0."
__maintainer__ = "Chakraborty, S."
__email__ = "shibaji7@vt.edu"
__status__ = "Research"

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns

import pandas as pd
import glob

from startup import *


sns.set_context("poster")
sns.set(color_codes=True)
plt.style.use("fivethirtyeight")


fontdict = {"family": "serif", "color":  "k", "weight": "normal", "size": 12}
fontlegend = {"family": "serif", "weight": "normal", "size": 7}

##############################################################################################
## Plot and store all SYM-H values against UT(Hours) and Greenwich mean siderial time(hours)
## Note that, this is a time versus month scatter plot for all years
##############################################################################################
def scatter_plot_symh_ut_gmst(years=[]):
    if len(years) == 0: years = range(1995,2019)
    fmt = matplotlib.dates.DateFormatter("%b")
    hdf5_base = BASE_LOCATION + "geomag/symh/hdf5/%d*.h5"
    for year in years:
        flist = glob.glob(hdf5_base%year)
        O = pd.DataFrame()
        for fname in flist:
            _o = pd.read_hdf(fname, mode="r", key="df")
            O = pd.concat([O, _o])
            pass
        print O.head()
        fig, axes = plt.subplots(figsize=(4,4),nrows=2,ncols=1,dpi=100)
        fig.subplots_adjust(hspace=0.1,wspace=0.1)
        
        ax = axes[0]
        ax.xaxis.set_major_formatter(fmt)
        
        plt.close()
        break
        pass
    return
 
if __name__ == "__main__":
    scatter_plot_symh_ut_gmst()
    pass
