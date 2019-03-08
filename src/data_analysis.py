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

import numpy as np
import pandas as pd
import datetime as dt
import glob

from startup import *


sns.set(color_codes=True)
plt.style.use("fivethirtyeight-custom")


fontdict = {"family": "serif", "color":  "k", "weight": "normal", "size": 12}
fontlegend = {"family": "serif", "weight": "normal", "size": 7}

##############################################################################################
## Plot and store all SYM-H values against UT(Hours) and Greenwich mean siderial time(hours)
## Note that, this is a time versus month scatter plot for all years
##############################################################################################
def scatter_plot_symh_ut_gmst(years=[], values="SYM-H"):
    dirc = BASE_LOCATION + "premodel_da_figures"
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
        O.DATE = pd.to_datetime(O.DATE)
        O["DAY"] = O.DATE.apply(lambda x: dt.datetime.strptime(x.strftime("%Y-%m-%d"),"%Y-%m-%d"))
        O["TIMESTAMP"] = O.DATE.apply(lambda x: x.hour*60 + x.minute)
        P = O.pivot(index="TIMESTAMP",columns="DAY",values=values)
        xTime = P.columns.tolist()
        yTime = P.index.tolist()
        xxTime, yyTime = np.meshgrid(xTime, yTime, sparse=False, indexing="ij")
        X = P.values.T
        
        fig, ax = plt.subplots(figsize=(7,3),nrows=1,ncols=1,dpi=100)
        fig.subplots_adjust(hspace=0.2,wspace=0.1)
        
        cmap = matplotlib.cm.get_cmap("jet")
        cmap.set_bad('white',1.)
        ax.xaxis.set_major_formatter(fmt)
        cax = ax.pcolormesh(xxTime, yyTime, X, cmap=cmap,vmax=50,vmin=-50)
        ax.set_xlabel("Months",fontdict=fontdict)        
        #ax.set_xticklabels(["Jan","Feb","Mar","Apr","May","Jun","Jul","Sep","Oct","Nov","Dec"],fontdict=fontdict,ha="center")
        ax.set_ylabel("UT",fontdict=fontdict)
        ax.set_yticks([])
        ax.set_yticks(np.arange(0,24,3)*60)
        ax.set_yticklabels(np.arange(0,24,3),fontdict=fontdict)
        cbar = fig.colorbar(cax, ax=ax,shrink=.5,ticks=[-50,0,50])
        cbar.ax.set_yticklabels(["< -50", "0", "> 50"])
        cbar.ax.set_title("Sym-H",fontdict=fontdict)
        fontdict["size"] = 8
        ax.text(1.15,0.95,r"$Sym-H_{max}=%.2f$"%np.max(X)+"\n"+r"$Sym-H_{min}=%.2f$"%np.min(X),horizontalalignment="center",
                    verticalalignment="center", transform=ax.transAxes,
                    fontdict=fontdict)

        fig.savefig("%s/%s_%d.png"%(dirc, values, year),bbox_inches='tight')
        plt.close()
        break
        pass
    return
 
if __name__ == "__main__":
    scatter_plot_symh_ut_gmst()
    pass
