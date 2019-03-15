#!/usr/bin/env python

"""database.py: database is dedicated to build data structures for pre-model data processing for different types of Deep models."""

__author__ = "Chakraborty, S."
__copyright__ = "Copyright 2019, Space@VT"
__credits__ = []
__license__ = "MIT"
__version__ = "1.0."
__maintainer__ = "Chakraborty, S."
__email__ = "shibaji7@vt.edu"
__status__ = "Research"

import numpy as np
import datetime as dt
import pandas as pd
import dask

from startup import *

#########################################################################################
## Method is used to fetch one data point. This method is used mainly for dynamic models
## Note that this method does not proide normalized data points
#########################################################################################
def fetch_data_with_durations(date, window=7., i_dur=3., o_dur=1., filter_ext=5, o_params=["SYM-H", "SYM-H_STD"], 
                                i_params=["Bx", "By_GSE", "Bz_GSE","V", "Vx_GSE", "Vy_GSE", "Vz_GSE", 
                                          "n", "T", "P_DYN", "E", "BETA", "MACH_A"], verbose = False):
    """
    This method fetches data from the file and converted into model readable format

    Input parameters: 
    ----------------
    date <datetime>         - Date time instance for which you need the data matrix
    winodw <float>          - Window of the training data (in days)
    i_dur <float>           - Duration of the training winodw (in hours)
    o_dur <float>           - Duration of the forecasted winodw (in hours)
    filter_ext <int>        - Extension of the filtered files (15 min resample data)
    o_param <str>           - Output parameter name
    i_paramis <list(str)>   - Input parameter name list
    
    Output parameter:
    ----------------
    O <Structure>       - Touple consisting list of date vectors, input matrix and output vectors
    """

    def load_file(year):
        _o = pd.DataFrame()
        file_store = BASE_LOCATION + "{ext}_%d.h5".replace("{ext}",str(filter_ext))
        f_name = file_store%year 
        _x = pd.read_hdf(f_name, mode="r", key="df", parse_dates=True)
        _o = pd.concat([_x,_o])
        if year > 1995:
            prev_year = year - 1
            f_name = file_store%prev_year
            _x = pd.read_hdf(f_name, mode="r", key="df", parse_dates=True)
            _o = pd.concat([_x,_o])
            pass
        return _o

    def fetch_delayed_out_vector(_o, d):
        v = _o[(_o.DATE>d-dt.timedelta(hours=o_dur)) & (_o.DATE<=d)][o_params].values
        return v

    def fetch_delayed_in_matrix(_o, d):
        m = _o[(_o.DATE>d-dt.timedelta(hours=i_dur)) & (_o.DATE<=d)][i_params].values
        return m

    _o = load_file(date.year)
    if verbose: print _o.head()
    dates = [date - dt.timedelta(minutes=i*filter_ext) for i in np.arange(window*24*60/filter_ext)]
    olist = []
    ilist = []
    
    delayed = []
    for d in dates:
        delayed.append(dask.delayed(fetch_delayed_out_vector(_o, d)))
        pass
    olist = dask.compute(*delayed,scheduler=SCHEDULER, num_workers=N_WORKER)
    olist = np.array(olist)
    if verbose: print "Output vector list shape : ",olist.shape

    delayed = []
    for d in dates:
        delayed.append(dask.delayed(fetch_delayed_in_matrix(_o, d)))
        pass
    ilist = dask.compute(*delayed,scheduler=SCHEDULER, num_workers=N_WORKER)
    ilist = np.array(ilist)
    if verbose: print "Output vector list shape : ",ilist.shape
    
    O = (dates, olist, ilist)
    return O


#################################################
## Main method used to test some functionality
#################################################
if __name__ == "__main__":
    isrun = False
    if isrun:
        import datetime as dt
        fetch_data_with_durations(dt.datetime(2001,3,2), verbose = True)
        pass
    pass
