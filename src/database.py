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

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from imblearn.under_sampling import RandomUnderSampler
from keras.utils import to_categorical

import utils
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


##################################################################################################
## Build dataset for Kp classifier
##################################################################################################
class kp_classifier_dataset(object):
    """
    This class is dedicated to building and rearranging classifier datasource
    """
    
    def load_data(self):
        file_store = BASE_LOCATION + "{ext}_%d.h5".replace("{ext}",str(self.file_ext))
        self.O = pd.DataFrame()
        for year in range(1995,2019):
            fname = file_store%year
            self.O = pd.concat([self.O, pd.read_hdf(fname, mode="r", key="df", parse_dates=True)])
            pass
        _x = utils.to_linear_Kp(delay_unit=1)
        self.O["Kp"] = _x["Kp"]
        self.O["STORM"] = _x["STORM"]
        return
    
    def __init__(self, look_back = 3, file_ext=180):
        self.look_back = look_back
        self.file_ext = file_ext
        self.load_data()
        self.xparams = ["Bx","B_T","THETA","SIN_THETA","V","n","T","P_DYN","BETA","MACH_A","STORM"]
        self.yparam = ["STORM"]
        self.O = utils.transform_variables(self.O)
        self.sclX = MinMaxScaler(feature_range=(0, 1))
        self.sclY = MinMaxScaler(feature_range=(0, 1))
        return

    def form_look_back_array(self,X,y):
        dataX, dataY = [], []
        for i in range(self.look_back+1,len(X)):
            a = X[i-self.look_back:i, :].T
            dataX.append(a)
            dataY.append(y[i].tolist())
            pass
        return np.array(dataX), np.array(dataY)

    def create_master_model_data(self):
        X, y = self.O[self.xparams].values, self.O[self.yparam].values.ravel()
        X = self.sclX.fit_transform(X)
        self.rus = RandomUnderSampler(return_indices=True)
        X_resampled, y_resampled, idx_resampled = self.rus.fit_sample(X,y)
        y_bin = to_categorical(y_resampled)
        X_resampled, y_resampled = self.form_look_back_array(X_resampled, y_bin)
        X_train, X_test, y_train, y_test = train_test_split(X_resampled, y_resampled, test_size=1.0/3.0, random_state=42)
        return X_train, X_test, y_train, y_test


#################################################
## Main method used to test some functionality
#################################################
if __name__ == "__main__":
    case = -1
    if case == 0:
        import datetime as dt
        fetch_data_with_durations(dt.datetime(2001,3,2), verbose = True)
        pass
    elif case == 1:
        kp_ds = kp_classifier_dataset()
        pass
    pass
