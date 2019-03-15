#!/usr/bin/env python

"""utils.py: utils is dedicated to utility functions."""

__author__ = "Chakraborty, S."
__copyright__ = "Copyright 2019, Space@VT"
__credits__ = []
__license__ = "MIT"
__version__ = "1.0."
__maintainer__ = "Chakraborty, S."
__email__ = "shibaji7@vt.edu"
__status__ = "Research"

import os
import numpy as np
import pandas as pd

from sklearn.preprocessing import MinMaxScaler
from imblearn.under_sampling import RandomUnderSampler
from sklearn.linear_model import LinearRegression, ElasticNet, BayesianRidge
from sklearn.svm import LinearSVR
from sklearn.tree import DecisionTreeRegressor, ExtraTreeRegressor
from sklearn.neighbors import KNeighborsRegressor
from sklearn.ensemble import AdaBoostRegressor, BaggingRegressor, ExtraTreesRegressor, GradientBoostingRegressor, RandomForestRegressor
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import RBF, Matern, RationalQuadratic as RQ

from startup import *

######################################################################################
## Marge staged yearly files
######################################################################################
def to_marged_yearly_files(case=1, years=[]):
    """
    This method joins all the parameter files and create yearly files
    case <int> is a parameter that decieds what files needs to be staged
    Case 0 : Binned using Kp and UT, Case 1 : Binned using F10.7 and Kp
    """
    params = ["Bx", "By_GSE", "Bz_GSE", "By_GSM", "Bz_GSM", "V", "Vx_GSE", "Vy_GSE", "Vz_GSE", "n", "T", "P_DYN", "E", "BETA", "MACH_A"]
    if len(years) == 0: years = range(1995,2019)

    ext = str(case) # Extension can be 0, 1 <Type of filter cases>
    file_ext = BASE_LOCATION + "omni/hdf5/intp/{ext}.%s.%d.h5".replace("{ext}",ext)
    file_store = BASE_LOCATION + "omni/hdf5/intp/{ext}.%d.h5".replace("{ext}",ext)
    for year in years:
        fname = file_store%year
        O = pd.DataFrame()
        for i,param in enumerate(params):
            f_name = file_ext%(param,year)
            _o = pd.read_hdf(f_name, mode="r", key="df", parse_dates=True)
            O[param] = _o[param]
            if i == 0: O["DATE"] = _o["DATE"]
            pass
        print "Save -to- %s"%fname
        O.to_hdf(fname, mode="w", key="df")
        pass

    ## Mearge symh data files year by year
    file_ext = BASE_LOCATION + "geomag/symh/hdf5/%d%02d.h5"
    file_store = BASE_LOCATION + "geomag/symh/hdf5/%d.h5"
    for year in years:
        fname = file_store%year
        O = pd.DataFrame()
        for month in range(1,13):
            f_name = file_ext%(year,month)
            _o = pd.read_hdf(f_name, mode="r", key="df", parse_dates=True)
            O = pd.concat([O,_o])
            pass
        print "Save -to- %s"%fname
        O.to_hdf(fname, mode="w", key="df")
        pass
    return


######################################################################################
## Resample (Downsample) to minutes
######################################################################################
def to_resample(minutes=5, case=1, years=[]):
    """
    """
    if len(years) == 0: years = range(1995,2019)
    ext = str(case) # Extension can be 0, 1 <Type of filter cases>
    omni_file_ext = BASE_LOCATION + "omni/hdf5/intp/{ext}.%d.h5".replace("{ext}",ext)
    symh_file_ext = BASE_LOCATION + "geomag/symh/hdf5/%d.h5".replace("{ext}",ext)
    fname_ext = BASE_LOCATION + str(minutes) + "_%d.h5"

    params = ["Bx", "By_GSE", "Bz_GSE", "By_GSM", "Bz_GSM", "V", "Vx_GSE", "Vy_GSE", "Vz_GSE", "n", "T", "P_DYN", "E", "BETA", "MACH_A", "SYM-H"]
    
    for year in years:
        fname = fname_ext%year
        omni_file = omni_file_ext%year
        symh_file = symh_file_ext%year
        _o = pd.read_hdf(omni_file, mode="r", key="df", parse_dates=True)
        _x = pd.read_hdf(symh_file, mode="r", key="df", parse_dates=True)
        _o["SYM-H"] = _x["SYM-H"]
        _o = _o.set_index("DATE")
        o = _o.groupby(pd.TimeGrouper(freq="%dMin"%minutes)).aggregate(np.mean)
        o["SYM-H_STD"] = _o.groupby(pd.TimeGrouper(freq="%dMin"%minutes)).aggregate(np.std)["SYM-H"]
        o = o.reset_index()
        o.to_hdf(fname, mode="w", key="df")
        pass
    return

##########################################################
## Fetch regressor by name
##########################################################
def get_deterministic_regressor_by_name(r_name):
    """
    This method is used to fetch deterministic regressor by name
    r_name :  Name of the regressor
    """

    REGs = {}
    # basic regressor            
    REGs["regression"] = LinearRegression()
    REGs["elasticnet"] = ElasticNet(alpha=.5,tol=1e-2)
    REGs["bayesianridge"] = BayesianRidge(n_iter=300, tol=1e-5, alpha_1=1e-06, alpha_2=1e-06, lambda_1=1e-06, lambda_2=1e-06, fit_intercept=True)

    # decission trees
    REGs["dtree"] = DecisionTreeRegressor(random_state=0,max_depth=5)
    REGs["etree"] = ExtraTreeRegressor(random_state=0,max_depth=5)

    # NN regressor
    REGs["knn"] = KNeighborsRegressor(n_neighbors=25,weights="distance")

    # ensamble models
    REGs["ada"] = AdaBoostRegressor()
    REGs["bagging"] = BaggingRegressor(n_estimators=50, max_features=3)
    REGs["etrees"] = ExtraTreesRegressor(n_estimators=50)
    REGs["gboost"] = GradientBoostingRegressor(max_depth=5,random_state=0)
    REGs["randomforest"] = RandomForestRegressor(n_estimators=100)
    return REGs[r_name]
