#!/usr/bin/env python

"""data_filling.py: data filling is dedicated for interpolateing NaN values, by solar cycle, Kp and UT binning method."""

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
import datetime as dt
import glob
import dask.dataframe as dd

np.random.seed(0)

from startup import *

############################################################################################################
## This method is used to convert Kp to linear binned scale and downsample it to 1m.
############################################################################################################
def to_binned_downsampled_Kp():
    Kp = pd.read_hdf(BASE_LOCATION + "geomag/Kp/hdf5/Kp.h5", mode="r", key="df", parse_dates=True)
    _sKp = Kp.Kp.tolist()
    _levels = ["0","0+","1-","1","1+","2-","2","2+","3-","3","3+","4-","4","4+",
            "5-","5","5+","6-","6","6+","7-","7","7+","8-","8","8+","9-","9"]
    _n = 0.33
    _lin_values = [3,3,3,3,3,3,3,3,3,3,6,6,6,6,
            6,6,6,6,6,9,9,9,9,9,9,9,9,9]
    _dict = dict(zip(_levels,_lin_values))
    _Kp_lin = []
    for _k in _sKp: _Kp_lin.append(_dict[_k])
    _Kp_lin = np.array(_Kp_lin)
    Kp.Kp = _Kp_lin
    Kp = Kp.set_index("DATE")
    Kp = Kp.resample("1T").interpolate(method="nearest")
    Kp = Kp.reset_index()
    return Kp

############################################################################################################
## This method is used to fetch and downsample F107 to 1m.
############################################################################################################
def to_binned_downsampled_F107():
    F107 = pd.read_hdf(BASE_LOCATION + "geomag/F10.7/hdf5/f10.7.h5", mode="r", key="df", parse_dates=True)
    F107 = F107.set_index("DATE")
    F107 = F107.resample("1T").interpolate(method="nearest")
    F107 = F107.reset_index()
    f107 = np.array(F107.F107)
    f107[f107<100.] = 50.
    f107[(f107>=100.) & (f107<200.)] = 150.
    f107[f107>=200.] = 250
    F107.F107 = f107
    return F107

#############################################################################################################
## This method is dedicated to interpolate the NaN entries individual solar wind driver.
## Note that, this method bins the SW parameter based on UT time and Kp and then tries to extrapolate.
## Solar Wind Parameters : "Bx", "By_GSE", "Bz_GSE", "By_GSM", "Bz_GSM", "V", "Vx_GSE", "Vy_GSE", 
##                         "Vz_GSE", "n", "T", "P_DYN", "E", "BETA", "MACH_A"
#############################################################################################################
def interpolate_sw_params(years=[], values="Bx"):

    def intp(x, _gr):
        v = x[values]
        if np.isnan(v):
            o = _gr[(_gr.index.get_level_values("UT") == x["UT"]) & (_gr.index.get_level_values("Kp") == x["Kp"])][values]
            v = np.random.normal(o["median"],o["std"])[0]
            pass
        return v

    def join(kp,o,values):
        _o = pd.DataFrame()
        _o["Kp"] = kp.Kp.tolist()
        _o["UT"] = o.UT.tolist()
        _o[values] = o[values].tolist()
        return _o

    nan_directory = {"Bx":9999.99, "By_GSE":9999.99, "Bz_GSE":9999.99, "By_GSM":9999.99, "Bz_GSM":9999.99, "V":99999.9,
                        "Vx_GSE":99999.9, "Vy_GSE":99999.9, "Vz_GSE":99999.9, "n":999.99, "T":9999999., "P_DYN":99.99,
                        "E":999.99, "BETA":999.99, "MACH_A":999.9}
    if len(years) == 0: years = range(1995,2019)
    hdf5_base = BASE_LOCATION + "omni/hdf5/%d*.h5"
    hdf5_yearly_intp_base = BASE_LOCATION + "omni/hdf5/intp/%d_%s.h5"
    Kp = to_binned_downsampled_Kp()
    for year in years:
        fname = hdf5_yearly_intp_base%(year,values)
        if not os.path.exists(fname):
            print "Processing SW parameter '%s' for year : %d"%(values, year)
            kp = Kp[(Kp.DATE >= dt.datetime(year,1,1)) & (Kp.DATE < dt.datetime(year+1,1,1))].reset_index()
            flist = glob.glob(hdf5_base%year)
            O = pd.DataFrame()
            for fname in flist:
                _o = pd.read_hdf(fname, mode="r", key="df", parse_dates=True)
                O = pd.concat([O, _o])
                pass
            O = O.sort_values("DATE")
            O = O[["DATE",values]]
            O["UT"] = O.DATE.apply(lambda x: x.hour)
            
            ## Convert bad values to NaNs
            O = O.replace(nan_directory[values],np.nan)
            jO = join(kp,O,values)
            _O = jO.dropna()
            print "After dropping NaN vlues, -to- %d,%d"%(len(O),len(_O))
            
            ## Bin dataset based on UT and Kp
            _gr = _O.groupby(["UT","Kp"]).agg({values:["median","std"]})
    
            ## Interpolate NaN values
            jO = dd.from_pandas(jO, npartitions=100)
            r_values = jO.apply(intp, axis=1, args=(_gr,)).compute(scheduler="threads")
            cnv = O[values].isna().sum()
            print "Converted NaNs - ",cnv
            O[values] = np.array(r_values).tolist()
            fname = hdf5_yearly_intp_base%(year,values)
            print "Save -to- %s"%fname
            O.to_hdf(fname, mode="w", key="df") 
            pass
        pass
    return

#############################################################################################################
## This method is dedicated to interpolate the NaN entries individual solar wind driver.
## Note that, this method bins the SW parameter based on F10.7 time and Kp and then tries to extrapolate.
## Solar Wind Parameters : "Bx", "By_GSE", "Bz_GSE", "By_GSM", "Bz_GSM", "V", "Vx_GSE", "Vy_GSE", 
##                         "Vz_GSE", "n", "T", "P_DYN", "E", "BETA", "MACH_A"
#############################################################################################################
def interpolate_sw_params_based_F107_Kp(years=[], values="Bx"):

    def intp(x, _gr):
        v = x[values]
        if np.isnan(v):
            o = _gr[(_gr.index.get_level_values("F107") == x["F107"]) & (_gr.index.get_level_values("Kp") == x["Kp"])][values]
            v = np.random.normal(o["median"],o["std"])[0]
            pass
        return v

    def join(kp,f107,o,values):
        _o = pd.DataFrame()
        _o["DATE"] = o.DATE.tolist()
        _o["Kp"] = kp.Kp.tolist()
        _o["F107"] = f107.F107.tolist()
        _o[values] = o[values].tolist()
        return _o

    nan_directory = {"Bx":9999.99, "By_GSE":9999.99, "Bz_GSE":9999.99, "By_GSM":9999.99, "Bz_GSM":9999.99, "V":99999.9,
                        "Vx_GSE":99999.9, "Vy_GSE":99999.9, "Vz_GSE":99999.9, "n":999.99, "T":9999999., "P_DYN":99.99,
                        "E":999.99, "BETA":999.99, "MACH_A":999.9}
    if len(years) == 0: years = range(1995,2019)
    hdf5_base = BASE_LOCATION + "omni/hdf5/%d*.h5"
    hdf5_yearly_intp_base = BASE_LOCATION + "omni/hdf5/intp/%d_%s_solcy_kp.h5"
    Kp = to_binned_downsampled_Kp()
    F107 = to_binned_downsampled_F107()
    for year in years:
        fname = hdf5_yearly_intp_base%(year,values)
        if not os.path.exists(fname):
            print "Processing SW parameter '%s' for year : %d"%(values, year)
            kp = Kp[(Kp.DATE >= dt.datetime(year,1,1)) & (Kp.DATE < dt.datetime(year+1,1,1))].reset_index()
            f107 = F107[(F107.DATE >= dt.datetime(year,1,1)) & (F107.DATE < dt.datetime(year+1,1,1))].reset_index()
            flist = glob.glob(hdf5_base%year)
            O = pd.DataFrame()
            for fname in flist:
                _o = pd.read_hdf(fname, mode="r", key="df", parse_dates=True)
                O = pd.concat([O, _o])
                pass
            O = O.sort_values("DATE")
            O = O[["DATE",values]]
            
            ## Convert bad values to NaNs
            O = O.replace(nan_directory[values],np.nan)
            jO = join(kp,f107,O,values)
            _O = jO.dropna()
            print "After dropping NaN vlues, -to- %d,%d"%(len(O),len(_O))
            
            ## Bin dataset based on UT and Kp
            _gr = _O.groupby(["Kp","F107"]).agg({values:["median","std"]})
    
            ## Interpolate NaN values
            jO = dd.from_pandas(jO, npartitions=100)
            r_values = jO.apply(intp, axis=1, args=(_gr,)).compute(scheduler="processes")
            cnv = O[values].isna().sum()
            print "Converted NaNs - ",cnv
            O[values] = np.array(r_values).tolist()
            fname = hdf5_yearly_intp_base%(year,values)
            print "Save -to- %s"%fname
            O.to_hdf(fname, mode="w", key="df") 
            pass
        pass
    return


if __name__ == "__main__":
    case = 1
    params = ["Bx", "By_GSE", "Bz_GSE", "By_GSM", "Bz_GSM", "V", "Vx_GSE", "Vy_GSE", "Vz_GSE", "n", "T", "P_DYN", "E", "BETA", "MACH_A"]
    for param in params:
        if case == 0: interpolate_sw_params(years=[], values=param)
        elif case == 1: interpolate_sw_params_based_F107_Kp(years=[], values=param)
        pass
    pass
