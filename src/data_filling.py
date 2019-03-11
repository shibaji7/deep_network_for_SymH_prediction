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

import numpy as np
import pandas as pd
import datetime as dt
import glob

np.random.seed(0)

from startup import *

############################################################################################################
## This method is used to convert Kp to linear scale and downsample it to 1m.
############################################################################################################
def to_linear_downsampled_Kp():
    Kp = pd.read_hdf(BASE_LOCATION + "geomag/Kp/hdf5/Kp.h5", mode="r", key="df", parse_dates=True)
    _sKp = Kp.Kp.tolist()
    _levels = ["0","0+","1-","1","1+","2-","2","2+","3-","3","3+","4-","4","4+",
            "5-","5","5+","6-","6","6+","7-","7","7+","8-","8","8+","9-","9"]
    _n = 0.33
    _lin_values = [0,0+_n,1-_n,1,1+_n,2-_n,2,2+_n,3-_n,3,3+_n,4-_n,4,4+_n,
            5-_n,5,5+_n,6-_n,6,6+_n,7-_n,7,7+_n,8-_n,8,8+_n,9-_n,9]
    _dict = dict(zip(_levels,_lin_values))
    _Kp_lin = []
    for _k in _sKp: _Kp_lin.append(_dict[_k])
    _Kp_lin = np.array(_Kp_lin)
    Kp.Kp = _Kp_lin
    Kp = Kp.set_index("DATE")
    Kp = Kp.resample("1T").interpolate(method="nearest")
    Kp = Kp.reset_index()
    return Kp

#############################################################################################################
## This method is dedicated to interpolate the NaN entries individual solar wind driver.
## Note that, this method bins the SW parameter based on UT time and Kp and then tries to extrapolate.
## Solar Wind Parameters : "Bx", "By_GSE", "Bz_GSE", "By_GSM", "Bz_GSM", "V", "Vx_GSE", "Vy_GSE", 
##                         "Vz_GSE", "n", "T", "P_DYN", "E", "BETA", "MACH_A"
#############################################################################################################
def interpolate_sw_params(years=[], values="Bx"):
    nan_directory = {"Bx":9999.99, "By_GSE":9999.99, "Bz_GSE":9999.99, "By_GSM":9999.99, "Bz_GSM":9999.99, "V":99999.9,
                        "Vx_GSE":99999.9, "Vy_GSE":99999.9, "Vz_GSE":99999.9, "n":999.99, "T":9999999., "P_DYN":99.99,
                        "E":999.99, "BETA":999.99, "MACH_A":999.9}
    if len(years) == 0: years = range(1995,2019)
    hdf5_base = BASE_LOCATION + "omni/hdf5/%d*.h5"
    hdf5_yearly_intp_base = BASE_LOCATION + "omni/hdf5/intp/%d_%s.h5"
    Kp = to_linear_downsampled_Kp()
    for year in years:
        print "Processing SW parameter '%s' for year : %d"%(values, year)
        kp = Kp[(Kp.DATE >= dt.datetime(year,1,1)) & (Kp.DATE < dt.datetime(year+1,1,1))]
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
        _O = O.copy(True).join(kp, how="inner", lsuffix="kp").dropna()
        print "After dropping NaN vlues, -to- %d,%d"%(len(O),len(_O))
        
        ## Bin dataset based on UT and Kp
        _gr = _O.groupby(["UT","Kp"]).agg({values:["median","std"]})

        ## Interpolate NaN values
        r_values = []
        print O.head()
        for i, row in O.iterrows():
            if np.isnan(row[values]):
                o = _gr[(_gr.index.get_level_values("UT") == row["UT"]) & (_gr.index.get_level_values("Kp") == row["Kp"])][values]
                v = np.random.normal(o["median"],o["std"])[0]
                print "None",row["DATE"]
                r_values.append(v)
            else:
                print row["DATE"]
                r_values.append(row[values])
                pass
        O[values] = r_values
        print O.head()
        fname = hdf5_yearly_intp_base%(year,values)
        print "Save -to- %s"%fname
        pass
    return

 
if __name__ == "__main__":
    params = ["Bx", "By_GSE", "Bz_GSE", "By_GSM", "Bz_GSM", "V", "Vx_GSE", "Vy_GSE", "Vz_GSE", "n", "T", "P_DYN", "E", "BETA", "MACH_A"]
    params = ["Bx"]
    for param in params:
        interpolate_sw_params(years=[1995], values=param)
        pass
    pass
