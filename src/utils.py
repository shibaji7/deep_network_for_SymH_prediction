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
import pandas as pd

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
    if len(years) == 0: years = range(1995,2019)
    hdf5_yearly_intp_base = BASE_LOCATION + "omni/hdf5/intp/1.%s.%d.h5"
    to_file = BASE_LOCATION + "omni/hdf5/base/%d.h5"
    params = ["Bx", "By_GSE", "Bz_GSE", "By_GSM", "Bz_GSM", "V", "Vx_GSE", "Vy_GSE", "Vz_GSE", "n", "T", "P_DYN", "E", "BETA", "MACH_A"]
    for year in years:
        O = pd.DataFrame()
        to_filename = to_file%year 
        if os.path.exists(to_filename):
            for param in params:
                fname = hdf5_yearly_intp_base%(param,) 
                pass
            print "Converted -to-", to_filename
            O.to_hdf(to_filename, mode="w", key="df")
            pass
        else: print "File exists - ", to_filename
        pass
    return
