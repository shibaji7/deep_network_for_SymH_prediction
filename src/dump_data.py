#!/usr/bin/env python

"""dump_data.py: dump data downloads all the ascii type files from different FTP repositories."""

__author__ = "Chakraborty, S."
__copyright__ = "Copyright 2019, Space@VT"
__credits__ = []
__license__ = "MIT"
__version__ = "1.0."
__maintainer__ = "Chakraborty, S."
__email__ = "shibaji7@vt.edu"
__status__ = "Research"

import os
import urllib2
import requests
import numpy as np
import datetime as dt
from calendar import monthrange

from startup import *

##############################################################################################
## Download 1m resolution solar wind omni data from NASA GSFC ftp server
##############################################################################################
def download_omni_dataset():
    base_uri = "ftp://spdf.gsfc.nasa.gov/pub/data/omni/high_res_omni/monthly_1min/omni_min%d%02d.asc"
    base_storage = BASE_LOCATION + "omni/raw/%d%02d.asc"
    for year in range(1995,2019):
        for month in range(1,13):
            url = base_uri%(year,month)
            response = urllib2.urlopen(url)
            f_path = base_storage%(year,month)
            with open(f_path,"w") as f:
                f.write(response.read())
                pass
            pass
        pass
    return


##############################################################################################
## Download 1m resolution ASY / SYM data from WDC Kyoto ftp server
##############################################################################################
def download_symH_dataset():
    base_uri = "http://wdc.kugi.kyoto-u.ac.jp/cgi-bin/aeasy-cgi?Tens=%s&Year=%s&Month=%s&Day_Tens=0&Days=1&Hour=00&min=00&Dur_Day_Tens=%s&Dur_Day=%s&Dur_Hour=00&Dur_Min=00&Image+Type=GIF&COLOR=COLOR&AE+Sensitivity=0&ASY/SYM++Sensitivity=0&Output=ASY&Out+format=IAGA2002&Email=shibaji7@vt.edu"
    base_storage = BASE_LOCATION + "geomag/symh/raw/%d%02d.asc"
    for year in range(1995, 2019):
        for month in range(1,13):
            ytens = str(int(year/10))
            yrs = str(np.mod(year,10))
            mnt = "%02d"%month
            _,dur = monthrange(year, month)
            durtens = "%02d"%(int(dur/10))
            dur = str(np.mod(dur,10))
            url = base_uri%(ytens,yrs,mnt,durtens,dur)
            f_path = base_storage%(year,month)
            cmd = "wget -O '%s' '%s'"%(f_path, url)
            os.system(cmd)
            pass
        pass
    return

##############################################################################################
## Download 3h resolution Kp data from WDC Kyoto ftp server
##############################################################################################
def download_Kp_dataset():
    cmd = "curl -o \"/home/shibaji7/DATA/geomag/Kp/raw/Kp.asc\" --data \"SCent=19&STens=9&SYear=5&From=1&ECent=20&ETens=1&EYear=8&To=12&Email=shibaji7%40vt.edu\" http://wdc.kugi.kyoto-u.ac.jp/cgi-bin/kp-cgi"
    os.system(cmd)
    return

##############################################################################################
## Download 1D resolution F10.7 data from Colorado LASP server
##############################################################################################
def download_F107_dataset():
    cmd = "wget -O " + BASE_LOCATION + "geomag/F10.7/raw/f10.7.asc 'http://lasp.colorado.edu/lisird/latis/noaa_radio_flux.csv?time>=1995-01-01&time<2019-01-01'"
    os.system(cmd)
    return

 
if __name__ == "__main__":
    download_F107_dataset()
    pass
