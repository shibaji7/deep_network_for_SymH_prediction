#!/usr/bin/env python

"""startup.py: startup instatntiate all the file/folder systems and other constants."""

__author__ = "Chakraborty, S."
__copyright__ = "Copyright 2019, Space@VT"
__credits__ = []
__license__ = "MIT"
__version__ = "1.0."
__maintainer__ = "Chakraborty, S."
__email__ = "shibaji7@vt.edu"
__status__ = "Research"


import os

BASE_LOCATION = "/home/shibaji7/DATA/"

##############################################################################################
## Initialize the folder systems
##############################################################################################
def ini():
    try:
        os.makedirs( BASE_LOCATION + "omni/raw/" )
    except: print "System exception. Check folder sytems: %s"%(BASE_LOCATION + "omni/raw/")

    try:
        os.mkdir( BASE_LOCATION + "omni/hdf5/" )
    except: print "System exception. Check folder: %s"%(BASE_LOCATION + "omni/hdf5/")

    try:
        os.makedirs( BASE_LOCATION + "geomag/symh/raw/" )
    except: print "System exception. Check folder sytems: %s"%(BASE_LOCATION + "geomag/symh/raw/")

    try:
        os.mkdir( BASE_LOCATION + "geomag/symh/hdf5/" )
    except: print "System exception. Check folder: %s"%(BASE_LOCATION + "geomag/symh/hdf5/")

    try:
        os.mkdir( BASE_LOCATION + "premodel_da_figures/" )
    except: print "System exception. Check folder: %s"%(BASE_LOCATION + "premodel_da_figures/")
    return
