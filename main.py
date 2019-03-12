#!/usr/bin/env python

"""main.py: This is the main function, used to submit any jobs on VT ARC system."""

__author__ = "Chakraborty, S."
__copyright__ = "Copyright 2019, Space@VT"
__credits__ = []
__license__ = "MIT"
__version__ = "1.0."
__maintainer__ = "Chakraborty, S."
__email__ = "shibaji7@vt.edu"
__status__ = "Research"


import subprocess

if __name__ == "__main__":
    print "Started main loop..."
    subprocess.call(["python", "src/process_data.py"])
    print "End main loop."
    pass
