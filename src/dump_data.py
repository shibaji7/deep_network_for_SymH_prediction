import urllib2

############################
## 
############################
def download_omni_dataset():
    base_uri = "ftp://spdf.gsfc.nasa.gov/pub/data/omni/high_res_omni/monthly_1min/omni_min%d%02d.asc"
    base_storage = "/home/shibaji7/DATA/omni/raw/%d%02d.asc"
    for year in range(1995,2019):
        for month in range(1,13):
            url = base_uri%(year,month)
            print url
            response = urllib2.urlopen(url)
            print url, response.getcode()
            f_path = base_storage%(year,month)
            with open(f_path,"w") as f:
                f.write(response.read())
                pass
            pass
        pass
    return



if __name__ == "__main__":
    download_omni_dataset()
    pass
