##################THINGS YOU NEED TO MODIFY##########################


# SET THE FOLDER PATHS
ceres_dir = "/disk1/brems/ceres/"  # where to run ceres from. Need it as its Py2
# the files below need to be sorted by target and date
default_science_dir = "/disk1/brems/ferostest/"
# directrories for the calib and science files. Eventually you want 
# default_science_dir = defaultcalib_dir for the downloading. In the reduction
# there is the keyword extra_calib_dir=True, which you can set to False then
default_calib_dir   = "/disk1/brems/ferostest/"
# for writing the log files while downloading. E.g. failed downloads
default_log_dir =     "/disk1/brems/ferostest/"
default_astroquery_dir = '/disk1/brems/astroquery_cache/'  # set to None to keep astroquerys default

# OTHER CONFIGURATIONS
default_eso_user = "sbrems"
# only get data after that date. E.g. after 2005-09-30 where there was a major intervention
default_startdate = '2005-09-30'
