# DOWNLOADING AND REDUCING FEROS DATA
## Installation
Note that the script is written in python3, but CERES is based on python2,
requiring you to have both installed.

### Non standard anaconda packages needed
To download the data you need
* astroquery   ( conda install -c astropy astroquery )
* gi           ( conda install -c conda-forge pygobject )


If you have issues with the keyring, try keyrings.alt
* keyrings.alt  (  conda install -c conda-forge keyrings.alt )

To also reduce the data, you additionally need
* tqdm         (  conda install -c conda-forge tqdm )
* pysynphot    ( conda config --add channels http://ssb.stsci.edu/astroconda && conda install pysynphot )
* my misc routines ( git clone https://github.com/sbrems/misc.git )
* CERES ( see https://github.com/rabrahm/ceres for installation notes)

If you want to collect the PDFs and CCFs, you also need:
* pypdf2 (  conda install -c conda-forge pypdf2 )
* reportlab ( conda install -c anaconda reportlab )


## Running the pipeline
First you need to set up the config.py. Here you need to specify the locations
where to save the data and the log files. Best is to set calib_dir = science_dir
so that the data can be reduced straight away.
### Downloading the files
To download all files from e.g. tau Cet do. Best is to avoid spaces in the name
```python
>>> from download_sort import full_download
>>> full_download("tau_Cet",
                  store_pwd=False,
                  unrobust_calibfiles=True,
                  sort_calibfiles_by_target=False, 
                  sort_sciencefiles_by_target=False)
```
The keywords are optional and the default setting. Note that the unrobust_calibfiles=True
setting will download all dark, flat and wave files of the daycalibration if the automatic
determination of the correct ones has failed. Check failed_calib.csv in the log_dir to see
which nights those were.
Please make sure, that the name is recognized by Simbad since parameters such
as coordinates need to be queried. The selection of the files is purely done
by coordinates returned from Simbad. A radius of 8 arcmin is used.

### Reducing the files
If you did not change the folder structure from the download process and want
to use 4 CPUs, simply type
```python
>>> import run_feros_pipeline
>>> run_feros_pipeline.all_subfolders(npools=4, 
                                      do_class=False)
```
The do_class is the passed to CERES, which can analyse some target properties like Teff.
False doesnt analyse those.

### Collecting the reduced files
run the following commands:
```python
>>> import collect_results
>>> collect_results.all_csvs()
```
You may set activity_indicators=True to return things like the Halpha index.
This is still experimental and might return wrong results.
The results will be stored in observation_summary.csv in your log_dir
