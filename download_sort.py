import os
import sys
import datetime
import numpy as np
import pandas as pd
from astroquery.eso import Eso
from subprocess import call
from astropy.time import Time
from shutil import copyfile
from warnings import warn
from glob import glob
from misc import find_night
from config import default_science_dir, default_calib_dir, default_log_dir, \
    default_astroquery_dir, default_eso_user, default_startdate

if __name__ == '__main__':
    full_download(sys.argv)


def full_download(target, extract=True, store_pwd=False,
                  overwrite_old="ask", clear_cache=False,
                  astroquery_dir=None,
                  calib_dir=None,
                  science_dir=None,
                  log_dir=None,
                  eso_user=None,
                  flat_min_exptime=1.,  # in sec
                  sort_calibfiles_by_target=False,
                  sort_sciencefiles_by_target=True,
                  query_radius="08+00",  # in "mm+ss"
                  startdate=None,
                  enddate="", ):
    '''Main function. Run this to get all FEROS science files and the corresponding caibration
    files for each night (5 BIAS, 10 flats, 12 wave calib). If there is anything off this standard
    calibration, no calib files are downloaded and the corresponding nights are stored in a file
    called failed_calib_<target>.txt. So check this out manually then.
    sort_calibfiles_by_target=True
    if true, make a subfolder with the targetname. Setting to false savese a lot of space
    and computing power if there are different targets in the same night
    sort_calibfiles_by_target=True
    if true, make a subfolder with the targetname. This is recommended
    overwrite_old="ask" [True, False]
    if True overwrites the old .fits files with the new ones during extraction.
    False does skip the extraction, ask will ask
    startdate/enddate= ""
    give the first and last date of the data to search. "" searches for all data.
    format is yyyy-mm-dd (exclusive for beginning)'''
    # load the default values if noothers were given
    if startdate is None:
        startdate=default_startdate
    print('Searching only for data after {}'.format(startdate))
    if eso_user is None:
        eso_user = default_eso_user
    if astroquery_dir is None:
        astroquery_dir = default_astroquery_dir
    if calib_dir is None:
        calib_dir = default_calib_dir
    if science_dir is None:
        science_dir = default_science_dir
    if log_dir is None:
        log_dir = default_log_dir

    if sort_calibfiles_by_target:
        calib_dir = os.path.join(calib_dir, target)
    if sort_sciencefiles_by_target:
        science_dir = os.path.join(science_dir, target)
    # file of the query result.e.g. which files are gonna be downloaded
    fn_query = "last_output_query.csv"

    t_science = query_eso(target, category='SCIENCE', 
                          sdate=startdate, edate=enddate,
                          fn_query=fn_query)
    for outdir in [calib_dir, science_dir, astroquery_dir]:
        if outdir is not None:
            if not os.path.exists(outdir):
                os.makedirs(outdir)

    assert len(query_radius) == 5
    # find the different date. Look for morning/evening and adjust date!
    nights = []
    failed_calib_nights = []
    calib_ids = []
    # for some reason ESO misses some downloads sometimes,
    # Even if theyre in the confirmation mail. catch them
    # edit: mostly those are files already in the cache.
    missing_downloads = []
    id2night = {}
    science_ids = []
    # get the right night for the science file
    for science_id, dtime in zip(t_science['Dataset ID'], 
                                 t_science['MJD-OBS']):
        science_ids.append(science_id)
        night = find_night(dtime)
        id2night[science_id]=night
        nights.append(night)
    nights = np.unique(nights)
    print('Found %d science obs in %d different nights.' % (len(t_science),
                                                            len(nights)))

    for night in nights:
        if not os.path.exists(ddir):
            os.mkdir(ddir)
#        os.chdir(ddir)
        these_calib_ids, these_failed_calib = get_calib(night,
                                      flat_min_exptime=flat_min_exptime)
        calib_ids.append(these_calib_ids)
        failed_calib_nights += these_failed_calib

        for tcalib_id in these_calib_ids:
            id2night[tcalib_id] = night
    

    print('Downloading the %d files for target %s' % (len(id2night.keys()),
                                                      target))
    astroquery_dir = download_id(id2night.keys(), eso_user, store_pwd=store_pwd,
                                 astroquery_dir=astroquery_dir)
    compress_files(astroquery_dir, fileending='.fits')
    print('Downloaded')
    fn_failed = os.path.join(log_dir, 'failed_calib_searches.csv')
    with open(fn_failed, 'a') as f_failed:
        for failed_night in failed_calib_nights:
            f_failed.write("{}, {}, {}\n".format(target, failed_night,
                                               datetime.date.today()))
    print('Moving files to the appropriate directories')
    # for fpath in downloaded:
    missing_downloads = distribute_files(id2night.keys(),
                                         id2night, target,
                                         astroquery_dir,
                                         calib_dir, science_dir,
                                         science_ids=science_ids)

    while (len(missing_downloads) >= 1):
        old_len_missing = len(missing_downloads)
        print('%d files got lost on the way. Try to redownload them...' %
              (old_len_missing))
        #astroquery_dir = download_id(missing_downloads, eso_user,
        #                             astroquery_dir=astroquery_dir,
        #                             store_pwd=store_pwd)
        compress_files(astroquery_dir, fileending='.fits')
        missing_downloads = distribute_files(missing_downloads,
                                             id2night, target,
                                             astroquery_dir,
                                             calib_dir, science_dir,
                                             science_ids=science_ids)
        if old_len_missing == len(missing_downloads):
            fn_failed_down = os.path.join(log_dir,
                                          'failed_download_files.csv')
            with open(fn_failed_down, 'a') as f_failed_down:
                for failed_down in missing_downloads:
                    f_failed_down.write("{}, {}, {}\n".format(target, failed_down,
                                                              datetime.date.today()))
            print('Could not download %d files. Please download them manually. \
You find them in %s' % (old_len_missing, fn_failed_down))
            break
    print('Done downloading %d files. Had problems with %d nights (stored in %s)'
          % (len(id2night.keys()), len(failed_calib_nights), fn_failed))
    if extract:
        overwrite_old = extract_files(direct=os.path.join(calib_dir),
                      overwrite_old=overwrite_old)
        overwrite_old = extract_files(direct=os.path.join(science_dir),
                      overwrite_old=overwrite_old)
    if clear_cache:
        cachefiles = os.listdir(astroquery_dir)
        for cfile in cachefiles:
            os.remove(os.path.join(astroquery_dir, cfile))
    print('Done with with all for target {}  :)'.format(target))



def query_eso(target, instrument='FEROS', category='SCIENCE',
              sdate="", edate="", maxrows=999999, query_radius="08+00",
              fn_query="last_output_query.csv"):
    call(["wget", "-O", fn_query, "http://archive.eso.org/wdb/wdb/eso/eso_archive_main/query?tab_object=on&target=" + target.replace('+', '%2B') + "&resolver=simbad&tab_target_coord=on&ra=&dec=&box=00+"+query_radius+"&deg_or_hour=hours&format=SexaHours&tab_prog_id=on&prog_id=&tab_instrument=on&instrument=" +
          instrument + "&stime=" + sdate + "&starttime=12&etime=" + edate + "&endtime=12&tab_dp_cat=true&dp_cat=" + category + "&top=" + str(maxrows) + "&wdbo=csv"])
    try:
        table = pd.read_csv(fn_query, comment='#', sep=',',
                            skip_blank_lines=True)
    except:
        # make an empty table if the file was empty
        table = pd.DataFrame(columns=['OBJECT', 'RA', 'DEC',
                                      'Program_ID', 'Instrument',
                                      'Category', 'Type', 'Mode',
                                      'Dataset ID', 'Release_Date',
                                      'TPL ID', 'TPL START',
                                      'Exptime', 'Filter',
                                      'MJD-OBS', 'Airmass'])
    if category == 'SCIENCE':
        # this is to remove any None lines the query sometimes returns. Dont check fo absolute values as airmass is not given for observations before 2003
        table = table.loc[lambda x:x.Airmass != 0.]
    return table


def filter_calib(table, date=None, keep=None, 
                 flat_min_exptime=1):  # in sec
    '''This routine tries to filter the FEROS calibration data.
    You give it the table and it returns only the needed data.
    Use check_calib afterwards to see if it has worked.
    keep: 'first', 'last' ; set this keyword to keep the first
    or last 27 datasets'''
    table = table.sort_values('MJD-OBS').reset_index()
    if date is not None:
        table = table.iloc[np.where([date in idate for idate in
                                     Time(table['MJD-OBS'],
                                          format='mjd').iso])[0]]
    table = table[(table.Type == 'BIAS') |
                  (table.Type == 'WAVE') |
                  ((table.Type == 'FLAT') &
                   (table.Exptime >= flat_min_exptime))]
    if len(table[table.Type == 'BIAS']) == 6:
        idx_first_bias = table[(table.Type == 'BIAS')].index[0]
        table.drop(idx_first_bias)
    table.reset_index()
    table = table.drop(table[(table.Type == 'FLAT') &
                             ~(table.Exptime >= flat_min_exptime)].index)
    # if still too many entries, try to cut the first or last
    table.reset_index()
    if keep == 'last':
        table = table.iloc[-27:]
    if keep == 'first':
        table = table.iloc[:27]
    return table


def check_calib(table, flat_min_exptime=1):
    if ((len(table[table.Type == 'BIAS']) == 5) &
        (len(table[(table.Type == 'FLAT') & (table.Exptime >= flat_min_exptime)]) == 10) &
            ((len(table[table.Type == 'WAVE']) in [6, 12]))):
        return True
    else:
        return False


def download_id(ids, eso_user, astroquery_dir=None,
                store_pwd=False):
    if not "eso" in locals():
        eso = Eso()
    if astroquery_dir is not None:
        eso.cache_location = astroquery_dir
    
    # find ids which were already downloaded
    len_before = len(ids)
    archivefiles = os.listdir(eso.cache_location)
    ids = [idd for idd in ids if idd+'.fits.Z' not in archivefiles]
    print('Of the {} ids requested, {} are already in cache'.format(
        len_before, len_before-len(ids)))
    # make sure ids is a list to not confuse eso and make it not too long
    maxlength = 500
    ids = [ii for ii in ids]
    ids = [ids[ii:ii + maxlength] for ii in range(0, len(ids), maxlength)]
    logged_in = False
    while not logged_in:
#        try:
            logged_in = eso.login(eso_user, store_password=store_pwd)
#        except ValueError:
#            logged_in = True
        
    for iid in ids:
        iid = iid
        print('ESO is getting the archive files ({} files) . \
This may take some time! Be patient ;) It might have been \
split up into smaller chunks.'.format(len(iid)))
        eso.retrieve_data(iid)
    return eso.cache_location

def get_calib(date, flat_min_exptime=1):
    date = date.replace(" ", "-")
    edate = Time(Time(date).jd + 1, format='jd').iso[0:10]

    t_query = query_eso("", category="CALIB",
                        sdate=date,
                        edate=edate)
    # remove non-technical time
    t_query = t_query[t_query.Program_ID.str.startswith('60.A-')]
    # try the first half of the night
    calib_failed = True
    if len(t_query) > 0:
        # try first half of the night, then second
        if check_calib(filter_calib(t_query, date=date, keep='first',
                                    flat_min_exptime=flat_min_exptime),
                                    flat_min_exptime=flat_min_exptime):
            t_query2 = filter_calib(t_query, date=date, keep='first',
                                    flat_min_exptime=flat_min_exptime)
            calib_failed = False
        elif check_calib(filter_calib(t_query, date=date, keep='last',
                                    flat_min_exptime=flat_min_exptime),
                                    flat_min_exptime=flat_min_exptime):
            t_query2 = filter_calib(t_query, date=date, keep='last',
                                    flat_min_exptime=flat_min_exptime)
            calib_failed = False
        elif check_calib(filter_calib(t_query, date=edate, keep='first',
                                    flat_min_exptime=flat_min_exptime),
                                    flat_min_exptime=flat_min_exptime):
            t_query2 = filter_calib(t_query, date=edate, keep='first',
                                    flat_min_exptime=flat_min_exptime)
            calib_failed = False
        elif check_calib(filter_calib(t_query, date=edate, keep='last',
                                    flat_min_exptime=flat_min_exptime),
                                    flat_min_exptime=flat_min_exptime):
            t_query2 = filter_calib(t_query, date=edate, keep='last',
                                    flat_min_exptime=flat_min_exptime)
            calib_failed = False

        if not calib_failed:
            down_ids = t_query2['Dataset ID'].tolist()
    if calib_failed:
        down_ids = []
        print('Cant find calib files automatically for date ', date)
        check_manually = [date, ]
    else:
        check_manually = []

    return down_ids, check_manually


def distribute_files(id_list, id2night, target, src_dir,
                     calib_dir, science_dir,
                     science_ids=[], fileending='.fits.Z'):
    '''Distribute the downloaded files to the output folder. If you provide
    the science ids, the science files will be copied in
    a different folder than the nights.'''
    missing_files = []
    outdir = {'calibfile': calib_dir,
              'sciencefile': science_dir}
    for iid in id_list:
        fpath = os.path.join(src_dir, iid + fileending)
        if iid in science_ids:
            filetype = 'sciencefile'
            pnout = os.path.join(outdir[filetype], iid+fileending)
        else:
            filetype = 'calibfile'
            pnout = os.path.join(outdir[filetype], id2night[iid].replace("-", ""),
                                 iid+fileending)
        if not os.path.isfile(pnout):
            try:
                copyfile(fpath, pnout)
            except IOError:
                if filetype == 'sciencefile':
                    print('Somehow {} {} is missing (protected file?) - or {} cannot be written. \
Trying downloading it later again.'.format(filetype, fpath, pnout))
                    missing_files.append(iid)
                else:
                    warn('{} {} could not be downloaded. Even though it is not a \
potentially protected sciencefile')
    return missing_files



def compress_files(direct, fileending='.fits'):
    '''Compress the files having the fileending "fileending=.fits". This is necessary
    as newer eso version automatically uncompresses compressed .fits.Z files. Recompressing
    saves space and handling easier, as it is independent of the version. Ignoring
    files which are compressed already.'''
    filelist = [ff for ff in os.listdir(direct) if np.logical_and(
        ff.endswith(fileending), not os.path.isfile(ff+'.Z'))]
    print('Compressing the {} files in {}'.format(len(filelist), direct))
    for ffile in filelist:
        call(["compress", os.path.join(direct, ffile)])


def extract_files(direct, overwrite_old="ask"):
    '''decompressing all .fits.Z files in the directory+subdirectories'''
    filelist = [yy for x in os.walk(direct)
                for yy in glob(os.path.join(x[0], '*.fits.Z'))]
    print('Uncompressing the %d files' % len(filelist))
    for ifile in filelist:
        # if the target file does exist, ask
        if os.path.isfile(ifile[:-2]):
            if overwrite_old not in [True, False]:
                overwrite_old = input(
                    "File %s does exist. Overwrite all existing files? Type 'y' or get asked for each file:" % (ifile[:-2]))
                if overwrite_old in ['y', 'Y', 'j', 'J', 't', 'T', 'True']:
                    overwrite_old = True
                else:
                    overwrite_old = 'ask'
            if overwrite_old:
                os.remove(ifile[:-2])
            if not overwrite_old:
                continue
        call(["uncompress", ifile])
    return overwrite_old
