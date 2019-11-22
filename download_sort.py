import os
import sys
import datetime
import time
import numpy as np
import pandas as pd
from itertools import groupby
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
                  sort_sciencefiles_by_target=False,
                  query_radius="08+00",  # in "mm+ss"
                  startdate=None,
                  enddate="", ):
    '''Main function. Run this to get all FEROS science files and the corresponding caibration
    files for each night (5 BIAS, 10 flats, 6 or 12 wave calib). If there is anything off this standard
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
    id2nights = {}
    science_ids = []
    # get the right night for the science file
    for science_id, dtime in zip(t_science['Dataset ID'], 
                                 t_science['MJD-OBS']):
        science_ids.append(science_id)
        night = find_night(dtime)
        if science_id in id2nights.keys():
            id2nights[science_id] = id2nights[science_id] + [night,]
        else:
            id2nights[science_id] = [night, ]
        nights.append(night)
    nights = np.unique(nights)
    print('Found %d science obs in %d different nights.' % (len(t_science),
                                                            len(nights)))

    for night in nights:
        ddir = os.path.join(calib_dir, night.iso[:10].replace("-", ""))
        if not os.path.exists(ddir):
            os.mkdir(ddir)
#        os.chdir(ddir)
        these_calib_ids, these_failed_calib = get_calib(night,
                                      flat_min_exptime=flat_min_exptime)
        calib_ids.append(these_calib_ids)
        failed_calib_nights += these_failed_calib

        for tcalib_id in these_calib_ids:
            if tcalib_id in id2nights.keys():
                id2nights[tcalib_id] = id2nights[tcalib_id] + [night, ]
            else:
                id2nights[tcalib_id] = [night, ]
    

    print('Downloading the %d files for target %s' % (len(id2nights.keys()),
                                                      target))
    astroquery_dir = download_id(id2nights.keys(), eso_user, store_pwd=store_pwd,
                                 astroquery_dir=astroquery_dir)

    print('Downloaded')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    fn_failed = os.path.join(log_dir, 'failed_calib_searches.csv')
    with open(fn_failed, 'a') as f_failed:
        for failed_night in failed_calib_nights:
            f_failed.write("{}, {}, {}\n".format(target, failed_night,
                                               datetime.date.today()))

    compress_files(astroquery_dir, fileending='.fits')

    print('Moving files to the appropriate directories')
    # for fpath in downloaded:
    missing_downloads, science_files = distribute_files(
        id2nights.keys(),
        id2nights,
        astroquery_dir,
        calib_dir, science_dir,
        science_ids=science_ids)

    # save the science file logs for later identification
    fn_scfiles = os.path.join(log_dir, 'science_files.csv')
    with open(fn_scfiles, 'a') as f_scfiles:
        for scf in science_files:
            f_scfiles.write("{}, {}, {}\n".format(target, scf,                                                          datetime.date.today()))

    while (len(missing_downloads) >= 1):
        old_len_missing = len(missing_downloads)
        print('%d files got lost on the way. Try to redownload them...' %
              (old_len_missing))
        astroquery_dir = download_id(missing_downloads, eso_user,
                                     astroquery_dir=astroquery_dir,
                                     store_pwd=store_pwd)
        compress_files(astroquery_dir, fileending='.fits')
        missing_downloads, science_files2 = distribute_files(
            missing_downloads,
            id2nights,
            astroquery_dir,
            calib_dir, science_dir,
            science_ids=science_ids)
        # science_files = np.unique(science_files + science_files2)
        if old_len_missing == len(missing_downloads):
            fn_failed_down = os.path.join(log_dir,
                                          'failed_download_files.csv')

            with open(fn_failed_down, 'a') as f_failed_down:
                for failed_down in missing_downloads:
                    f_failed_down.write("{}, {}, {}\n".format(target, failed_down,
                                                              datetime.date.today()))
            with open(fn_scfiles, 'a') as f_scfiles:
                for scf in science_files2:
                    f_scfiles.write("{}, {}, {}\n".format(target, scf,
                                                          datetime.date.today()))
            print('Could not download %d files. Please download them manually. \
Probably they are proprietary. You find them in %s' % (old_len_missing, fn_failed_down))
            break
    print('Done downloading %d files. Had problems with %d nights (stored in %s)'
          % (len(id2nights.keys()), len(failed_calib_nights), fn_failed))

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
              sdate="", edate="", starttime="12", endtime="12",
              maxrows=999999, query_radius="08+00",
              fn_query="last_output_query.csv"):
    call(["wget", "-O", fn_query, "http://archive.eso.org/wdb/wdb/eso/eso_archive_main/query?tab_object=on&target=" + target.replace('+', '%2B') + "&resolver=simbad&tab_target_coord=on&ra=&dec=&box=00+"+query_radius+"&deg_or_hour=hours&format=SexaHours&tab_prog_id=on&prog_id=&tab_instrument=on&instrument=" +
          instrument + "&stime=" + sdate + "&starttime=" + starttime + "&etime=" + edate + "&endtime=" + endtime + "&tab_dp_cat=true&dp_cat=" + category + "&top=" + str(maxrows) + "&wdbo=csv"])
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


def filter_calib(table, date, keep=None,
                 flat_min_exptime=1):  # in sec
    '''This routine tries to filter the FEROS calibration data.
    You give it the table and it returns only the needed data.
    Use check_calib afterwards to see if it has worked.
    keep: 'first', 'last' ; set this keyword to keep the first
    or last 27 datasets'''
    if Time(date, format='iso') < Time('2017-12-13T12:00:00.000', format='isot'):
        new_calib = False
    else:
        new_calib = True

    table = table.sort_values('MJD-OBS').reset_index()

    if date is not None:
        table = table.iloc[np.where([date in idate.iso for idate in
                                     Time(table['MJD-OBS'],
                                          format='mjd')])[0]]
    table = table[(table.Type == 'BIAS') |
                  (table.Type == 'WAVE') |
                  ((table.Type == 'FLAT') &
                   (table.Exptime >= flat_min_exptime))]
    # if there are 6 BIASES, drop the first one as only 5 are needed and error
    # is known
    if np.sum(table.Type == 'BIAS') == 6:
        idx_first_bias = table[(table.Type == 'BIAS')].index[0]
        table = table.drop(idx_first_bias)
    table.reset_index()
    table = table.drop(table[(table.Type == 'FLAT') &
                             ~(table.Exptime >= flat_min_exptime)].index)
    # if all waves are taken in a row, keep the last of them
    # first find the longest consecutive waves
    waveidz = longest_sequence_idz(list(table.Type), key='WAVE')
    if new_calib:
        max_waves = 30
    else:
        max_waves = 12
    if len(waveidz) > max_waves:
        if keep == 'last':
            waveidz = waveidz[-max_waves:]
        elif keep == 'first':
            waveidz = waveidz[:max_waves]
    # now remove the other waves
    remidz = [ri for ri in table[table.Type=='WAVE'].index if ri not in waveidz]
    table = table.drop(remidz)
    table.reset_index()

    # if still too many entries, try to cut the first or last if still the
    # old calibration plan before 2018. Otherwise keep all with 30s integration
    # as discussed with R. Brahm
    if not new_calib:
        if len(table) > 27:
            if keep == 'last':
                table = table.iloc[-27:]
            elif keep == 'first':
                table = table.iloc[:27]
    else:
        wrongwaveidz = table[np.logical_and(table['OBJECT'] == 'WAVE',
                                            np.abs(table['Exposure']-30) >= 2)].index
        table = table.drop(wrongwaveidz)
        table.reset_index()
        nwave = len(table[table.Type == 'WAVE'])
        if keep == 'last':
            table = table[-(15+nwave):]
        elif keep == 'first':
            table = table[:15+nwave]
        table.reset_index()

    return table


def check_calib(table, flat_min_exptime=1):
    if ((len(table[table.Type == 'BIAS']) == 5) &
        (len(table[(table.Type == 'FLAT') & (table.Exptime >= flat_min_exptime)]) == 10) &
            ((len(table[table.Type == 'WAVE']) in np.hstack(((6, 12), np.arange(21, 31)))))):
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

    logged_in = eso.authenticated()
    while not logged_in:
        try:
            eso.login(eso_user, store_password=store_pwd)
        except:
            print('Login Failed. Wrong username or password?')
            time.sleep(3)
        logged_in = eso.authenticated()

    for iid in ids:
        iid = iid
        print('ESO is getting the archive files ({} files) . \
This may take some time! Be patient ;) It might have been \
split up into smaller chunks.'.format(len(iid)))
        try:
            eso.retrieve_data(iid)
        except:
            print('An error during downloading occured. Waiting 30s and trying again')
            time.sleep(30)
            eso.retrieve_data(iid)
    return eso.cache_location

def get_calib(night, flat_min_exptime=1):
    # convert Time to strings for query
    edate = Time(night.jd + 1, format='jd').iso[:10]
    date = night.iso[:10]

    t_query = query_eso("", category="CALIB",
                        sdate=date, starttime="00",
                        edate=edate, endtime="24")
    # remove non-technical time
    t_query = t_query[t_query.Program_ID.str.startswith('60.A-')]
    # try the first half of the night
    calib_failed = True
    if len(t_query) > 0:
        # try first half of the night, then second
        if check_calib(filter_calib(t_query, date=date, keep='last',
                                    flat_min_exptime=flat_min_exptime),
                                    flat_min_exptime=flat_min_exptime):
            t_query2 = filter_calib(t_query, date=date, keep='last',
                                    flat_min_exptime=flat_min_exptime)
            calib_failed = False
        elif check_calib(filter_calib(t_query, date=date, keep='first',
                                    flat_min_exptime=flat_min_exptime),
                                    flat_min_exptime=flat_min_exptime):
            t_query2 = filter_calib(t_query, date=date, keep='first',
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


def distribute_files(id_list, id2nights,
                     src_dir,
                     calib_dir, science_dir,
                     science_ids=[], fileending='.fits.Z'):
    '''Distribute the downloaded files to the output folder. If you provide
    the science ids, the science files will be copied in
    a different folder than the calib files.'''
    missing_files = []
    science_files = []
    outdir = {'calibfile': calib_dir,
              'sciencefile': science_dir}
    for iid in id_list:
        fpath = os.path.join(src_dir, iid + fileending)
        if iid in science_ids:
            filetype = 'sciencefile'
            pnouts = [os.path.join(outdir[filetype], tnight.iso[:10].replace("-", ""),
                                   iid+fileending) for tnight in id2nights[iid]]
        else:
            filetype = 'calibfile'
            pnouts = [os.path.join(outdir[filetype], tnight.iso[:10].replace("-", ""),
                                 iid+fileending) for tnight in id2nights[iid]]
        for pnout in pnouts:
            if not (os.path.isfile(pnout) or os.path.isfile(pnout[:-2])):
                if filetype == 'sciencefile':
                    science_files.append(pnout.replace(".fits.Z", ".fits"))
                try:
                    copyfile(fpath, pnout)
                except IOError:
                    if filetype == 'sciencefile':
                        print('Somehow {} {} is missing (protected file?) - or {} cannot be \
written. Trying downloading it later again.'.format(filetype, fpath, pnout))
                        missing_files.append(iid)
                    else:
                        warn('{} {} could not be downloaded. Even though it is not a \
potentially protected sciencefile')
    return missing_files, science_files



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
    # filelist = [yy for x in os.walk(direct)
    #             for yy in glob(os.path.join(x[0], '*.fits.Z'))]
    filelist = []
    for root, dirs, files in os.walk(direct):
        for ff in files:
            if ff.endswith('.fits.Z'):
                filelist.append(os.path.join(root, ff))

    print('Uncompressing the {} files in {}'.format(len(filelist), direct))
    for ifile in filelist:
        # if the target file does exist, ask
        if os.path.isfile(ifile[:-2]):
            if overwrite_old not in [True, False]:
                overwrite_old = input(
                    "File %s does exist. Overwrite all existing files? Type 'y' or get asked for each file:" % (ifile[:-2]))
                if overwrite_old in ['y', 'Y', 'j', 'J', 't', 'T', 'True', True]:
                    overwrite_old = True
                else:
                    overwrite_old = 'ask'
            if overwrite_old:
                os.remove(ifile[:-2])
            if not overwrite_old:
                continue
        call(["uncompress", ifile])
    return overwrite_old


def longest_sequence_idz(array, key):
    '''Return the indices of the longest reoccuring time of key. E.g.
    key = 0:
    [0, 3, 2, 0, 0, 0, 3] returns [3, 4, 5].
    Return [] if key not in array array'''
    if key in array:
        pos, max_len, cum_pos = 0, 0, 0
        for k, g in groupby(array):
            if k == key:
                pat_size = len(list(g))
                pos, max_len = (pos, max_len) if pat_size < max_len else (cum_pos, pat_size)
                cum_pos += pat_size
            else:
                cum_pos += len(list(g))
        return list(range(pos, pos+max_len))
    else:
        return []
