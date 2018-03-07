import os
import numpy as np
import pandas as pd
from astroquery.eso import Eso
from subprocess import call
# from astropy.io import fits
from astropy.time import Time
from shutil import copyfile
from glob import glob

eso = Eso()
astroquery_dir = '/disk1/brems/astroquery_cache/'
eso.cache_location = astroquery_dir
# file of the query result.e.g. which files are gonna be downloaded
fn_query = "last_output_query.csv"
home_dir = "/disk1/brems/FEROS/"
par_dir = os.path.join(home_dir, "raw_A2F")
eso_user = "sbrems"
# store_pwd = False
flat_min_exptime = 1.  # in sec


def query_eso(target, instrument='FEROS', category='SCIENCE',
              sdate="", edate="", maxrows=999999):
    call(["wget", "-O", fn_query, "http://archive.eso.org/wdb/wdb/eso/eso_archive_main/query?tab_object=on&target=" + target.replace('+', '%2B') + "&resolver=simbad&tab_target_coord=on&ra=&dec=&box=00+08+00&deg_or_hour=hours&format=SexaHours&tab_prog_id=on&prog_id=&tab_instrument=on&instrument=+" +
          instrument + "+&stime=" + sdate + "&starttime=12&etime=" + edate + "&endtime=12&tab_dp_cat=true&dp_cat=" + category + "&top=" + str(maxrows) + "&wdbo=csv"])
    try:
        table = pd.read_csv(fn_query, comment='#', sep=',',
                            skip_blank_lines=True)
    except:
        # make an empty table if the file was empty
        table = pd.DataFrame(columns=['OBJECT', 'RA', 'DEC',
                                      'Program_ID', 'Instrument',
                                      'Category', 'Type', 'Mode',
                                      'Dataset'  'ID', 'Release_Date',
                                      'TPL'  'ID', 'TPL'  'START',
                                      'Exptime', 'Filter',
                                      'MJD-OBS', 'Airmass'])
    if category == 'SCIENCE':
        # this is to remove any None lines the query sometimes returns. Dont check fo absolute values as airmass is not given for observations before 2003
        table = table.loc[lambda x:x.Airmass != 0.]
    return table


def filter_calib(table, date=None, keep=None):
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


def check_calib(table):
    if ((len(table[table.Type == 'BIAS']) == 5) &
        (len(table[(table.Type == 'FLAT') & (table.Exptime >= flat_min_exptime)]) == 10) &
            ((len(table[table.Type == 'WAVE']) in [6, 12]))):
        return True
    else:
        return False


def download_id(ids, store_pwd=False):
    # make sure ids is a list to not confuse eso and make it not too long
    maxlength = 500
    ids = [ii for ii in ids]
    ids = [ids[ii:ii + maxlength] for ii in range(0, len(ids), maxlength)]
    logged_in = False
    while logged_in is not True:
        try:
            logged_in = eso.login(eso_user, store_password=store_pwd)
        except ValueError:
            logged_in = True
        
    for iid in ids:
        iid = iid
        print('ESO is getting the archive files ({} files) . \
This may take some time! Be patient ;) It might have been \
split up into smaller chunks.'.format(len(iid)))
        eso.retrieve_data(iid)
        # except:
        #    logged_in = False
        #    print('Could not log in or download the data. Spwaning an ipdb')
        #    import ipdb
        #    ipdb.set_trace()


def get_calib(date, check_manually):
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
        if check_calib(filter_calib(t_query, date=date, keep='first')):
            t_query2 = filter_calib(t_query, date=date, keep='first')
            calib_failed = False
        elif check_calib(filter_calib(t_query, date=date, keep='last')):
            t_query2 = filter_calib(t_query, date=date, keep='last')
            calib_failed = False
        elif check_calib(filter_calib(t_query, date=edate, keep='first')):
            t_query2 = filter_calib(t_query, date=edate, keep='first')
            calib_failed = False
        elif check_calib(filter_calib(t_query, date=edate, keep='last')):
            t_query2 = filter_calib(t_query, date=edate, keep='last')
            calib_failed = False

        if not calib_failed:
            down_ids = t_query2['Dataset ID'].tolist()
    if calib_failed:
        down_ids = []
        print('Cant find calib files automatically for date ', date)
        check_manually.append(date)
#    if len(down_ids) >= 1:
#        download_id(down_ids)
    return down_ids, check_manually


def find_night(dtime):
    dtime = Time(dtime, format='mjd')
    if (dtime.mjd % 1) >= 0.5:  # evening
        return dtime.iso[0:10]
    else:  # morning. previous day
        return Time(dtime.jd - 1, format='jd').iso[0:10]


def distribute_files(id_list, id2night, target, src_dir=None,
                     target_dir=None, fileending='.fits.Z'):
    '''Distribute the downloaded files to the output folder'''
    missing_files = []
    if src_dir is None:
        src_dir = astroquery_dir
    if target_dir is None:
        target_dir = os.path.join(par_dir, target)
    for iid in id_list:
        fpath = os.path.join(src_dir, iid + fileending)
        try:
            copyfile(fpath,
                     os.path.join(target_dir, id2night[iid].replace("-", ""),
                                  iid + fileending))
        except:
            print(
                'Somehow file %s is missing. Trying downloading it later again.' % fpath)
            missing_files.append(iid)
    return missing_files


def extract_files(direct=None, overwrite_old=None):
    '''decompressing all .fits.Z files in the directory+subdirectories'''
    if direct is None:
        direct = par_dir
    filelist = [yy for x in os.walk(direct)
                for yy in glob(os.path.join(x[0], '*.fits.Z'))]
    print('Uncompressing the %d files' % len(filelist))
    for ifile in filelist:
        if os.path.isfile(ifile[:-2]) and overwrite_old == None:
            overwrite_old = input(
                "File %s does exist. Overwrite all existing files? Type 'y' or get asked for each file:" % (ifile[:-2]))
        if os.path.isfile(ifile[:-2]) and overwrite_old.lower() == 'y':
            os.remove(ifile[:-2])
        call(["uncompress", ifile])


def full_download(target, extract=True, store_pwd=False,
                  overwrite_old=None, clear_cache=False):
    '''Main function. Run this to get all FEROS science files and the corresponding caibration
    files for each night (5 BIAS, 10 flats, 12 wave calib). If there is anything off this standard
    calibration, no calib files are downloaded and the corresponding nights are stored in a file
    called failed_calib_<target>.txt. So check this out manually then.'''
    t_science = query_eso(target, category='SCIENCE')
    # find the different date. Look for morning/evening and adjust date!
    nights = []
    failed_calib = []
    missing_downloads = []  # for some reason ESO misses some downloads.
    # Even if theyre in the confirmation mail
    id2night = {}
    for dtime in t_science['MJD-OBS']:
        nights.append(find_night(dtime))
    nights = np.unique(nights)
    print('Found %d science obs in %d different nights.' % (len(t_science),
                                                            len(nights)))
#    print('Getting calib frames for %d nights' %(len(nights)))
    if not os.path.exists(os.path.join(par_dir, target)):
        os.mkdir(os.path.join(par_dir, target))
    for night in nights:
        ddir = os.path.join(par_dir, target, night.replace("-", ""))
        if not os.path.exists(ddir):
            os.mkdir(ddir)
#        os.chdir(ddir)
        ids, failed_calib = get_calib(night, failed_calib)
        for obsid in ids:
            id2night[obsid] = night
#    os.chdir(home_dir)
#    print('Downloading %d science obs'  %(len(t_sciene)) )
    for ii in t_science.index:
        obsid = t_science['Dataset ID'][ii]
        date = t_science['MJD-OBS'][ii]
        night = find_night(date)
        id2night[obsid] = night
#        os.chdir(par_dir+target+'/'+night.replace("-","")+'/')
#        download_id(obsid)
#    os.chdir(home_dir)
    print('Downloading the %d files for target %s' % (len(id2night.keys()),
                                                      target))
    download_id(id2night.keys(), store_pwd=store_pwd)
    print('Downloaded')
    fn_failed = os.path.join(par_dir, 'failed_calib_' + target + '.txt')
    f_failed = open(fn_failed, 'w+')
    for failed_night in failed_calib:
        f_failed.write("%s\n" % (failed_night))
    f_failed.close()
    print('Moving files to the appropriate directories')
    # for fpath in downloaded:
    missing_downloads = distribute_files(id2night.keys(),
                                         id2night, target)

    while (len(missing_downloads) >= 1):
        old_len_missing = len(missing_downloads)
        print('%d files got lost on the way. Try to redownload them...' %
              (old_len_missing))
        download2 = download_id(missing_downloads, store_pwd=store_pwd)
        missing_downloads = distribute_files(missing_downloads,
                                             id2night, target)
        if old_len_missing == len(missing_downloads):
            fn_failed_down = os.path.join(par_dir,
                                          'failed_download_' + target + '.txt')
            f_failed_down = open(fn_failed_down, 'w+')
            for failed_down in missing_downloads:
                f_failed_down.write("%s\n" % (failed_down))
            f_failed_down.close()
            print('Could not download %d files. Please download them manually. \
You find them in %s' % (old_len_missing, fn_failed_down))
            break
    print('Done downloading %d files. Had problems with %d nights (stored in %s)'
          % (len(id2night.keys()), len(failed_calib), fn_failed))
    if extract:
        extract_files(direct=os.path.join(par_dir, target),
                      overwrite_old=overwrite_old)
    if clear_cache:
        cachefiles = os.listdir(astroquery_dir)
        for cfile in cachefiles:
            os.remove(os.path.join(astroquery_dir, cfile))
    print('Done with with all for target {}  :)'.format(target))
