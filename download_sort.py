import numpy as np
import os
import pandas as pd
from astroquery.eso import Eso
from subprocess import call
#from astropy.io import fits
from astropy.time import Time
from shutil import copyfile
from glob import glob


eso = Eso()
fn_query = "last_output_query.csv" #file of the query result.e.g. which files are gonna be downloaded
home_dir = "/disk1/brems/FEROS/"
par_dir = home_dir+"raw/"
astroquery_dir = "/home/brems/.astropy/cache/astroquery/Eso/"
eso_user= "sbrems"
#store_pwd = False
flat_min_exptime = 1. #in sec

def query_eso(target,instrument='FEROS',category='SCIENCE',
              sdate="",edate="",maxrows=999999):
    call(["wget","-O",fn_query,"http://archive.eso.org/wdb/wdb/eso/eso_archive_main/query?tab_object=on&target="+target.replace('+','%2B')+"&resolver=simbad&tab_target_coord=on&ra=&dec=&box=00+10+00&deg_or_hour=hours&format=SexaHours&tab_prog_id=on&prog_id=&tab_instrument=on&instrument=+"+instrument+"+&stime="+sdate+"&starttime=12&etime="+edate+"&endtime=12&tab_dp_cat=true&dp_cat="+category+"&top="+str(maxrows)+"&wdbo=csv"])
    table = pd.read_csv(fn_query,comment='#',sep=',',skip_blank_lines=True)
    if category =='SCIENCE':
        table = table.loc[lambda x:x.Airmass >= 0.] #this is to remove any None lines the query sometimes returns
    return table

def check_calib(table):
    if ((len(table[table.Type == 'BIAS']) ==5 )&\
        (len(table[(table.Type == 'FLAT') & (table.Exptime >= flat_min_exptime)  ]) ==10)&\
        ((len(table[table.Type == 'WAVE']) ==12) | (len(table[table.Type == 'WAVE']) == 6))):
        return True
    else:
        return False

def download_id(ids,store_pwd=False):
    logged_in = False
    while not logged_in:
        logged_in = eso.login(eso_user,store_password=store_pwd)
    print('ESO is getting the archive files. This may take some time! Be patient ;)')
    return eso.retrieve_data(ids)

def get_calib(date,check_manually):
    date = date.replace(" ","-")
    edate = Time(Time(date).jd+1,format='jd').iso[0:10]
    
    t_query = query_eso("",category="CALIB",sdate=date,edate=edate)
    #remove non-technical time
    t_query = t_query[t_query.Program_ID.str.startswith('60.A-')]
    #try the first half of the night
    if check_calib(t_query[(t_query['TPL START'].str.contains(date))]):
        t_query2 = t_query[(t_query['TPL START'].str.contains(date))]
        down_ids = t_query2[(t_query2.Type=='BIAS') | \
                            (t_query2.Type=='WAVE') | \
                            ((t_query2.Type=='FLAT') & (t_query2.Exptime >= flat_min_exptime))]['Dataset ID'].tolist()
    #try second half of the night
    elif check_calib(t_query[(t_query['TPL START'].str.contains(edate))]):
        t_query2 = t_query[(t_query['TPL START'].str.contains(edate))]
        down_ids = t_query2[(t_query2.Type=='BIAS') | \
                            (t_query2.Type=='WAVE') | \
                            ((t_query2.Type=='FLAT') & (t_query2.Exptime >= flat_min_exptime))]['Dataset ID'].tolist()
    else: 
        down_ids = []
        print('Cant find calib files automatically for date ', date)
        check_manually.append(date)
#    if len(down_ids) >= 1:
#        download_id(down_ids)
    return down_ids,check_manually

def find_night(dtime):
    dtime = Time(dtime)
    if (dtime.mjd%1) >= 0.5: #evening
        return dtime.iso[0:10]
    else: #morning. previous day
        return Time(dtime.jd-1,format='jd').iso[0:10]
def distribute_files(id_list,id2night,target,src_dir=None,target_dir=None,fileending='.fits.Z'):
    '''Distribute the downloaded files to the output folder'''
    missing_files = []
    if src_dir == None:
        src_dir = astroquery_dir
    if target_dir == None:
        target_dir =  par_dir+target+'/'
    for iid in id_list:
        fpath = src_dir+iid+fileending
        try:
            copyfile(fpath,target_dir+ id2night[iid].replace("-","") +'/'+ iid+fileending)
        except:
            print('Somehow file %s is missing. Trying downloading it later again.' %fpath)
            missing_files.append(iid)
    return missing_files

def extract_files(direct=None,overwrite_old = None):
    '''decompressing all .fits.Z files in the directory+subdirectories'''
    if direct == None:
        direct = par_dir
    filelist = [yy for x in os.walk(direct) for yy in glob(os.path.join(x[0], '*.fits.Z'))]
    print('Uncompressing the %d files' %len(filelist))
    for ifile in filelist:
        if os.path.isfile(ifile[:-2]) and overwrite_old == None:
            overwrite_old = input("File %s does exist. Overwrite all existing files? Type 'y' or get asked for each file:"%(ifile[:-2]))
        if os.path.isfile(ifile[:-2]) and overwrite_old.lower() == 'y':
            os.remove(ifile[:-2])
        call(["uncompress",ifile])

def full_download(target,extract=True,store_pwd=False,overwrite_old=None,clear_cache=False):
    '''Main function. Run this to get all FEROS science files and the corresponding caibration
    files for each night (5 BIAS, 10 flats, 12 wave calib). If there is anything off this standard
    calibration, no calib files are downloaded and the corresponding nights are stored in a file
    called failed_calib_<target>.txt. So check this out manually then.'''
    t_science = query_eso(target,category='SCIENCE')
    #find the different date. Look for morning/evening and adjust date!
    nights = []
    failed_calib = []
    missing_downloads = [] #for some reason ESO misses some downloads. 
    #Even if theyre in the confirmation mail
    id2night = {}
    for dtime in t_science['TPL START']:
        nights.append(find_night(dtime))
    nights = np.unique(nights)
    print('Found %d science obs in %d different nights.' %(len(t_science),len(nights)) )
#    print('Getting calib frames for %d nights' %(len(nights)))
    if not os.path.exists(par_dir+target):
        os.mkdir(par_dir+target)
    for night in nights:
        ddir = par_dir+target+'/'+night.replace("-","")+'/'
        if not os.path.exists(ddir):
            os.mkdir(ddir)
#        os.chdir(ddir)
        ids,failed_calib = get_calib(night,failed_calib)
        for obsid in ids:
            id2night[obsid]=night
#    os.chdir(home_dir)
#    print('Downloading %d science obs'  %(len(t_sciene)) )
    for ii in t_science.index:
        obsid = t_science['Dataset ID'][ii]
        date =  t_science['TPL START'][ii]
        night = find_night(date)
        id2night[obsid]=night
#        os.chdir(par_dir+target+'/'+night.replace("-","")+'/')
#        download_id(obsid)
#    os.chdir(home_dir)
    print('Downloading the %d files for target %s' %(len(id2night.keys()),target) )
    downloaded = download_id(id2night.keys(),store_pwd=store_pwd)#gives the path+fn of the files
    fn_failed = par_dir+'failed_calib_'+target+'.txt'
    f_failed = open(fn_failed,'w')
    for failed_night in failed_calib:
        f_failed.write("%s\n" %(failed_night))
    f_failed.close()
    print('Moving files to the appropriate directories')
    #for fpath in downloaded:
    missing_downloads = distribute_files(id2night.keys(),id2night,target)
        
    while (len(missing_downloads) >= 1):
        old_len_missing = len(missing_downloads)
        print('%d files got lost on the way. Try to redownload them...'%(old_len_missing))
        download2 = download_id(missing_downloads,store_pwd=store_pwd)
        missing_downloads = distribute_files(missing_downloads,id2night,target)
        if old_len_missing == len(missing_downloads):
            fn_failed_down = par_dir+'failed_download_'+target+'.txt'
            f_failed_down = open(fn_failed_down,'w')
            for failed_down in missing_downloads:
                f_failed_down.write("%s\n" %(failed_down))
            f_failed_down.close()
            print('Could not download %d files. Please download them manually.You find them in %s'\
                  %(old_len_missing,fn_failed_down))
            break
    print('Done downloading %d files. Had problems with %d nights (stored in %s)'\
          %(len(id2night.keys()),len(failed_calib),fn_failed))
    if extract: extract_files(direct=par_dir+target+'/',overwrite_old=overwrite_old)
    if clear_cache:
        cachefiles = os.listdir(astroquery_dir)
        for cfile in cachefiles:
            os.remove(astroquery_dir+cfile)
    print('Done with all :)')

        
