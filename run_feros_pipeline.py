import os
import numpy as np
import astropy.units as u
from astropy.table import Table, vstack
import re
import warnings
from glob import glob
from misc import find_night
from starclass import Star
from subprocess import Popen, PIPE
from astropy.io import fits
from shutil import copy
from tqdm import tqdm
from config import ceres_dir, default_science_dir, default_calib_dir, \
    default_log_dir


if __name__ == '__main__':
    print('Processing all files in current directory. Assuming they are \
sorted by targetname and date')
    all_targets()

    

def show_pdfs(target, prog=None,
              science_dir=None,):
    if science_dir is  None:
        science_dir = default_science_dir
    # direct = os.path.join(default_science_dir, target)
    pdf_files = []
    pdf_files = sorted(glob(os.path.join(science_dir, '**/FEROS*{}*.pdf'.format(target)),
                                  recursive=True))

    if prog is not None:
        for pdf in pdf_files:
            Popen([prog, pdf])
    return pdf_files


def _all_targets(science_dir=None, npools=10,
                do_class=False, extra_calib_dir=False,
                calib_dir=None, ignore_dirs=[],
                only_dirs=[]):
    '''Running CERES-FEROS pipeline on all subfolders. Assuming the folders are sorted
    by Target only, e.g. ./direct/HD10000/sciencefiles.fits - unless extra_calib_dir=True
    Use all_subfolders routine of the same module to reduce files also sorted by
    date.
    do_class=False
    Set to true to do the classification for Teff and other stellar parameters CERES
    provides
    extra_calib_dir = True
    An extra calibdirectory exists, it is assumed the calibdirectory is sorted
    by date only, e.g. ./calibdir/311220000/calibfiles.fits
    The calibfiles are then copied to the sciencefiles, processed and deleted again to
    save memory
    ignore_dirs=[] and only_dirs=[] can be used to reduce only certain folders'''
    if science_dir is None:
        science_dir = default_science_dir
    if extra_calib_dir and calib_dir is None:
        calib_dir = default_calib_dir
    elif calib_dir is None:
        calib_dir = science_dir

    home_dir = os.getcwd()
    for tardir in tqdm(os.scandir(science_dir)):
        if tardir.is_dir() and not tardir.name.endswith('red') and not \
           tardir.name in ignore_dirs:
            if only_dirs != [] and tardir not in only_dirs:
                print('{} is not specified in only_dirs. Ignoring it')
                continue
            tarname = tardir.name

            scfiles = [ffile for ffile in os.scandir(tardir) if (
                ffile.name.endswith('.fits') and
                ffile.name.startswith('FEROS'))]
            if len(scfiles) == 0:
                print('Didnt find any observations for {}. Skipping it'.format(tarname))
                continue
            nights = [find_night(fits.getheader(ffile.path)['MJD-OBS'])
                      for ffile in scfiles]
            nights = np.unique(nights)
            print('Processing target {} with {} different nights'.format(tarname, len(nights)))
            # store the calibfiles which are copied later and then deleted if extra_calib_dir
            calibfiles = []
            for night in nights:
                # store the calibfiles which are copied later and then deleted if extra_calib_dir
                if extra_calib_dir:
                    calibfiles.append([cfile for cfile in os.scandir(
                        os.path.join(calib_dir, night)) if (
                                  cfile.is_file() and cfile.name.endswith('.fits'))])
                    if not len(calibfiles[-1]) >= 12:
                        print('Did not find calibfiles for night {}. Skipping it'.format(
                            night))
                        continue
                    for calibfile in calibfiles[-1]:
                        if not os.path.exists(os.path.join(tardir, calibfile.name)):
                            copy(calibfile, tardir)
            fitsfiles = [ffile.name for ffile in os.scandir(tardir) if (
                    ffile.name.endswith('.fits') and ffile.is_file())
                         and ffile.name.startswith('FEROS')]
            _make_reffile(tarname, tardir, fitsfiles)
            os.chdir(ceres_dir)

            _run_ferospipe(do_class=do_class, root=tardir.path,
                           npools=npools)
            os.chdir(home_dir)
#            if extra_calib_dir:
#                print('Cleaning the calibdata...')
#                for calibfile in [ff for nightlist in calibfiles for ff in nightlist]:
#                    os.remove(calibfile.path)

        
def all_subfolders(direct=None, npools=4,
                   do_class=False, show_pdfs=False,
                   pnreffile=None,
                   log_dir=None):
    '''runs the feros ceres pipeline on all subfolders of the directory which have
    at least 16 .fits files in it and do not end on _red. Use this routine if your
    files are sorted by date only.
    pnreffile=None
    provied the path to the reffile for CERES. If not provided, will try to find the
    logs from the download sequence to identify the targets to decide which mask to 
    use.'''
    if direct is None:
        direct = os.path.abspath(default_science_dir)
    if log_dir is None:
        log_dir = os.path.abspath(default_log_dir)
    if pnreffile is None:
        pnreffle = os.path.abspath(os.path.join(log_dir, 'reffile.txt'))
    else:
        pnreffile = os.path.abspath(pnreffile)
    home_dir = os.getcwd()
    os.chdir(ceres_dir)
    for root, subdirs, files in tqdm(os.walk(direct)):
        fits_files = [x for x in files if x.endswith('.fits')]
        # at least 5+10+12(6) calibration files + 1 science
        if len(fits_files) >= 16 and not root.endswith('_red') and not root.endswith('proc'):

            print('\n\
                   #######################################################\n\
                   Processing dir %s containing %d fits-files\n\
                   #######################################################\n'%(root,len(fits_files)))
                  

            _write_tarnames2header_and_reffile(root, log_dir)
            _run_ferospipe(do_class=do_class, root=root, npools=npools,
                           pnreffile=pnreffile)
        else:
            print('\n Ignoring folder {} as it has less than 16 files, is "proc" or ends with "_red"'.format(
                root))

    os.chdir(home_dir)
    print('Done reducing all subfolders in %s (successfully?)'%direct)
    if show_pdfs:
        print('Opening the pdfs')
        show_pdfs(direct)


def check_fits_files(check_dir=os.getcwd(), recursive=True):
    '''Some fits files get downloaded wrongly. Check recursively. 
    Check the checksums
    and notify those not openable or with wrong checksum. Also store
    their folder so they can be redownloaded'''
    if os.path.isfile(check_dir):
        ffiles = [ffiles, ]
    else:
        if recursive:
            ffiles = glob(os.path.join(check_dir, '**/*.fits'),
                          recursive=True)
        else:
            ffiles = glob(os.path.join(check_dir, '*.fits'))
        ffiles = sorted(ffiles)
    failsizes = []
    failfiles = []
    failreasons = []
    
    print('Checking {} files in {}'.format(len(ffiles), check_dir))
    with warnings.catch_warnings():
        warnings.filterwarnings('error')
        for ffile in tqdm(ffiles):
            try:
                hdul = fits.open(ffile, checksum=True)
                hdul.close()
                continue
            except Warning:
                print('ChecksumError found')
                failreasons.append('checksumError')
            except:
                failreasons.append('notOpenable')
            failsizes.append(os.path.getsize(ffile))
            failfiles.append(ffile)
            hdul.close()
    tfailed = Table([failreasons, failsizes, failfiles],
                    names=('reason', 'size', 'path'))
    tfailed['size'].unit = u.byte
    return tfailed


def _run_ferospipe(do_class=False, root=os.getcwd(),
                   npools=4, pnreffile=None):
    if pnreffile is None:
        pnreffile=os.path.join(root, 'reffile.txt')
    predir = os.getcwd()
    print('Running pipeline on folder {}'.format(root))
    os.chdir(os.path.join(ceres_dir, 'feros'))
    if os.path.exists("ferospipe_fp.py"):
        pipeline = "ferospipe_fp.py"
    else:
        pipeline = "ferospipe.py"
    if do_class:

        p = Popen(["python2", pipeline, root,
                   "-npools", str(npools), "-do_class",
                   "-reffile", pnreffile],
                  stdin=PIPE, stdout=PIPE, stderr=PIPE)
    else:
        p = Popen(["python2", pipeline, root, "-npools",
                   str(npools), 
                   "-reffile", os.path.join(root, 'reffile.txt')],
                  stdin=PIPE, stdout=PIPE, stderr=PIPE)
    os.chdir(predir)
    # for stdout_line in iter(p.stdout.readline, ""):
        #print(stdout_line)
    output, err = p.communicate()
    output = output.decode('utf-8')
    err = err.decode('utf-8')
    print(str(output), str(err))
    if not os.path.exists(root+'_red'):
        os.mkdir(root+'_red')
    fnoutput = root+'_red/output.txt'
    with open(fnoutput, 'w+') as outputfile:
        outputfile.write(str(output)+'\n'+'Erroroutput:\n'+str(err))
    #rc = p.returncode
    p.stdout.close()
    regex = re.compile('Achievable RV precision is\s+[0-9]*\.[0-9]+')
    rvs = [x[26::].strip() for x in regex.findall(output)]
    fnrv = root+'_red/achievable_rvs.txt'
    rvfile = open(fnrv,'w+')
    for entry in rvs:
        rvfile.write("%s\n"%entry)
    rvfile.close()
    print('achieveable rvs: %s(saved in %s):'%(rvs, fnrv))

    
def _make_reffile(root, fits_files, science_dir=None, tarname=None):
    '''A routine to make the reffile for the CERES pipeline. The coordinates are
    used from the header, the mask is determined via SIMBAD. If not found,
    G2 is used.'''
    if science_dir is None:
        science_dir = default_science_dir
    science_fits = [os.path.join(root, ff) for ff in fits_files if
                    str(fits.getheader(
                        os.path.join(root,
                                     ff))['ESO DPR CATG']).upper().strip()
                    == 'SCIENCE']
    # targets = []
    # if there is no fscience file found and the number corresponds to a
    # possible calib number, dont process this folder

    if (len(science_fits) == 0) and (len(fits_files) == 27 or
                                     len(fits_files) == 21):
        warnings.warn('Ignoring folder {} as there seem to be no science \
files.'.format(root))
        with open(os.path.join(science_dir, 'unreduced_folders.txt'), 'a+') as f:
            f.write(root+'\n')
    else:
        starget = Star(tarname)
        if len(science_fits) == 0:
            warnings.warn('No fits file found for target {}. Using coordinates also from Simbad'.format(tarname))
            starget.coordinates = 'auto'
        else:
            for st in science_fits:
                headername = str(fits.getheader(st)['OBJECT'].upper().strip())
                if headername != tarname:
                    warnings.warn('Name of target ({}) is {} in header of file {}. \
                    Continuing using name {} and changing it in header for CERES to work'.format(
                        tarname, headername, st, tarname))
                    dat, head = fits.getdata(st, header=True)
                    head['OBJECT'] = tarname
                    fits.writeto(st, dat, header=head, overwrite=True,
                                 output_verify='warn')
            stheader = fits.getheader(science_fits[0])
            starget.coordinates = [[stheader['RA'], stheader['DEC']],
                                   [u.deg, u.deg], 'icrs']
        starget.pm = 'auto'
        #except:
        #    print('Couldnt get pm for {}. Setting it to 0'.format(starget.sname))
        #    starget.pm = [0., 0.] * u.mas/u.yr
        try:
            starget.get_SpT()
            assert np.isfinite(starget.SpT_num())  # try it can be converted to num value
            print('Found SpT for {} on Simbad as {}'.format(starget.sname,
                                                            starget.SpT))
        except:
            print('Couldnt find SpT on Simbad for {}. \
    Setting it to G2III'.format(starget.sname))
            starget.SpT = ('G2V')
        # determine which of the three masks to use. 05III has numerical val 15
        print('Using mask {}'.format(mask))
        with open(os.path.join(root, 'reffile.txt'), 'a') as reff:
            reff.write('{},{},{},{},{},{},{},{}\n'.format(
                starget.sname,
                starget.coordinates.ra.to_string(u.hourangle, sep=':'),
                starget.coordinates.dec.to_string(u.degree, sep=':'),
                starget.pm[0].to('mas / yr').value,
                starget.pm[1].to('mas / yr').value,
                0,
                mask,
                10.0))


def _write_tarnames2header_and_reffile(direct, log_dir):
    '''Find the tarnames of the files. They should be stored in a file created by the
    download routine.'''
    sciencefiles = [os.path.abspath(os.path.join(direct, f)) for f in os.listdir(direct) \
                    if f.endswith('.fits') and \
                     fits.getheader(os.path.join(direct, f))['ESO DPR CATG'].strip().upper() \
                    == 'SCIENCE']

    pndownloadlog = os.path.join(log_dir, 'science_files.csv')
    if not os.path.exists(pndownloadlog):
        raise FileNotFoundError('File {} not found with the queried targetnames. But it is \
needed to identify the different sciencefiles.'.format(pndownloadlog))
    tdownloadlog = Table.read(os.path.join(log_dir, 'science_files.csv'), delimiter=',',
                                          names=['tarnamequery', 'abspath', 'querydate'])
    for sf in sciencefiles:
        if sf in tdownloadlog['abspath']:
            # update headername
            tarnamefits = fits.getheader(sf)['OBJECT']
            tarnamequery = tdownloadlog['tarnamequery'][tdownloadlog['abspath']==sf][0]
            if tarnamefits != tarnamequery:
                print('Replacing header name {} by queryname {}'.format(
                    tarnamefits, tarnamequery))
                with fits.open(sf, mode='update') as hdul:
                     hdul[0].header['OBJECT'] = tarnamequery
                     hdul.flush()
            # write reffile
            _update_reffile(os.path.join(log_dir, 'reffile.txt'), tarnamequery)
            print('Found sciencefile for target {}'.format(tarnamequery))
            
        else:
            warnings.warn('Unknown sciencefile {}. Letting CERES find parameters'.format(sf))


def _update_reffile(pnreffile, tarname):
    reffilecolnames = ['tarname', 'RA', 'DEC', 'pmRA', 'pmDEC', 'userefcoords',
                                     'mask', 'vsini']

    if os.path.exists(pnreffile):
        treffile = Table.read(pnreffile, delimiter=',', format='ascii.no_header',
                              names=reffilecolnames,
                              )
    else:
        treffile = Table([[]]*8, names=reffilecolnames,)
    if tarname in treffile['tarname']:
        pass
    else:
        try:
            star = Star(tarname)
            star.pm = 'auto'
            star.coordinates = 'auto'
            star.get_SpT()
            if star.SpT_num() >= 70:  # Mstar
                mask = 'M2'
            elif star.SpT_num() >= 60:  # Kstar
                mask = 'K5'
            else:  # the default mask
                mask = 'G2'
            print('Using mask {} for {}'.format(mask, star.sname))
            with open(pnreffile, 'a') as reff:
                reff.write('{},{},{},{},{},{},{},{}\n'.format(
                    star.sname,
                    star.coordinates.ra.to_string(u.hourangle, sep=':')[0],
                    star.coordinates.dec.to_string(u.degree, sep=':')[0],
                    star.pm[0].to('mas / yr').value,
                    star.pm[1].to('mas / yr').value,
                    1,
                    mask,
                    10.0 ,))
                
        except:
            import ipdb; ipdb.set_trace()
            warning.warn('Couldnt automatically find parameters for {}'.format(tarname))

