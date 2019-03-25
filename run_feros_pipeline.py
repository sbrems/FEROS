import os
import numpy as np
import astropy.units as u
import re
import warnings
from misc import find_night
from starclass import Star
from subprocess import Popen, PIPE
from astropy.io import fits
from shutil import copy
from tqdm import tqdm
from config import ceres_dir, default_science_dir, default_calib_dir

import ipdb


if __name__ == '__main__':
    print('Processing all files in current directory. Assuming they are \
sorted by targetname and date')
    all_targets()

    

def show_pdfs(target, prog="xdg-open",
              science_dir=None):
    if science_dir is  None:
        science_dir = default_science_dir
    direct = os.path.join(default_science_dir, target)
    pdf_files = []
    for root, subdirs, files in os.walk(direct):
        pdf_files += [os.path.join(root, x) for x in files if x.endswith('.pdf')]
    for pdf in pdf_files:
        Popen([prog, pdf])


def all_targets(science_dir=None, npools=10,
                do_class=True, extra_calib_dir=True,
                calib_dir=None):
    '''Running CERES-FEROS pipeline on all subfolders. Assuming the folders are sorted
    by Target only, e.g. ./direct/HD10000/sciencefiles.fits
    Use all_subfolders routine of the same module to reduce files also sorted by
    date.
    extra_calib_dir = True
    An extra calibdirectory exists, it is assumed the calibdirectory is sorted
    by date only, e.g. ./calibdir/311220000/calibfiles.fits
    The calibfiles are then copied to the sciencefiles, processed and deleted again to
    save memory'''
    if science_dir is None:
        science_dir = default_science_dir
    if extra_calib_dir and calib_dir is None:
            calib_dir = default_calib_dir
    home_dir = os.getcwd()
    for tardir in tqdm(os.scandir(science_dir)):
        if tardir.is_dir() and not tardir.name.endswith('red'):
            tarname = tardir.name
            # already processed files...delete later!
            if tarname in ['HD106906', 'HD108874', 'HD108904', 'HD109085',
                           'HD111103', 'HD111170', 'HD118972',
                           'HD128311', 'HD130322',
                           'HD140374', 'HD131511',
                           'HD146897', 'HD16673', 'HD168746',
                           'HD170773', 'HD180134', 'HD187897', 'HD199532',
                           'HD202917', 'HD203',
                           'HD206893', 'HD209253', 'HD30447',
                           'HD35114', 'HD3296', 'HD38397',
                           'HD38949', 'HD40136', 'HD48370',
                           'HD52265', 'HD53143', 'HD55052',
                           'HD59967', 'HD72687',
                           'HD74340', 'HD76653',
                           'HD84075', 'HD870', 'HD93932',
                           'MML36', 'MML43', 'SAO150676', ]:
                print('SKIPPING {} AS MANUALLY SELECTED'.format(tarname))
                continue
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
                    if len(calibfiles[-1]) not in [21, 27]:
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
            _run_ferospip(do_class=do_class, root=tardir.path,
                          npools=npools)
            os.chdir(home_dir)
            if extra_calib_dir:
                print('Cleaning the calibdata...')
                for calibfile in [ff for nightlist in calibfiles for ff in nightlist]:
                    os.remove(calibfile.path)

        
def all_subfolders(target, direct=None, npools=4,
                   do_class=True, show_pdfs=False):
    '''runs the feros ceres pipeline on all subfolders of the directory which have
    at least 22 .fits files in it and do not end on _red'''
    if direct is None:
        direct = os.path.join(default_science_dir, target)
    home_dir = os.getcwd()
    os.chdir(ceres_dir)
    for root, subdirs, files in os.walk(direct):
        fits_files = [x for x in files if x.endswith('.fits')]
        if len(fits_files) >= 22 and not root.endswith('_red'):  # at least 5+10+12(6) calibration files + 1 science
            print('\n\
                   #######################################################\n\
                   Processing dir %s containing %d fits-files\n\
                   #######################################################\n'%(root,len(fits_files)))
            tarname = os.path.split(os.path.dirname(root))[-1]
            _make_reffile(tarname, root, fits_files)
            _run_ferospip(do_class=do_class, root=root)
        else:
            print('Ignoring folder {} as it has less than 22 files or ends with "_red"'.format(
                root))

    os.chdir(home_dir)
    print('Done reducing all subfolders in %s (successfully?)'%direct)
    if show_pdfs:
        print('Opening the pdfs')
        show_pdfs(target)


def _run_ferospip(do_class=True, root=os.getcwd(),
                  npools=4):
    if do_class:
        p = Popen(["python2", "ferospipe.py", root,
                   "-npools", str(npools), "-do_class",
                   "-reffile", 'reffile.txt'],
                  stdin=PIPE, stdout=PIPE, stderr=PIPE)
    else:
        p = Popen(["python2", "ferospipe.py", root,"-npools",
                   str(npools), "-reffile", 'reffile.txt'],
                  stdin=PIPE, stdout=PIPE, stderr=PIPE)
    output, err = p.communicate()
    output = output.decode('utf-8')
    err = err.decode('utf-8')
    print(str(output), str(err))
    if not os.path.exists(root+'_red'):
        os.mkdir(root+'_red')
    fnoutput = root+'_red/output.txt'
    outputfile = open(fnoutput, 'w+')
    outputfile.write(str(output)+'\n'+'Erroroutput:\n'+str(err))
    outputfile.close()
    #rc = p.returncode
    regex = re.compile('Achievable RV precision is\s+[0-9]*\.[0-9]+')
    rvs = [x[26::].strip() for x in regex.findall(output)]
    fnrv = root+'_red/achievable_rvs.txt'
    rvfile = open(fnrv,'w+')
    for entry in rvs:
        rvfile.write("%s\n"%entry)
    rvfile.close()
    print('achieveable rvs: %s(saved in %s):'%(rvs, fnrv))

    
def _make_reffile(tarname, root, fits_files, science_dir=None):
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
        starget = Star(tarname)
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
            starget.SpT = ('G2III')
        # determine which of the three masks to use. 05III has numerical val 15.
        if starget.SpT_num() >= 70:  # Mstar
            mask = 'M2'
        elif starget.SpT_num() >= 60:  # Kstar
            mask = 'K5'
        else:  # the default mask
            mask = 'G2'
        print('Using mask {}'.format(mask))
        with open(os.path.join(root, 'reffile.txt'), 'w') as reff:
            reff.write('{}, {}, {}, {}, {}, {}, {}, {}'.format(
                starget.sname,
                ':'.join([str(cc) for cc in starget.coordinates.ra.hms[0:3]]),
                ':'.join([str(cc) for cc in starget.coordinates.dec.dms[0:3]]),
                starget.pm[0].to('mas / yr').value,
                starget.pm[1].to('mas / yr').value,
                0,
                mask,
                4.0))
