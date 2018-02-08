import os
import numpy as np
from starclass import Star
from subprocess import Popen, PIPE
from astropy.io import fits
import astropy.units as u
import re
import warnings

par_dir = "/disk1/brems/FEROS/raw_A2F"  # parent folder to target folder
ceres_dir = "/disk1/brems/ceres/feros"  # where to run ceres from


def show_pdfs(target, prog="xdg-open"):
    direct = par_dir+target+'/'
    pdf_files = []
    for root, subdirs, files in os.walk(direct):
        pdf_files += [root+'/'+x for x in files if x.endswith('.pdf')]
    for pdf in pdf_files:
        Popen([prog, pdf])


def all_subfolders(target, direct=None, npools=5,
                   do_class=True, show_pdfs=False):
    '''runs the feros ceres pipeline on all subfolders of the directory which have
    at least 22 .fits files in it and do not end on _red'''
    if direct is None:
        direct = os.path.join(par_dir, target)
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

    os.chdir(home_dir)
    print('Done reducing all subfolders in %s (successfully?)'%direct)
    if show_pdfs:
        print('Opening the pdfs')
        show_pdfs(target)


def _make_reffile(tarname, root, fits_files):
    '''A routine to make the reffile for the CERES pipeline. The coordinates are
    used from the header, the mask is determined via SIMBAD. If not found,
    G2 is used.'''
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
        with open(os.path.join(par_dir, 'unreduced_folders.txt'), 'a+') as f:
            f.write(root+'\n')
    else:
        for st in science_fits:
            headername = str(fits.getheader(st)['OBJECT'].upper().strip())
            if headername != tarname:
                warnings.warn('Name of target ({}) is {} in header of file {}. \
Continuing using name {}'.format(
                    tarname, headername, st, tarname))
        stheader = fits.getheader(science_fits[0])
        starget = Star(tarname)
        starget.coordinates = [[stheader['RA'], stheader['DEC']],
                               [u.deg, u.deg], 'icrs']
        try:
            starget.pm = 'auto'
        except:
            print('Couldnt get pm for {}. Setting it to 0'.format(starget.sname))
            starget.pm = [0., 0.] * u.mas/u.yr
        try:
            starget.SpT = 'auto'
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
