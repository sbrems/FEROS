import os
#import numpy as np
from subprocess import call

import ipdb

par_dir = "/disk1/brems/FEROS/raw/" #parent folder to target folder
ceres_dir = "/disk1/brems/ceres/feros/" #where to run ceres from

def show_pdfs(target,prog="xdg-open"):
    direct = par_dir+target+'/'
    pdf_files = []
    for root, subdirs, files in os.walk(direct):
        pdf_files += [root+'/'+x for x in files if x.endswith('.pdf')]
    for pdf in pdf_files:
        call([prog,pdf])

def all_subfolders(target,direct=None,npools=10,do_class=True,show_pdfs=False):
    '''runs the feros ceres pipeline on all subfolders of the directory which have
    at least 28 .fits files in it'''
    if direct == None:
        direct = par_dir+target+'/'
    home_dir = os.getcwd()
    os.chdir(ceres_dir)
    for root,subdirs,files in os.walk(direct):
        fits_files = [x for x in files if x.endswith('.fits')]
        if len(fits_files) >= 22 :#at least 5+10+12(6) calibration files + 1 science
            print('#######################################################\n\
                   Processing dir %s containing %d fits-files\n\
                   #######################################################'%(root,len(fits_files)))
            if do_class:
                call(["python","ferospipe.py",root,"-npools",str(npools),"-do_class"])
            else:
                call(["python","ferospipe.py",root,"-npools",str(npools)])
    os.chdir(home_dir)
    print('Done reducing all subfolders in %d (successfully?)'%direct)
    if show_pdfs: 
        print('Opening the pdfs')
        show_pdfs(target)
    ipdb.set_trace()
