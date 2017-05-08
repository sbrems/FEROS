import os
#import numpy as np
from subprocess import Popen, PIPE
import re


par_dir = "/disk1/brems/FEROS/raw/" #parent folder to target folder
ceres_dir = "/disk1/brems/ceres/feros/" #where to run ceres from

def show_pdfs(target,prog="xdg-open"):
    direct = par_dir+target+'/'
    pdf_files = []
    for root, subdirs, files in os.walk(direct):
        pdf_files += [root+'/'+x for x in files if x.endswith('.pdf')]
    for pdf in pdf_files:
        Popen([prog,pdf])

def all_subfolders(target,direct=None,npools=5,do_class=True,show_pdfs=False):
    '''runs the feros ceres pipeline on all subfolders of the directory which have
    at least 28 .fits files in it'''
    if direct == None:
        direct = par_dir+target+'/'
    home_dir = os.getcwd()
    os.chdir(ceres_dir)
    for root,subdirs,files in os.walk(direct):
        fits_files = [x for x in files if x.endswith('.fits')]
        if len(fits_files) >= 22 :#at least 5+10+12(6) calibration files + 1 science
            print('\n\
                   #######################################################\n\
                   Processing dir %s containing %d fits-files\n\
                   #######################################################\n'%(root,len(fits_files)))
            if do_class:
                p = Popen(["python","ferospipe.py",root,"-npools",str(npools),"-do_class"],\
                          stdin=PIPE,stdout=PIPE,stderr=PIPE)
            else:
                p = Popen(["python","ferospipe.py",root,"-npools",str(npools)],\
                          stdin=PIPE,stdout=PIP,stderr=PIPE)
            output, err = p.communicate()
            print(str(output),str(err))
            fnoutput = root+'_red/output.txt'
            outputfile = open(fnoutput,'w')
            outputfile.write(str(output)+'\n'+'Erroroutput:\n',str(err))
            outputfile.close()
            #rc = p.returncode
            regex = re.compile('Achievable RV precision is\s+[0-9]*\.[0-9]+')
            rvs = [x[26::].strip() for x in regex.findall(output)]
            fnrv = root+'_red/achievable_rvs.txt'
            rvfile = open(fnrv,'w')
            for entry in rvs:
                rvfile.write("%s\n"%entry)
            rvfile.close()
            print('achieveable rvs: %s(saved in %s):'%(rvs,fnrv))

            
    os.chdir(home_dir)
    print('Done reducing all subfolders in %s (successfully?)'%direct)
    if show_pdfs: 
        print('Opening the pdfs')
        show_pdfs(target)

