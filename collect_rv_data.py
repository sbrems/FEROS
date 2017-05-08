import numpy as np
import os
from astropy.table import Table,vstack
from astropy.io    import ascii
#from astropy.time import Time

pardir = '/disk1/brems/FEROS/raw/'
outdir = '/disk1/brems/FEROS/'

nceres = 18 #number of ceres outputs

def collect():
    '''Collects all the data from the FEROS reanalysis and store in a single table'''
    for direct in [pardir,outdir]:
        if not os.path.exists(direct):
            os.mkdir(direct)
    
    #first get all the folder in pardir which are the targetnames
    targets = [d for d in os.listdir(pardir) if \
               os.path.isdir(os.path.join(pardir,d))]

    tentries = ['target','night',#from folder structure
                'hname','bjd','rv','sig_rv','bisec_span','sig_bisec_span',#from ceres output
                'inst','pipeline','resolving_power',
                'Teff','logg','[Fe/H]','vsini',
                'lowest_continuum','std_gaus2ccf',
                'Texp','snr','pdfpath']
    dtypes  =  [str,np.int,
                str,np.float64,np.float64,np.float64,np.float64,np.float64,
                str,str,np.float64,
                np.float64,np.float64,np.float64,np.float64,
                np.float64,np.float64,
                np.float64,np.float64,str]
    t_obs = Table([[]]*len(tentries),names=tentries,dtype=dtypes)
    #now get the nights for each target:
    for target in targets:
        nights_dum = [d for d in os.listdir(pardir+target) if \
                     (os.path.isdir(os.path.join(pardir,target,d))
                      and not d.endswith ('_red'))]
        for night in nights_dum:
            fnresults = os.path.join(pardir,target,night+'_red','proc','results.txt')
            try:
                tdum = Table.read(fnresults,
                                  delimiter=" ",names=tentries[-nceres:],
                                  format='ascii.no_header',guess =False)
                tdum['target'] = [target]*len(tdum)
                tdum['night']   = [np.int(night)]  *len(tdum)

                
            except:
                print('The observation of %s at day %s were not analysed'%(target,night),\
                      'Probably no calibration files were found')
                temp = [[target],[night]]
                tdum = Table(temp+nceres*[[np.nan]],names = tentries,
                             dtype=dtypes)
            t_obs = vstack((t_obs,tdum),join_type='outer')
    
    fntable = os.path.join(outdir,'observation_summary.csv')
    t_obs.write(fntable,delimiter=',')
    print('Done with rollect_rv_data.collect.Saved table in %s'%fntable)

                
                
