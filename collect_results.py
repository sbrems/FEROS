import numpy as np
import pandas as pd
import os
import matplotlib.pyplot as plt

import ipdb

par_dir = "/disk1/brems/FEROS_archive/raw/" #parent folder to target folder
save_dir = "/disk1/brems/FEROS_archive/results/"
def get_csv(target,plot=True,check_name=False):
    direct = par_dir+target+'/'
    res_files = []
    columns = ['Name','BJD','RV','sig_RV','bisector_span','sig_bisector_span',\
                                    'inst','pipeline','resolving_power','Teff','logg','[Fe_H]',\
                                    'vsini','continuumCCF','sig_gaussian_CCF','Texp','SNR5150',\
                                    'path_pdf']
    table = pd.DataFrame(columns = columns)
    for root,subdirs,files in os.walk(direct):
        for f in files:
            if f == 'results.txt':
                table_dum = pd.read_csv(root+'/'+f,names=columns,sep="\s+")
                if check_name:
                    table = table.append(table_dum.loc[lambda x: x.Name == target])
                else:
                    table = table.append(table_dum)
    table = table.reset_index()
    table.RV =1000* table.RV #km/s to m/s
    table.sig_RV= 1000*table.sig_RV
    table.to_csv(save_dir+target+'_results.csv',sep=',',index=False)
    with open(save_dir+target+'_results.csv','a') as myfile:
        myfile.write('#RV[m/s]: %d +- %d \n'%(np.mean(table.RV),np.std(table.RV)))
        myfile.write('#vsini[km/s]: %d +- %d \n'%(np.mean(table.vsini),np.std(table.vsini)))
    
    if plot:
        fig,ax =plt.subplots()
        ax.errorbar(table.BJD,table.RV,yerr=table.sig_RV,fmt='o')
        ax.set_xlabel('BJD')
        ax.set_ylabel('RV [m/s]')
        ax.set_title(target+' (#='+str(len(table.RV))+', RV: '+str(np.mean(table.RV))+' +- '+str(np.std(table.RV))+')')
        plt.savefig(save_dir+target+'_results.pdf')
    
    print('Done :)')
    ipdb.set_trace()
                
