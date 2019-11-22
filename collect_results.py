import os
import numpy as np
import pandas as pd
import subprocess
import matplotlib.pyplot as plt
import seaborn as sns
from astropy.table import Table, vstack
from astropy.time import Time
from glob import glob
import io
from config import default_science_dir, default_log_dir

nceres = 18  # number of ceres outputs
achievable_rv_lim = 7.  # max output number of rv_lim to still consider it good. Paula said ~2-3


def get_pdfs():
    '''get the PDFS for all stars and store it in the parent directory for
    each star individually'''
    dirs = [os.path.join(par_dir, o) for o in os.listdir(par_dir) 
            if os.path.isdir(os.path.join(par_dir,o))]
    for ddir in dirs:
        dtarget = os.path.basename(ddir)

        pnpdfs = []
        pndates = [os.path.join(ddir, o) for o in os.listdir(ddir)
                   if (os.path.isdir(os.path.join(ddir,o)) and o.endswith('_red'))]
        for pndate in pndates:
            pdfdir = os.path.join(pndate, 'proc')
            pnpdfs += [os.path.join(pdfdir, ff) for ff in os.listdir(pdfdir)
                           if ff.endswith('.pdf')]

        if len(pnpdfs) >= 1:
            print('Found and merging {} pdfs for {}'.format(len(pnpdfs), dtarget))
            subprocess.run(["pdftk",]+pnpdfs+ ["output",
                            os.path.join(ddir, "allpdfs_{}.pdf".format(dtarget))])

        else:
            print('No pdf found for {}'.format(dtarget))


def all_csvs(science_dir=None, log_dir=None, activity_indicators=False):
    '''Collects all the data from the FEROS reanalysis and store in a single table
    activity_indicators=False
    If True, it will try to determine activity_indicators such as H_alpha and others'''
    if science_dir is None:
        science_dir = default_science_dir
    if log_dir is None:
        log_dir = default_log_dir
    for direct in [log_dir]:
        if not os.path.exists(direct):
            os.mkdir(direct)
    
    tentries = ['achievable_rvs_good', 
                'target','bjd','rv','sig_rv','bisec_span','sig_bisec_span',#from ceres output
                'inst','pipeline','resolving_power',
                'Teff','logg','[Fe/H]','vsini',
                'lowest_continuum','std_gaus2ccf',
                'Texp','snr','pdfpath',]
    dtypes  =  [bool,
                str,np.float64,np.float64,np.float64,np.float64,np.float64,
                str,str,np.float64,
                np.float64,np.float64,np.float64,np.float64,
                np.float64,np.float64,
                np.float64,np.float64,str,]
    t_obs = Table([[]]*len(tentries),names=tentries,dtype=dtypes)

    procdirectories = glob('**/~proc/', recursive=True)
    for procdir in procdirectories:
        fnresults = os.path.join(procdir, 'results.txt')
        par_dir = os.path.abspath(os.path.normpath(os.path.join(
            procdir, os.pardir)))
        fnachievable_rvs = os.path.join(par_dir, 'achievable_rvs.txt')
        try:
            tdum = Table.read(fnresults,
                              delimiter=" ",names=tentries[-nceres:],
                              format='ascii.no_header', guess =False)
        except IOError:
            print('Coudnt find {}'.format(fnresults))
            continue
        except (IOError, ValueError) as error:
            print('The observation of %s at day %s were not analysed'%(target,night),\
                  'Probably no calibration files were found')
        tdum['achievable_rvs_good'] = False
        try:
            with open(fnachievable_rvs, "r") as frv:
                rvs = frv.read().strip().split('\n')
            if rvs[0] == "":
                rvs = [np.nan]
            rvs = np.array([float(rv) for rv in rvs])
            if np.max(rvs) <= achievable_rv_lim:
                print('Max rv is {}. Assuming this was a good fit'.format(
                    np.max(rvs)))
                tdum['achievable_rvs_good'] = True
            else:
                print('Max rv is {}. Assuming this was a bad fit'.format(
                    np.max(rvs)))
        except IOError:
            print('No output achievable_rvs output found. Assuming theyre bad.\
ked here: {} )'.format(fnachievable_rvs))

            # try to get the activity_indicators
            if activity_indicators:
                for tobs in tdum:
                    try:
                        from starclass import Star
                        star = Star(tobs['target'])
                        pnspectrum = os.path.join(
                            procdir,
                            star.sname + Time(tobs['bjd'], format='jd').isot + '_sp.fits'
                        )
                        star.read_ceres_spectrum(pnspectrum)
                        star.get_activity_indicators()
                        actindics = star.activity_indicators()
                        for aidx, valerr in actindics:
                            tdum[aidx] = valerr[0]
                            tdum[aidx+'err'] = valeer[1]
                    except IOError:
                        print('Couldnt get activity indicators since file {} not \
found'.format(pnspectrum))
                    except:
                        print('Couldnt get activity indicator for {} for unknown \
reasons'.format(tobs))
                            
        t_obs = vstack((t_obs,tdum), join_type='outer')
    
    pntable = os.path.join(log_dir, 'observation_summary.csv',)
    t_obs.write(pntable, delimiter=',', overwrite=True)
    print('Done with collect_rv_data.collect. Saved table in {}'.format(pntable))

    
def get_csv(target, plot=True, check_name=False):
    for ddir in [par_dir, save_dir]:
        if not os.path.exists(ddir):
            os.mkdir(ddir)
    direct = par_dir+target+'/'
    # res_files = []
    columns = ['Name', 'BJD', 'RV', 'sig_RV', 'bisector_span', 'sig_bisector_span',
                                    'inst', 'pipeline', 'resolving_power', 'Teff', 'logg', '[Fe_H]',
                                    'vsini', 'continuumCCF', 'sig_gaussian_CCF', 'Texp','SNR5150',
                                    'path_pdf']
    table = pd.DataFrame(columns=columns)
    for root, subdirs, files in os.walk(direct):
        for f in files:
            if f == 'results.txt':
                table_dum = pd.read_csv(root+'/'+f, names=columns, sep="\s+")
                if check_name:
                    table = table.append(table_dum.loc[lambda x: x.Name == target])
                else:
                    table = table.append(table_dum)
    table = table.reset_index()
    table.RV = 1000* table.RV  # km/s to m/s
    table.sig_RV = 1000*table.sig_RV
    table.to_csv(save_dir+target+'_results.csv', sep=',', index=False)
    with open(save_dir+target+'_results.csv', 'a') as myfile:
        myfile.write('#RV[m/s]: %d +- %d \n'%(np.mean(table.RV),np.std(table.RV)))
        myfile.write('#vsini[km/s]: %d +- %d \n'%(np.mean(table.vsini),np.std(table.vsini)))
    
    if plot:
        fig,ax = plt.subplots()
        ax.errorbar(table.BJD, table.RV, yerr=table.sig_RV, fmt='o')
        ax.set_xlabel('BJD')
        ax.set_ylabel('RV [m/s]')
        ax.set_title(target+' (#='+str(len(table.RV))+', RV: '+str(np.mean(table.RV))+' +- '+str(np.std(table.RV))+')')
        plt.savefig(save_dir+target+'_results.pdf')
    
    print('Done :)')

                


def merge_final_CCF(parent_directory, plot_dir=None):
    '''Only store the second page of the CCF:
    Collect all *.pdf files in a proc-folder in a subdirectory of parent dir
    and extract the
    CCF functions CERES returns. Sort those by target and collect them in
    a big pdf.'''
    from PyPDF2 import PdfFileReader, PdfFileWriter
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import letter

    if plot_dir is None:
        plot_dir=parent_directory
    pnpdfs = glob(os.path.join(parent_directory, '**/proc/*.pdf'), recursive=True)
    fnpdfs = [pn.split('/')[-1] for pn in pnpdfs]
    tarnames = ['_'.join(fn.split('.')[-2].split('_')[0:-2]) for fn in fnpdfs]
    times = [Time(fn.split('.')[1], format='isot') for fn in fnpdfs]
    tpdfs = Table([tarnames, times, fnpdfs, pnpdfs],
                  names=['target', 'time','pdfname', 'pdfpath'])
    tpdfs = tpdfs.group_by('target')

    for tgroup in tpdfs.groups:
        tgroup.sort('time')
        tarname = tgroup['target'][0]
        output = PdfFileWriter()
        print('Processing {} ({} files)'.format(tarname, len(tgroup)))
        ccfs = []
        openfiles = []
        for time, pdfpath in tgroup[['time', 'pdfpath']]:
             openfiles.append(open(pdfpath, 'rb'))
             page = PdfFileReader(openfiles[-1]).getPage(1)
             page.cropBox.lowerLeft = (12, 5)
             page.cropBox.upperRight = (418, 305)
             output.addPage(page)

             # add the text
             packet = io.BytesIO()
             # create a new PDF with Reportlab
             can = canvas.Canvas(packet, pagesize=letter)
             can.drawString(280, 45, time.iso)
             can.save()
            
             #move to the beginning of the StringIO buffer
             packet.seek(0)
             textpdf = PdfFileReader(packet)
             page.mergePage(textpdf.getPage(0))

        with open(os.path.join(plot_dir, '{}_CCFs.pdf'.format(tarname)), 'wb') as out_pdf:
            output.write(out_pdf)
        for of in openfiles:
            of.close()
