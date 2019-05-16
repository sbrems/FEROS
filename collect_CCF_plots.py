import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from astropy.table import Table
from astropy.time import Time
from glob import glob
from PyPDF2 import PdfFileReader, PdfFileWriter
import io
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
import os

import ipdb

def do(parent_directory, plot_dir=None):
    '''Collect all *.pdf files in a proc-folder in a subdirectory of parent dir
    and extract the
    CCF functions CERES returns. Sort those by target and collect them in
    a big pdf.'''
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

    ipdb.set_trace()
