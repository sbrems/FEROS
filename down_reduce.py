from download_sort import full_download
from run_feros_pipeline import all_subfolders
targets = [#'HD_96064',
        #'HD_139084','HD_183414','HD_216803','HD_224725',
        #'CD-78_24','CD-31_571','HD_139084','CCDM_J01377+1836',
        #'HD_17662','CD-37_1123','HD_21955','1RXS_J033149.8-633155',
        #'HD_23208','CD-31_1688','CD-58_860','HD_26980',
        #'CD-43_1395','CD-43_1451','TYC_5891-69-1','1RXS_J043451.0-354715',
        #'V1204_Tau','HD_31950','HD_286264','HD_32372',
        #'HD_34700','GW_Ori','HD_36329','HD_269620',
        #'HD_37551','HD_42270','HD_44748','HD_49078',
        #'HD_51062','HD_51797','1RXS_J070153.4-422759','TYC_8558-1148-1',
        #'TYC_8559-1016-1','CD-84_0080','TYC_8911-2430-1','TYC_8142-1112-1',
        #'CD-38_4458','EG_Cha','HD_81544','TYC_7697-2254-1',
        #'CR_Cha','DI_Cha','T_Cha','RX_J1233.5-7523',
        #'1RXS_J123332.4-571345','TYC_9412-59-1','TYC_8654-1115-1','CD-69_1055',
        #'HD_124784','HD_129181','LQ_Lup','LT_Lup',
        #'LX_Lup','LY_Lup','MP_Lup','MS_Lup',
        #'1RXS_J153328.4-665130','RX_J1538.6-3916','CD-43_10072','MU_Lup',
        #'HT_Lup','HD_140637','RX_J1547.6-4018','GQ_Lup',
        #'IM_Lup',
        #'HBC_603','RU_Lup','GSC_06195-00768',
        'RY_Lup','CD-36_10569','MZ_Lup','EX_Lup',
        'TYC_7334-429-1',
        #'V1002_Sco',
        'HD_147048','EM*_SR21',
        'EM*_SR9','V2129_Oph','1RXS_J162950.1-272834','DoAr_44',
        'Wa_Oph6','V1121_Oph','TYC_8726-57-1','CD-27_11535',
        'TYC_6812-348-1','HD_319896','HD_161460','V709_CrA',
        'CD-37_13029','CD-38_13398','HD_207278','1RXS_J223929.1-520525',
        'CP-72_2713','CD-40_14901','HD_217897','BD-03_5579',
        'HD_220054','TYC_584-343-1','BD-13_6424','HD_25457']
    # targets_selected = ['HD139084',
    #            'HD183414',
    #            'HD216803',
    #            '1RXS_J033149.8-633155',
    #            'CD-37_1123',
    #            'CD-37_13029',
    #            'CD7824',
    #            'HD217897',
    #            'TYC_5891-69-1',
    #            'TYC_8654-1115-1',
    #            'HBC_603',
    #            '1RXS_J043451.0-354715',
    #            'CD-40 14901',
    #            'HD140637',
    #            'HD23208',
    #            'HD25457',
    #            'HD51797',
    #            'HD81544',
    #            'V2129_Oph'
    #        ]

def download(store_pwd=True):

    for ii,target in enumerate(targets):
        target = target.replace(' ','_')
        targets[ii] = target
        print('DOWNOLADING target %s (%d of %d)'%(target,ii,len(targets)))
        print('###########################################')
        clear_cache = False
        if ii%6 ==5:
            clear_cache=True
        if target not in ['HD_139084','HD_183414','HD_216803','HD_224725',
                          'CD-78_24','CD-31_571','HD_139084',
                          'CCDM_J01377+1836',
                          'HD_17662','CD-37_1123','HD_21955','1RXS_J033149.8-633155',
                          'HD_23208','CD-31_1688','CD-58_860','HD_26980',
                          'CD-43_1395','CD-43_1451','TYC_5891-69-1','1RXS_J043451.0-354715',
                          'V1204_Tau','HD_31950','HD_286264','HD_32372',
                          'HD_34700','GW_Ori','HD_36329','HD_269620',
                          'HD_37551','HD_42270','HD_44748','HD_49078',
                          'HD_51062','HD_51797','1RXS_J070153.4-422759','TYC_8558-1148-1',
                          'TYC_8559-1016-1','CD-84_0080','TYC_8911-2430-1','TYC_8142-1112-1',
                          'CD-38_4458','EG_Cha','HD_81544','TYC_7697-2254-1',
                          'CR_Cha','DI_Cha','T_Cha','RX_J1233.5-7523',        	
                          '1RXS_J123332.4-571345','TYC_9412-59-1','TYC_8654-1115-1','CD-69_1055',
                          'HD_124784','HD_129181','LQ_Lup','LT_Lup',      
                          'LX_Lup','LY_Lup','MP_Lup','MS_Lup','1RXS_J153328.4-665130',
                          'RX_J1538.6-3916','CD-43_10072','MU_Lup',
                          'HT_Lup','HD_140637','RX_J1547.6-4018','GQ_Lup',
                          #'IM_Lup',   
                          'HBC_603','RU_Lup','GSC_06195-00768',
                          'RY_Lup','CD-36_10569','MZ_Lup','EX_Lup',
                          'TYC_7334-429-1',
                          #'V1002_Sco',
                          'HD_147048','EM*_SR21',
                          'EM*_SR9','V2129_Oph','1RXS_J162950.1-272834','DoAr_44',
                          'Wa_Oph6','V1121_Oph','TYC_8726-57-1','CD-27_11535',
                          'TYC_6812-348-1','HD_319896','HD_161460','V709_CrA',
                          'CD-37_13029','CD-38_13398','HD_207278','1RXS_J223929.1-520525',
                          'CD-40_14901','HD_217897','HD_220054','TYC_584-343-1',
                          'HD_25457',
                      ]:
            
            full_download(target,store_pwd=store_pwd,overwrite_old='y',clear_cache=clear_cache)
    print('Downloaded :)')


def reduce(mod):
    for ii,target in enumerate(targets):
        if ii%4==mod:
            print('PROCESSING target %s (%d of %d)'%(target,ii,len(targets)))
            print('###########################################')
            print('(Processing modulo %d)'%mod)
            all_subfolders(target,show_pdfs=False)
    print('Wohooo!!!')

