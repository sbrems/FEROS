from download_sort import full_download
from run_feros_pipeline import all_subfolders


targets = [
    'HD_25457',
    'HD_10700'
]

def download(store_pwd=True):
    for ii, target in enumerate(targets):
        target = target.replace(' ', '_')
        targets[ii] = target
        print('DOWNOLADING target %s (%d of %d)' %
              (target, ii, len(targets) - 1))
        print('###########################################')
        clear_cache = False
        # if ii%6 == 5:
        #   clear_cache = True  # clear the download folder. On the cluster I have only
        # limited space there
        if target not in ignore_targets:
            full_download(target, store_pwd=store_pwd,
                          overwrite_old='y', clear_cache=clear_cache)
    print('Downloaded data and calib for all {} targets :)'.format(len(targets)))


def reduce(mod=None, do_class=False, npools=5):
    if do_class:
        print('PROCESSING {} targets WITH spectral classification. \
This takes some time.'.format(len(targets)))
    else:
        print('PROCESSING {} targets withOUT spectral classification. \
This is a bit faster than with.'.format(len(targets)))
    for ii, target in enumerate(targets):
        if target in ignore_targets:
            print('Ignoring target {} as its in the ignore list'.format(target))
        else:
            if mod is None:
                print('PROCESSING target %s (%d of %d)' % (target, ii,
                                                           len(targets)))
                print('###########################################')
                print('(Processing all targets)')
                all_subfolders(target, show_pdfs=False, do_class=do_class,
                               npools=npools)
            else:
                if ii % 4 == mod:
                    print('PROCESSING target %s (%d of %d)' % (target, ii,
                                                               len(targets)))
                    print('###########################################')
                    print('(Processing targets modulo %d)' % mod)
                    all_subfolders(target, show_pdfs=False,
                                   do_class=do_class,
                                   npools=npools)
    print('Wohooo!!!')
