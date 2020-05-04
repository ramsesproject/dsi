#! /home/cc/usr/miniconda3/bin/python

import logging, sys
from boxsdk import OAuth2, Client, JWTAuth

import os, pickle, time
import numpy as np 
from multiprocessing import Pool

logging.disable(logging.CRITICAL)

auth = JWTAuth.from_settings_file('box-key.json')
client = Client(auth)

# sys.stdout = open('prints.log', 'w') 

def upload_one_file(args):
    sfn, dfn, pfid = args
    folder = client.folder(folder_id=pfid)
    _file = folder.upload(file_path=sfn, file_name=dfn, upload_using_accelerator=False)

def list_root_dirs():
    dir2id = {}
    for item in client.folder('0').get_items():
        if item.type.capitalize() == 'Folder':
            dir2id[item.name] = item.id
    return dir2id

def upload_ds(nf, cc):
    sfn_1g_cc = ["/data/ds-1g/%04d-files/%04d.bin" %  (nf, i) for i in range(nf)]
    dfn_1g_ws = ["%04d.bin" % (i, ) for i in range(nf)]
    ds_bytes  = np.sum([os.path.getsize(_fn) for _fn in sfn_1g_cc])

    dir_name = '1g-%04d-files' % nf
    dir2id = list_root_dirs()
    if dir2id.get(dir_name) is None:
        dir_id = client.folder('0').create_subfolder(dir_name).id
    else:
        dir_id = dir2id[dir_name]    

    folder = client.folder(folder_id=dir_id)
    # get a list of files and del if exist
    fn2id = {}
    for item in folder.get_items():
        if item.type.capitalize() == 'File':
            fn2id[item.name] = item.id
    for _dfn in dfn_1g_ws:
        if fn2id.get(_dfn) is not None:
            client.file(fn2id[_dfn]).delete()

    task_list = []
    for _s, _d in zip(sfn_1g_cc, dfn_1g_ws):
        task_list.append((_s, _d, dir_id))

    trs_ts = time.time()
    if cc == 1:
        folder = client.folder(folder_id=dir_id)
        for sfn, dfn, _ in task_list:
            _file = folder.upload(file_path=sfn, file_name=dfn, upload_using_accelerator=False)
    else:
        with Pool(cc) as p:
            p.map(upload_one_file, task_list)

    _t_elps = time.time() - trs_ts
    print("Average transfer rate is: %.2f Mbps when uploading %d files totaling %.2f GB using cc=%d" % (\
          8 * ds_bytes / _t_elps * 1e-6, len(task_list), ds_bytes*1e-9, cc))
    return ds_bytes / _t_elps

def download_one_file(args):
    sid, dfn = args
    client.file(sid).get().download_to(open(dfn, 'wb'))

def download_ds(nf, cc):
    dfn_1g_cc = ["/data/dl-tmp/%04d.bin" %  (i) for i in range(nf)]
    sfn_1g_ws = ["%04d.bin" % (i,) for i in range(nf)]
    dir_name = '1g-%04d-files' % nf

    dir2id = list_root_dirs()
    if dir2id.get(dir_name) is None:
        print('Folder %s does not exist' % dir_name)
        return

    dir_id = dir2id[dir_name]   

    folder = client.folder(folder_id=dir_id)
    fn2id = {}
    for item in folder.get_items():
        if item.type.capitalize() == 'File':
            fn2id[item.name] = item.id

    sf_id = [fn2id[_title] for _title in sfn_1g_ws]

    task_list = []
    for _s, _d in zip(sf_id, dfn_1g_cc):
        task_list.append((_s, _d))

    trs_ts = time.time()
    if cc == 1:
        for _task in task_list:
            download_one_file(_task)
    else:  
        with Pool(cc) as p:
            p.map(download_one_file, task_list)
    _t_elps = time.time() - trs_ts
    ds_bytes  = np.sum([os.path.getsize(_fn) for _fn in dfn_1g_cc])
    print("Average transfer rate is: %.2f Mbps when download %d files totaling %.2f GB using cc=%d" % (\
          8 * ds_bytes / _t_elps * 1e-6, len(task_list), ds_bytes*1e-9, cc))
    return ds_bytes / _t_elps

if __name__ == '__main__':
    for nf in (50, 100, 200, 400, 600, 800, 1000)[5:]:
        upload_ds(nf, cc=1)
        # sys.stdout.flush()

    for nf in (50, 100, 200, 400, 600, 800, 1000)[:]:
        download_ds(nf, cc=1)
        # sys.stdout.flush()
