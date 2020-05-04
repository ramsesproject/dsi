#! /home/cc/usr/miniconda3/bin/python

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import os, pickle, time
import numpy as np 
from multiprocessing import Pool


if os.path.exists('pdrive.pkl'):
    with open('pdrive.pkl', 'rb') as fd:
        drive = pickle.load(fd)

# node this piece of code need to run with a browser 
# we run this piece on laptopm save the token and upload to server for 
# following experiments, the token will expire after sometime, 
# but you can always update once expire

else:
    g_login = GoogleAuth()
    g_login.LocalWebserverAuth()
    drive = GoogleDrive(g_login)

    with open('pdrive.pkl', 'wb') as token:
        pickle.dump(drive, token)


def upload_one_file(args):
    sfn, dfn, pid = args
    file_drive = drive.CreateFile({'title': dfn, \
                                   'parents':[{'id':pid},]})
    file_drive.SetContentFile(sfn) 
    file_drive.Upload()

def list_root_dirs():
    dir2id = {}
    for f in drive.ListFile({'q': "'root' in parents and trashed=false"}).GetList():
        if f['mimeType'] == 'application/vnd.google-apps.folder':
            dir2id[f['title']] = f['id']
    return dir2id

def upload_ds(nf, cc):
    sfn_1g_cc = ["/data/ds-1g/%04d-files/%04d.bin" %  (nf, i) for i in range(nf)]
    dfn_1g_ws = ["%04d.bin" % (i,) for i in range(nf)]

    dir_name = '1g-%04d-files' % nf
    dir2id   = list_root_dirs()
    if dir2id.get(dir_name) is None:
        folder_metadata = {'title' : dir_name, 'mimeType' : 'application/vnd.google-apps.folder'}
        folder = drive.CreateFile(folder_metadata)
        folder.Upload()
    dir_id = list_root_dirs()[dir_name]

    ds_bytes  = np.sum([os.path.getsize(_fn) for _fn in sfn_1g_cc])

    task_list = []
    for _s, _d in zip(sfn_1g_cc, dfn_1g_ws):
        task_list.append((_s, _d, dir_id))

    trs_ts = time.time()
    with Pool(cc) as p:
        p.map(upload_one_file, task_list)
    _t_elps = time.time() - trs_ts
    print("Average transfer rate is: %.2f Mbps when uploading %d files totaling %.2f GB using cc=%d" % (\
          8 * ds_bytes / _t_elps * 1e-6, len(sfn_1g_cc), ds_bytes*1e-9, cc))
    return ds_bytes / _t_elps

def download_one_file(args):
    sid, dfn = args
    file_local = drive.CreateFile({'id': sid})
    file_local.GetContentFile(dfn)

def download_ds(nf, cc):
    dfn_1g_cc = ["/data/dl-tmp/%04d.bin" %  (i) for i in range(nf)]
    sfn_1g_ws = ["%04d.bin" % (i, ) for i in range(nf)]
    dir_name = '1g-%04d-files' % nf

    dir2id = list_root_dirs()
    if dir2id.get(dir_name) is None:
        print('Folder %s does not exist' % dir_name)
        return

    dir_id = dir2id[dir_name]

    title2id = {}
    for f in drive.ListFile({'q': "'%s' in parents and trashed=false" % (dir_id,)}).GetList():
        title2id[f['title']] = f['id']

    sf_id = [title2id[_title] for _title in sfn_1g_ws]

    task_list = []
    for _s, _d in zip(sf_id, dfn_1g_cc):
        task_list.append((_s, _d))

    trs_ts = time.time()
    with Pool(cc) as p:
        p.map(download_one_file, task_list)
    _t_elps = time.time() - trs_ts
    ds_bytes  = np.sum([os.path.getsize(_fn) for _fn in dfn_1g_cc])
    print("Average transfer rate is: %.2f Mbps when download %d files totaling %.2f GB using cc=%d" % (\
          8 * ds_bytes / _t_elps * 1e-6, len(dfn_1g_cc), ds_bytes*1e-9, cc))
    return ds_bytes / _t_elps

if __name__ == '__main__':
    for nf in (50, 100, 200, 400, 600, 800, 1000)[5:]:
        upload_ds(nf, cc=1)


    for nf in (50, 100, 200, 400, 600, 800, 1000):
        download_ds(nf, cc=1)
