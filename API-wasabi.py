import argparse, glob, os, boto3, time
from multiprocessing import Pool
import numpy as np 

s3 = boto3.resource('s3',
                    endpoint_url = 'https://s3.wasabisys.com',
                    aws_access_key_id = 'YOUR-KEY-ID',
                    aws_secret_access_key = 'YOU-KEY')

def upload_one_file(args):
    fn, key = args
    s3.meta.client.upload_file(fn, 'boto3', key)

def upload_ds(nf, cc):
    sfn_5g_cc = ["/data/ds-5g/%04d-files/%04d.bin" %  (nf, i) for i in range(nf)]
    dfn_5g_ws = ["5g-%04d-files/%04d.bin" % (nf, i) for i in range(nf)]

    ds_bytes  = np.sum([os.path.getsize(_fn) for _fn in sfn_5g_cc])

    task_list = []
    for _s, _d in zip(sfn_5g_cc, dfn_5g_ws):
        task_list.append((_s, _d))

    trs_ts = time.time()
    with Pool(cc) as p:
        p.map(upload_one_file, task_list)
    _t_elps = time.time() - trs_ts
    print("Average transfer rate is: %.2f Mbps when uploading %d files totaling %.2f GB using cc=%d" % (\
          8 * ds_bytes / _t_elps * 1e-6, len(sfn_5g_cc), ds_bytes*1e-9, cc))
    return ds_bytes / _t_elps


def download_one_file(args):
    key, fn = args
    s3.meta.client.download_file(Bucket='boto3', Key=key, Filename=fn)

def download_ds(nf, cc):
    dfn_5g_cc = ["/data/dl-tmp/%04d.bin" %  (i) for i in range(nf)]
    sfn_5g_ws = ["5g-%04d-files/%04d.bin" % (nf, i) for i in range(nf)]

    task_list = []
    for _s, _d in zip(sfn_5g_ws, dfn_5g_cc):
        task_list.append((_s, _d))

    trs_ts = time.time()
    with Pool(cc) as p:
        p.map(download_one_file, task_list)
    _t_elps = time.time() - trs_ts
    ds_bytes  = np.sum([os.path.getsize(_fn) for _fn in dfn_5g_cc])
    print("Average transfer rate is: %.2f Mbps when download %d files totaling %.2f GB using cc=%d" % (\
          8 * ds_bytes / _t_elps * 1e-6, len(dfn_5g_cc), ds_bytes*1e-9, cc))
    return ds_bytes / _t_elps


if __name__ == '__main__':
    for nf in (1, 50, 100, 200, 400, 600, 800, 1000)[:1]:
    # # for nf in (400, 800, ):
        upload_ds(nf, cc=1)

    for nf in (1, 50, 100, 200, 400, 600, 800, 1000)[:1]:
        download_ds(nf, cc=1)



