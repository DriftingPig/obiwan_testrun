#e.g. python brickstat.py --name_for_run decals_ngc --rs rs0 --lepipv DR9.6.4
#SV_bricks.txt
#/global/cscratch1/sd/huikong/Obiwan/dr8/obiwan_out/SV_south/output/tractor/
import os
import subprocess
import astropy.io.fits as fits
topdir = '/global/cfs/cdirs/desi/users/huikong/'
obiwan_out_dir = topdir+'/NAME4RUN/production_run/'
NAaaaME_FOR_RUN=None
RS=None
REAL_BRICKS_FN=None
LEPIPV=None

def mkdir(fn):
    if os.path.exists(fn):
       pass
    else:
       import subprocess
       subprocess.call(["mkdir","-p",fn])


def OneBrickClassify(brickname):
    print(brickname)
    global NAME_FOR_RUN
    global RS
    global REAL_BRICKS_FN
    global obiwan_out_dir
    obiwan_out_dir=obiwan_out_dir.replace('NAME4RUN',NAME_FOR_RUN)
    log_dir = obiwan_out_dir+'/%s/logs/%s/log.%s'%(RS, brickname[:3],brickname)
    data = open(log_dir).read()
    for line in data.splitlines():
        if 'All done:' in line:
            string = line.split(' ')
            time = float(string[11])
            f1 = open('./%s/stats/Walltime-%s.txt'%(NAME_FOR_RUN,LEPIPV), 'a')
            f1.write("%f\n"%time)
            f1.close()
            return 1
    raise ValueError('not done')

def BrickClassify():
    import numpy as np
    import multiprocessing as mp
    N=1
    p = mp.Pool(N)
    global NAME_FOR_RUN
    f1 = open('./%s/stats/Walltime-%s.txt'%(NAME_FOR_RUN,LEPIPV), 'w')
    f1.close()
    bricks = np.loadtxt('./%s/FinishedBricks-%s.txt'%(NAME_FOR_RUN,LEPIPV), dtype=np.str)
    p.map(OneBrickClassify,bricks)


def get_parser():
    import argparse
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,description='brickstat')
    parser.add_argument('--name_for_run', type=str, required=True, help='name of production run')#currently: elg_like_run,elg_ngc_run
    parser.add_argument('--rs', type=str, required=True, help='e.g. rs0, more_rs0,rs200')
    parser.add_argument('--lepipv', type=str, required=True,default='None', help='legacypipe docker version used')
    return parser
if __name__ == '__main__':
    parser= get_parser()
    args = parser.parse_args()  
    NAME_FOR_RUN = args.name_for_run
    RS = args.rs
    REAL_BRICKS_FN = 'bricks_%s_%s.txt'%(NAME_FOR_RUN, args.lepipv)
    LEPIPV = args.lepipv
    mkdir(NAME_FOR_RUN) 
    BrickClassify()

