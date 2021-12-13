#e.g. python brickstat.py --name_for_run decals_ngc --rs rs0 --lepipv DR9.6.4
#SV_bricks.txt
#/global/cscratch1/sd/huikong/Obiwan/dr8/obiwan_out/SV_south/output/tractor/
import os
import subprocess
import astropy.io.fits as fits
topdir = '/global/cfs/cdirs/desi/users/huikong/'
obiwan_out_dir = topdir+'/NAME4RUN/production_run/'
NAME_FOR_RUN=None
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
    #print(log_dir)

    tractor_dir = obiwan_out_dir+'/%s/tractor/%s/tractor-%s.fits'%(RS, brickname[:3],brickname)
    print(tractor_dir)
    if os.path.isfile(tractor_dir):
        f2 = open('./%s0/FinishedBricks-%s.txt'%(NAME_FOR_RUN,LEPIPV), 'a')
        f2.write(str(brickname)+'\n')
        f2.close()
        return 1
    else:
        logdir = obiwan_out_dir+'/%s/logs/%s/log.%s'%(RS, brickname[:3],brickname)
        if os.path.isfile(logdir):
            string = "Obiwan: simcat length is 0, return None"
            if string in open(log_dir).read():
                f2 = open('./%s0/FinishedBricks-%s.txt'%(NAME_FOR_RUN,LEPIPV), 'a')
                f2.write(str(brickname)+'\n')
                f2.close()
                return 1
            else:
                f1 = open('./%s0/UnfinishedBricks-%s.txt'%(NAME_FOR_RUN,LEPIPV), 'a')
                f1.write(str(brickname)+'\n')
                return -1
        else:
            f1 = open('./%s0/UnfinishedBricks-%s.txt'%(NAME_FOR_RUN,LEPIPV), 'a')
            f1.write(str(brickname)+'\n')
            f1.close()
            return -1

def BrickClassify():
    import numpy as np
    import multiprocessing as mp
    N=1
    p = mp.Pool(N)
    global NAME_FOR_RUN
    f1 = open('./%s0/FinishedBricks-%s.txt'%(NAME_FOR_RUN,LEPIPV), 'w')
    f1.close()
    f2 = open('./%s0/UnfinishedBricks-%s.txt'%(NAME_FOR_RUN,LEPIPV), 'w')
    f2.close()
    bricks = np.loadtxt('./real_brick_lists/%s'%REAL_BRICKS_FN, dtype=np.str)
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

