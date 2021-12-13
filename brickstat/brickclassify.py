#e.g. python brickstat.py --name_for_run dr9m_test --rs rs0 --real_bricks_fn bricks_dr9f_south.txt
#SV_bricks.txt
#/global/cscratch1/sd/huikong/Obiwan/dr8/obiwan_out/SV_south/output/tractor/
import os
import subprocess
import astropy.io.fits as fits
topdir = os.environ['CSCRATCH']
obiwan_out_dir = topdir+'/Obiwan/dr9_LRG/obiwan_out/NAME4RUN/output/'
NAME_FOR_RUN=None
RS=None
REAL_BRICKS_FN=None

def mkdir(fn):
    if os.path.exists(fn):
       pass
    else:
       import subprocess
       subprocess.call(["mkdir","-p",fn])



def BrickClassify():
    import numpy as np
    import multiprocessing as mp
    N=1
    global NAME_FOR_RUN

    #next, for the unfinished bricks, record the LEGPIPEV to separate files
    import subprocess
    subprocess.call(["rm", './%s/UnfinishedBricks-*.txt'%NAME_FOR_RUN])
    bricks = np.loadtxt('./%s/UnfinishedBricks.txt'%NAME_FOR_RUN, dtype = np.str)
    LEGPIPEV=[]
    i=0
    for brickname in bricks:
        if i%1000==0:
            print(i)
        i+=1
        fn = "/global/cfs/cdirs/cosmo/work/legacysurvey/dr9/south/tractor/%s/tractor-%s.fits"%(brickname[:3],brickname)
        ver = fits.open(fn)[0].header['LEGPIPEV']
        print(ver)
        if ver in LEGPIPEV:
            f = open('./%s/UnfinishedBricks-%s.txt'%(NAME_FOR_RUN, ver), 'a')
            f.write(str(brickname)+'\n')
            f.close()
        else:
            LEGPIPEV.append(ver)
            f = open('./%s/UnfinishedBricks-%s.txt'%(NAME_FOR_RUN, ver), 'w')
            f.write(str(brickname)+'\n')
            f.close()
def get_parser():
    import argparse
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,description='brickstat')
    parser.add_argument('--name_for_run', type=str, required=True, help='name of production run')#currently: elg_like_run,elg_ngc_run
    parser.add_argument('--rs', type=str, required=True, help='e.g. rs0, more_rs0,rs200')
    parser.add_argument('--real_bricks_fn', type=str, required=False,default='None', help='bricks processed in this run')
    return parser
if __name__ == '__main__':
    parser= get_parser()
    args = parser.parse_args()  
    #global NAME_FOR_RUN
    #global RS
    #global REAL_BRICKS_FN
    NAME_FOR_RUN = args.name_for_run
    RS = args.rs
    if args.real_bricks_fn == 'None':
        REAL_BRICKS_FN = 'bricks_%s.txt'%NAME_FOR_RUN
    else:
        REAL_BRICKS_FN = args.real_bricks_fn   
    mkdir(NAME_FOR_RUN) 
    BrickClassify()

