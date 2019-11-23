# import pymp
import itertools
import csv
import subprocess
import os
import random
import sys
import string
import glob
import shutil
from ttictoc import TicToc
from collections import defaultdict
from mpi4py import MPI

if __name__ == '__main__':
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()

    names = sys.argv[2]
    dir = sys.argv[1]
    os.chdir(dir)

    if rank == 0:
        names = [name for name in names.split(',')]
        scatter_data = [[] for _ in range(size)]
        for i, name in enumerate(names):
            scatter_data[i % size].append(name)
        names = scatter_data
    else:
        names = []

    names = comm.scatter(names, root=0)

    for name in names:
        subprocess.call(['sfdx', 'force:data:bulk:upsert', '-u', 'nitro_scratch', '-s', name, '-f', name + '.csv', '-i', 'IID__c', '-w', '2'])
        # subprocess.call(['mvmt', 'bulk', '-a', 'nitro_scratch', '-o', name, '-p', name + '.csv', '--insert'])

    # print('Rank {0}: {1}'.format(rank, names))

    # for file in files:
    #     with TicToc('Rank {0}'.format(rank)):
    #         print(file)

    # subprocess.call(['mvmt', 'bulk', '-a', 'nitro_scratch', '-o', x, '-p', os.path.join(dir, x + '.csv'), '--insert'])

    # for file in files:
    #     os.remove(file)
    #     shutil.copyfile(os.path.join('archive', file), os.path.join('.', file))