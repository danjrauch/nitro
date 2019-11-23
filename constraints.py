# import pymp
import itertools
import csv
import os
import random
import sys
import string
import glob
import shutil
from ttictoc import TicToc
from collections import defaultdict
from mpi4py import MPI

def convert_date(slash_date):
    year = slash_date.split('/')[2]
    month = slash_date.split('/')[0]
    day = slash_date.split('/')[1]
    if int(month) < 10 and len(month) < 2:
        month = '0' + month
    if int(day) < 10 and len(day) < 2:
        day = '0' + day
    return year+'-'+month+'-'+day


def constraints(file):
    # SEQUENTIAL
    keys = set()
    fieldnames = []
    with open(file, 'r') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        new_rows = []
        for row_num, row in enumerate(reader):
            if row['IID__c'] in keys:
                row['IID__c'] = row_num
            keys.add(row['IID__c'])
            if 'Birthdate' in row:
                row['Birthdate'] = convert_date(row['Birthdate'])
            if 'Closedate' in row:
                row['Closedate'] = convert_date(row['Closedate'])
            new_rows.append(row)

        new_keys = set()
        for row in new_rows:
            if row['IID__c'] in new_keys:
                print('Uh Oh... ' + str(row['IID__c']))
            new_keys.add(row['IID__c'])

    with open(file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(new_rows)

    # os.remove('new'+file)
    return 0

if __name__ == '__main__':
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()

    dir = sys.argv[1]

    os.chdir(dir)

    if rank == 0:
        files = glob.glob('*.{}'.format('csv'))
        scatter_data = [[] for _ in range(size)]
        for i, file in enumerate(files):
            scatter_data[i % size].append(file)
        files = scatter_data
    else:
        files = []

    files = comm.scatter(files, root=0)

    for file in files:
        with TicToc('Rank {0}'.format(rank)):
            constraints(file)

    # for file in files:
    #     os.remove(file)
    #     shutil.copyfile(os.path.join('archive', file), os.path.join('.', file))