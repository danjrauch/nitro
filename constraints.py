# import pymp
import itertools
import csv
import os
import random
import sys
import string
import glob
from ttictoc import TicToc
from collections import defaultdict
from mpi4py import MPI

def constraint_keys(file):
    # SEQUENTIAL
    keys = set()
    fieldnames = []
    with open(file, 'r') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        new_rows = []
        for row_num, row in enumerate(reader):
            if row['IID'] in keys:
                row['IID'] = row_num # "".join(random.choices(string.ascii_letters, k=3)) + str(random.randint(1,100000))
            keys.add(row['IID']) # "".join(random.choices(string.ascii_letters, k=3)) + str(row_num))
            new_rows.append(row)

        new_keys = set()
        for row in new_rows:
            if row['IID'] in new_keys:
                print('Uh Oh... ' + str(row['IID']))
            new_keys.add(row['IID'])

    with open('new'+file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerows(new_rows)

    os.remove('new'+file)

    # 1to1 THREAD TO FILE
    # ex_dict = pymp.shared.dict()
    # with pymp.Parallel(2) as p:
    #     if p.thread_num == 0:
    #         keys = {}
    #         with open(files[0], 'r' ) as f:
    #             reader = csv.DictReader(f)
    #             for row_num, row in enumerate(reader):
    #                 if row['IID'] in keys:
    #                     keys[row['IID']].append(int(row_num))
    #                 else:
    #                     keys[row['IID']] = [ int(row_num) ]
    #         ex_dict[p.thread_num] = keys
    #     else:
    #         keys = {}
    #         with open(files[1], 'r' ) as f:
    #             reader = csv.DictReader(f)
    #             for row_num, row in enumerate(reader):
    #                 if row['IID'] in keys:
    #                     keys[row['IID']].append(int(row_num))
    #                 else:
    #                     keys[row['IID']] = [ int(row_num) ]
    #         ex_dict[p.thread_num] = keys
    # print(ex_dict[1][3])


    # Nto1 THREAD TO FILE
    # fieldnames = []
    # rows = []
    # with open(files[0], 'r') as f:
    #     reader = csv.DictReader(f)
    #     fieldnames = reader.fieldnames
    #     # rows =
    #     rows = pymp.shared.list([row for row in reader])
    #     N = len(rows)
    #     ex_dict = pymp.shared.dict()
    #     with pymp.Parallel(4) as p:
    #         start = p.thread_num * int(N/4)
    #         end = (p.thread_num + 1) * int(N/4)
    #         keys = defaultdict(int)
    #         my_rows = []
    #         with p.lock:
    #             my_rows = rows[start:end]
    #         row_num = start
    #         for row in my_rows:
    #             if row['IID'] in keys:
    #                 keys[row_num] = row_num # generate a new unique key here
    #             else:
    #                 keys[row['IID']] = row_num
    #             row_num += 1
    #         with p.lock:
    #             for key, val in keys.items():
    #                 if key in ex_dict:
    #                     n_key = random.randint(N+1, N+N)
    #                     while n_key in ex_dict:
    #                         n_key = random.randint(N+1, N+5*N)
    #                     rows[val]['IID'] = n_key
    #                     ex_dict[rows[val]['IID']] = val
    #                 else:
    #                     ex_dict[key] = val

    # with open('new'+files[0], 'w', newline='') as f:
    #     writer = csv.DictWriter(f, fieldnames=fieldnames)

    #     writer.writeheader()
    #     writer.writerows(rows)

    # os.remove('new'+files[0])
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
        files = None

    # if rank == 0:
    #     for fl in files:
    #         for file in fl:
    #             constraint_keys(file)

    files = comm.scatter(files, root=0)

    for file in files:
        with TicToc('Constraint Keys for Rank {0}'.format(rank)):
            result = constraint_keys(file)