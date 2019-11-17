import pymp
import itertools
import csv
import os
import random
import string
from collections import defaultdict

def constraint_keys(files):
    # SEQUENTIAL
    for file in files:
        keys = defaultdict(list)
        fieldnames = []
        with open(file, 'r') as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            new_rows = []
            for row_num, row in enumerate(reader):
                if row['IID'] in keys:
                    row['IID'] = row_num # "".join(random.choices(string.ascii_letters, k=3)) + str(random.randint(1,100000))
                keys[row['IID']].append(row_num) # "".join(random.choices(string.ascii_letters, k=3)) + str(row_num))
                new_rows.append(row)

            new_keys = {}
            for row in new_rows:
                if row['IID'] in new_keys:
                    print('Uh Oh... ' + str(row['IID']))
                    new_keys[row['IID']].append(row_num)
                else:
                    new_keys[row['IID']] = [ row_num ]

        with open('new'+file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)

            writer.writeheader()
            writer.writerows(new_rows)

        # os.remove('new'+file)

    # 1to1 THREAD TO FILE
    # ex_dict = pymp.shared.dict()
    # with pymp.Parallel(2) as p:
    #     if p.thread_num == 0:
    #         keys = {}
    #         with open(files[0], 'r' ) as f:
    #             reader = csv.DictReader(f)
    #             for row_num, row in enumerate(reader):
    #                 if int(row['IID']) in keys:
    #                     keys[int(row['IID'])].append(int(row_num))
    #                 else:
    #                     keys[int(row['IID'])] = [ int(row_num) ]
    #         ex_dict[p.thread_num] = keys
    #     else:
    #         keys = {}
    #         with open(files[1], 'r' ) as f:
    #             reader = csv.DictReader(f)
    #             for row_num, row in enumerate(reader):
    #                 if int(row['IID']) in keys:
    #                     keys[int(row['IID'])].append(int(row_num))
    #                 else:
    #                     keys[int(row['IID'])] = [ int(row_num) ]
    #         ex_dict[p.thread_num] = keys
    # print(ex_dict[1][3])


    # Nto1 THREAD TO FILE
    # with open(files[0], 'r' ) as f:
    #     reader = csv.DictReader(f)
    #     # N = sum(1 for row in reader)
    #     rows = [row for row in reader]
    #     ex_dict = pymp.shared.dict()
    #     with pymp.Parallel(4) as p:
    #         if p.thread_num == 0:
    #             keys = {}
    #             rows = rows[0:25000]
    #             row_num = 0
    #             for row in rows:
    #                 if int(row['IID']) in keys:
    #                     keys[int(row['IID'])] = keys[int(row['IID'])] + [row_num]
    #                 else:
    #                     keys[int(row['IID'])] = [ row_num ]
    #                 row_num += 1
    #             with p.lock:
    #                 for key, val in keys.items():
    #                     if key in ex_dict:
    #                         ex_dict[key] = ex_dict[key] + val
    #                     else:
    #                         ex_dict[key] = val
    #         if p.thread_num == 1:
    #             keys = {}
    #             rows = rows[25000:50000]
    #             row_num = 25000
    #             for row in rows:
    #                 if int(row['IID']) in keys:
    #                     keys[int(row['IID'])] = keys[int(row['IID'])] + [row_num]
    #                 else:
    #                     keys[int(row['IID'])] = [ row_num ]
    #                 row_num += 1
    #             with p.lock:
    #                 for key, val in keys.items():
    #                     if key in ex_dict:
    #                         ex_dict[key] = ex_dict[key] + val
    #                     else:
    #                         ex_dict[key] = val
    #         if p.thread_num == 2:
    #             keys = {}
    #             rows = rows[50000:75000]
    #             row_num = 50000
    #             for row in rows:
    #                 if int(row['IID']) in keys:
    #                     keys[int(row['IID'])] = keys[int(row['IID'])] + [row_num]
    #                 else:
    #                     keys[int(row['IID'])] = [ row_num ]
    #                 row_num += 1
    #             with p.lock:
    #                 for key, val in keys.items():
    #                     if key in ex_dict:
    #                         ex_dict[key] = ex_dict[key] + val
    #                     else:
    #                         ex_dict[key] = val
    #         if p.thread_num == 3:
    #             keys = {}
    #             rows = rows[75000:100000]
    #             row_num = 75000
    #             for row in rows:
    #                 if int(row['IID']) in keys:
    #                     keys[int(row['IID'])] = keys[int(row['IID'])] + [row_num]
    #                 else:
    #                     keys[int(row['IID'])] = [ row_num ]
    #                 row_num += 1
    #             with p.lock:
    #                 for key, val in keys.items():
    #                     if key in ex_dict:
    #                         ex_dict[key] = ex_dict[key] + val
    #                     else:
    #                         ex_dict[key] = val
    #     print('PYMP')
    #     print(ex_dict[16])
    return 0