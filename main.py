import os
import csv
from tqdm import tqdm

dir_src_path = "PARENT_DIR_SORCE_FILE"
file_raw_path = "FILE_SORCE_NAME"

file_path = os.path.join(dir_src_path, file_raw_path)

batch = 0
n_batch = 10000000
limit = n_batch
base_file_path = "DESTINATION FILE BATCH %S"
header = None

fp = open(base_file_path % batch, 'w', encoding='latin-1')
writer = csv.writer(fp, lineterminator='\n')


def change_batch():
    global batch
    batch += 1

    global limit
    limit += n_batch

    global fp
    fp.close()
    fp = open(base_file_path % batch, 'w', encoding='latin-1')

    global writer
    writer = csv.writer(fp, lineterminator='\n')


with open(file_path, 'r', encoding='latin-1') as file:
    reader = csv.reader(file)
    for index, row in tqdm(enumerate(reader)):

        if (index == 0):
            header = row

        if (index >= limit):
            change_batch()
            writer.writerow(header)

        writer.writerow(row)

fp.close()
