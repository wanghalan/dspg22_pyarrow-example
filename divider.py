import pandas as pd
import numpy as np
import os
import math
import itertools
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import logging
import argparse


def calculate_number_of_files(large_file_path, file_size_limit=50):
    # Given the size of a large file, return the total number of files that needs to be created rounded up
    return math.ceil(os.path.getsize(large_file_path) / 1000000 / file_size_limit)


def append_divide_column(df, num_files, div_col_name='_tmp-divide'):
    # given the data frame and number of divisions, repeat elements of a list n times
    assert div_col_name not in df.columns
    num_rows = len(df)
    lst = range(0, num_files)
    division_list = list(itertools.chain.from_iterable(
        itertools.repeat(x, math.ceil(num_rows / num_files)) for x in lst))
    df[div_col_name] = division_list[:num_rows]
    return df


def get_dataset(dir_name):
    dataset = pq.ParquetDataset(dir_name)
    table = dataset.read()
    logging.info(table)


def size_limit(x):
    x = int(x)
    if x <= 0:
        raise argparse.ArgumentTypeError("Minimum filesize is 1 MB")
    return x


if __name__ == '__main__':
    '''
    Given a large file, find the number of files it needs to be divided into, and then evenly distribute those files into partitioning
    '''
    parser = argparse.ArgumentParser(
        description='Take a file and divide it into partitions of specific sizes')
    parser.add_argument('-i', '--input', type=str,
                        help='The large file to be partitioned', required=True)
    parser.add_argument('-s', '--size', type=size_limit, default=50,
                        help='Maximum size of the partitioned file in MB')
    parser.add_argument('-o', '--output', type=str, required=True)
    parser.add_argument('-v', '--verbose',
                        action=argparse.BooleanOptionalAction)
    args = parser.parse_args()
    log_level = logging.INFO
    if args.verbose:
        log_level = logging.DEBUG

    logging.basicConfig(
        format='%(levelname)s: %(message)s', level=log_level)

    # large_file_path = 'output_2019_q1.parquet'
    large_file_path = args.input
    num_files = calculate_number_of_files(large_file_path)
    logging.info('Number of files to divide into: %s' % num_files)
    logging.info('Reading table')
    table = pq.read_table(large_file_path)
    df = table.to_pandas()
    div_col_name = '_tmp-divide'
    logging.info('Appending divide column')
    df = append_divide_column(df, num_files, div_col_name)
    table = pa.Table.from_pandas(df)
    logging.info('Writing dataset')
    pq.write_to_dataset(table, root_path=args.output,
                        partition_cols=[div_col_name])

    logging.info('Dataset written: %s' % (os.path.isdir(args.output)))
