import pandas as pd
import numpy as np
import os
import math
import itertools
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds


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


if __name__ == '__main__':
    '''
    Given a large file, find the number of files it needs to be divided into, and then evenly distribute those files into partitioning
    '''
    large_file_path = 'output_2019_q1.parquet'
    num_files = calculate_number_of_files(large_file_path)
    print('Number of files to divide into: %s' % num_files)
    print('Reading table')
    table = pq.read_table(large_file_path)
    df = table.to_pandas()
    div_col_name = '_tmp-divide'
    print('Appending divide column')
    df = append_divide_column(df, num_files, div_col_name)
    table = pa.Table.from_pandas(df)
    print('Writing dataset')
    # ds.write_dataset(table, "savedir", format="parquet",
    #                  partitioning=ds.partitioning(
    #                      pa.schema([table.schema.field(div_col_name)])
    #                  ))
    dataset_name = 'ookla_dataset'
    pq.write_to_dataset(table, root_path=dataset_name,
                        partition_cols=[div_col_name])

    print('Dataset written: %s' % (os.path.isdir(dataset_name)))
    dataset = pq.ParquetDataset(dataset_name)
    table = dataset.read()
    print(table)
    print('Test complete')
