#!/usr/bin/env python3
"""
@author RW Taggart
@date   Apr. 2021

CSV random data generator
Simple example to randomly populate pandas dataframes with column names

This is intended to provide data for pandas benchmarking. It will support the following configurations:
  - rows: 10, 100, 1k, 100k, 1M
  - cols: 2, 5, 10, 100
  - dtype: int, float, str, % of each

Usage:
  python gen_data.py --generate (-g) --sizes (-s) [(10,10), (1000,10), (10000, 20), (100000, 50), (500000, 75), (1000000, 100)]
  ./samples
    ./a_1000x10.csv
    ./b_1000x10.csv


CSV data comparator
Simple example to diff two csv files. This has multiple implementations with the intention of comparing performance.

This is intended to provide data for pandas benchmarking. It will support the following configurations:
  - rows: 10, 100, 1k, 100k, 1M
  - cols: 2, 5, 10, 100
  - dtype: int, float, str, % of each

Experiment:
  1. Generate 2 CSV files of same shape
  2. Read CSV files into dataframe
  3. Run comparison using 3 techniques:
    a. Column op  (vector-wise)
    b. apply      (element-wise)
    b. for each   (element-wise)
"""
##### EXAMPLES: #####
# GENERATE: 
#   $ python3 cmp_data.py --generate -s "[(10,10), (1000,10), (10000, 20), (100000, 50), (500000, 75), (1000000, 100)]" 
# ANALYZE:
#   $ python3 cmp_data.py --analyze -d ./samples


import os
from sys import stderr
import logging
import argparse
import json
import math
import time
import re
from glob import glob
from collections import defaultdict
from pandas import read_csv, concat, DataFrame
from numpy.random import default_rng


logger = logging.getLogger(__name__)
DEFAULT_OUTPUT_DIR = './samples'
DEFAULT_PREFIXES = 'a,b'

def make_dir(path_name):
    if not os.path.exists(os.path.join(path_name)):
        try:
            logger.info(f'(I): Creating dir: "{path_name}"')
            os.makedirs(os.path.join(path_name))
        except OSError as err: # Guard against race condition
            logger.error(f'(E): Found an error: {err}')
            if err.errno != errno.EEXIST:
                raise

def elapsed(t): 
    return str(int(math.floor(t / 3600))) + 'h ' + str(int(math.floor(t % 3600 / 60))) + 'm ' + str(round(t % 3600 % 60, 3)) + 's'

class Timer:
    '''Timer will measure the execution time of some python code as a context manager.
        Usage:
        with Timer('task A'):
            pass
    '''
    def __init__(self, name:str=None, print_on_exit:bool=True):
        self.name = name
        self.print_on_exit = print_on_exit
    def __enter__(self):
        self.start = time.time()
        self.end = None
        return self
    def __exit__(self, *args):
        self.end = time.time()
        if self.print_on_exit:
            print('(T):  ', self)
    @property
    def interval(self):
        if self.end is not None:
            return self.end - self.start
        else:
            return time.time() - self.start
    def __str__(self):
        return f'{self.name} took {elapsed(self.interval)} ({round(self.interval, 3)}s)'


def cmp_for_col_row_elem(dfa, dfb):
    """ Iterate over columns and rows to compare element-wise """
    if not dfa.columns.equals(dfb.columns):  raise ValueError('DataFrame columns must be equivalent')
    diffs = defaultdict(list)
    for c in dfa.columns:
        for i, ri_a in enumerate(dfa[c]):
            diffs[c].append(dfb[c][i] - ri_a)
    return diffs


def cmp_for_col_apply_elem(dfa, dfb):
    """ Iterate over columns and use DataFrame.apply() to compare element-wise """
    if not dfa.columns.equals(dfb.columns):  raise ValueError('DataFrame columns must be equivalent')
    diffs = defaultdict(list)
    cols = dfa.columns
    dfA = dfa.add_suffix('_a')
    dfB = dfa.add_suffix('_b')
    dfM = concat([dfA, dfB], axis=1)
    for c in cols:
        diffs[c].append(dfM.apply(lambda row: row[c+'_a'] - row[c+'_b'], axis=1))  # element-wise, despite using .apply()
    return diffs


def cmp_for_col_vec(dfa, dfb):
    """ Iterate over columns to compare vector-wise """
    if not dfa.columns.equals(dfb.columns):  raise ValueError('DataFrame columns must be equivalent')
    diffs = defaultdict(list)
    for c in dfa.columns:
        diffs[c] = dfb[c] - dfa[c]
    return diffs

def cmp_df_compare(dfa, dfb):
    """ use DataFrame.compare() """
    if not dfa.columns.equals(dfb.columns):  raise ValueError('DataFrame columns must be equivalent')
    return dfa.compare(dfb)


# def cmp_series_apply(dfa, dfb):
#     """ use Series.apply() to compare element-wise """
#     # FIXME: I can't figure this one out...
#     if not dfa.columns.equals(dfb.columns):  raise ValueError('DataFrame columns must be equivalent')
#     diffs = defaultdict(list)
#     for c in dfa.columns:
#         dfa[c].apply(lambda ri: )


# def cmp_df_apply(dfa, dfb):
#     """ use DataFrame.apply() to compare vector-wise """
#     # FIXME: I can't figure this one out...
#     if not dfa.columns.equals(dfb.columns):  raise ValueError('DataFrame columns must be equivalent')
#     diffs = defaultdict(list)
#     for c in dfa.columns:
#         dfa[c].apply(lambda ri: , axis=0)


def gen_data(sizes, prefixes, odir):
    make_dir(odir)
    randGen = default_rng()
    print(f"(I): Generating data for {prefixes} and {','.join([str(s) for s in sizes])}.")
    for p in prefixes:
        for s in sizes:
            fname = f"{p}_{s[0]}x{s[1]}.csv"
            print(f'(D): {fname}')
            col_names = ['c'+ str(i) for i in range(s[1])]
            sample_ints = randGen.integers(10, 10000, (s[0], s[1]))
            d = DataFrame(data=sample_ints, columns=col_names)
            d.to_csv(os.path.join(odir, fname), index=False)


def analyze_data(file_a, file_b):
    with Timer('read csv'):
        df_a = read_csv(file_a)
        df_b = read_csv(file_b)
    # t_end_read = time()
    # print(f'(T): read csv - {elapsed(t_end_read - t_st_read)}')
    print(f'(I): Analyzing DataFrames {df_a.shape}, {df_b.shape}.')
    with Timer('"for each column, for each row" element-wise'):
        d_for_elem = cmp_for_col_row_elem(df_a, df_b)
    with Timer('"for each column with apply" element-wise'):
        d_for_apply_elem = cmp_for_col_apply_elem(df_a, df_b)
    with Timer('"for each column" vector-wise'):
        d_for_col_vec = cmp_for_col_vec(df_a, df_b)
    with Timer('DataFrame.compare()'):
        d_for_col_vec = cmp_df_compare(df_a, df_b)
    


def parse_args():
    parser = argparse.ArgumentParser(description='Pandas DataFrame manipulation performance benchmarking')
    mode_g = parser.add_mutually_exclusive_group()
    mode_g.add_argument('-g', '--generate', action='store_true', help="Generate randomly sampled data. See 'Generate Data' section.")
    mode_g.add_argument('-a', '--analyze', action='store_true', help="Analyze Pandas DataFrame performance. See 'Analyze Data' section.")

    generate_g = parser.add_argument_group("Generate Data", description='Generate pandas DataFrame with random sample data')
    generate_g.add_argument('-o', '--output', default=DEFAULT_OUTPUT_DIR, help=f"path to output directory (default: '{DEFAULT_OUTPUT_DIR}'")
    generate_g.add_argument('-p', '--prefixes',  default=DEFAULT_PREFIXES, help=f"prefixes to use for file names (default: '{DEFAULT_PREFIXES}')")
    generate_g.add_argument('-r', '--rows', type=int, help=f"Number of rows to generate.")
    generate_g.add_argument('-c', '--cols', type=int, help=f"Number of columns to generate.")
    generate_g.add_argument('-s', '--sizes', type=str, help=f"List of tuples correspnding to file sizes (format: '[(100,10), (1000,20)]').")
    generate_g.add_argument('-t', '--types', help=f"string describing the data types")

    analyze_g = parser.add_argument_group("Analyze Data")
    analyze_g.add_argument('-f1', '--file1', help=f"path to file A")
    analyze_g.add_argument('-f2', '--file2', help=f"path to file B")
    analyze_g.add_argument('-d', '--dir', help=f"path to directory to load data")
    analyze_g.add_argument('-m', '--method', help=f"Method to use")
    analyze_g.add_argument('--limit', type=int, help=f"Limit number of data sets to analyze")

    args = parser.parse_args()
    if args.types is not None:
        raise NotImplementedError('Invalid options: -t and --types is not yet supported.')
    if args.method is not None:
        raise NotImplementedError('Invalid options: -m and --method is not yet supported.')
    if args.generate:
        if not (args.sizes or (args.cols and args.rows)):
            print('(E): Invalid Arguments: either (--rows and --cols) or --sizes are required with --generate')
            exit(1)
    if args.analyze:
        if not (args.dir or (args.file1 and args.file2)):
            print('(E): Invalid Arguments: --dir or (--file1 and --file2) are required with --analyze')
            exit(1)
    return args


def split_size(s):
    r,c = s.split('x')
    return int(r) * int(c)


if __name__ == "__main__":
    t_st_main = time.time()
    args = parse_args()
    if args.generate:
        with Timer('Generate Data'):
            sizes = list()
            if args.sizes:
                sizes.extend(json.loads(args.sizes.replace('(','[').replace(')',']')))
            if args.rows and args.cols:
                sizes.append((args.rows, args.cols))
            prefixes = args.prefixes.split(',')
            gen_data(sizes, prefixes, os.path.abspath(args.output))
    else:
        if args.dir:
            files = glob(args.dir+'/*.csv')
            sizes_d = defaultdict(list)
            for fname in files:
                r = re.compile('(\w+)_(\d+x\d+)\.csv')
                m = r.match(os.path.basename(fname))
                m_groups = m.groups()
                f_size = os.stat(fname).st_size
                sizes_d[m_groups[1]].append((fname, m_groups[0], m_groups[1], f_size))
            for i, size_k in enumerate(sorted(sizes_d.keys(), key=split_size)):
                if args.limit and i >= args.limit:  break
                f_data = sizes_d[size_k]
                print(f'(I): Analyzing {size_k} with {len(f_data)} files of size {[str(round(d[3] / 1e3, 3))+"K" for d in f_data]}.')
                analyze_data(f_data[0][0], f_data[1][0])
                # reversed(sorted(sizes.keys()))
        else:
            analyze_data(args.file1, args.file2)
    t_end_main = time.time()
    print(f'(T): __main__ - {elapsed(t_end_main - t_st_main)}')

