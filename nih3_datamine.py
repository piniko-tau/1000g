#!/usr/bin/env python


import sys
import re
import multiprocessing as mp
import argparse
import logging
from progressbar import AnimatedMarker, Bar, BouncingBar, Counter, ETA, FileTransferSpeed, FormatLabel, Percentage, ProgressBar, ReverseBar, RotatingMarker, SimpleProgress, Timer
import datetime
import pandas as pd

#__author__ = 'piniko'


'''
data mine the nih3 data
docs link :
https://docs.google.com/document/d/1pkyGsyIXzsuNYPYi3CloR6dfBdw3wkM4R5tqZFoZjaI/edit
'''


parser = argparse.ArgumentParser(prog='nih3_datamine',usage='python nih3_datamine.py ..args \
[-nih3_file file to process]\
[-args.analyse_col_file]\
',\
description='Load annotated snp database & Create a 1000G sql table from all Chromosomes - using a connection to a postgresql DB.')
parser.add_argument("-analyse_col_file",help='analyse_col_file print col var table',metavar='analyse_col_file')
parser.add_argument("-nih3_file",help='nih3_file file to process',metavar='nih3_file')
parser.add_argument("-v", "--verbose", help="increase output verbosity",action="store_true")
args = parser.parse_args()

#setup logging option
if args.verbose:
    logging.basicConfig(level=logging.DEBUG)

if args.analyse_col_file:

    pd1=pd.read_csv(args.analyse_col_file,sep=' ')

    for col in pd1:
        print (pd1[col].value_counts())


if args.nih3_file:

    '''   -retain only amino change, order by codons by abc, order by chrom pos asc'''
