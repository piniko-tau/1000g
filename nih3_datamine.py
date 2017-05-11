#!/usr/bin/python


import sys
import re
import multiprocessing as mp
import argparse
import logging
import datetime
import pandas as pd
from random import randint

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

    pd1=pd.read_csv(args.analyse_col_file, sep='\t')

    for col in pd1:
        print (pd1[col].value_counts())

if args.nih3_file:
    pd1=pd.read_csv(args.nih3_file, sep='\t')
    for nrow in range(pd1.shape[0]):
        if pd1.loc[nrow,'REF'] == pd1.loc[nrow,'ALT']:
            print(pd1.loc[nrow,'REF']+pd1.loc[nrow,'ALT']+"drop row "+nrow)
            pd2=pd1.drop(pd1.index[nrow])
    # with open('vcf_nih3_file.out','w') as f:

        # for index, row in pd1.iterrows():
        #    # print row['ID'],row['REF'],row['ALT']
        #     if (not row['REF'] == row['ALT']) and (not '.'== row['ALT'] or row['REF'] =='.'):
        #         print row
        #         f.write(str(row))

    #add ok rows to empty dataframe
#next join pd.merge between rsid_nih3 and the rsid_gene
#pd.merge(df_a, df_b, on='rsid', how='inner')