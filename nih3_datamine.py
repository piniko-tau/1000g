#!/usr/bin/python


import sys
import re
import multiprocessing as mp
import argparse
import logging
import datetime
import pandas as pd
from random import randint
import urllib
import os.path
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
parser.add_argument("-rsid_2_gene_table",help='get rsid to gene table from ensemble',action="store_true")
parser.add_argument("-rsid_2_gene_table_test",help='get rsid to gene table from ensemble',action="store_true")
parser.add_argument("-get_peptides_table",help='get peptide table from ucsc',action="store_true")
parser.add_argument("-nih3_merge_genemnames",help='inner join nih3 and variation_genenames',action="store_true")
parser.add_argument("-nih3_merge_peptides",help='inner join nih3 and ucsc peptides',action="store_true")
parser.add_argument("-nih3_make_ml_table",help='nih3_make_ml_table',action="store_true")
parser.add_argument("-to_csv",help='write merge to file',action="store_true")
parser.add_argument("-v", "--verbose", help="increase output verbosity",action="store_true")
args = parser.parse_args()

#functions:
def get_df_memory_usage(df1):
    print '\nmem usage:\n'
    print (df1.memory_usage(index=True).sum())
    print '\n\n'

#setup logging option
if args.verbose:
    logging.basicConfig(level=logging.DEBUG)

if args.analyse_col_file:

    pd1=pd.read_csv(args.analyse_col_file, sep='\t')

    for col in pd1:
        print (pd1[col].value_counts())


if args.nih3_file:
    pd1=pd.read_csv(args.nih3_file, sep='\t')
    drop_row_list=[]
    # print(pd1.to_string())
    for nrow in range(pd1.shape[0]):
        if pd1.loc[nrow,'REF'] == pd1.loc[nrow,'ALT']:
            print(pd1.loc[nrow,'REF']+pd1.loc[nrow,'ALT']+"drop row "+str(nrow))
            drop_row_list.append(pd1.index[nrow])
            print(pd1.index[nrow])
    pd2=pd1.drop(pd1.index[drop_row_list])

if args.rsid_2_gene_table:
    if not os.path.exists("./variation.txt.gz"):
        print "getting variation.txt.gz from ensemble..."
        urllib.urlretrieve("ftp://ftp.ensembl.org/pub/release-75/mysql/homo_sapiens_variation_75_37/variation.txt.gz", "variation.txt.gz")
        print "done."
    if not os.path.exists("./variation_genename.txt.gz"):
        print "getting variation_genename.txt.gz from ensemble..."
        urllib.urlretrieve("ftp://ftp.ensembl.org/pub/release-75/mysql/homo_sapiens_variation_75_37/variation_genename.txt.gz", "variation_genename.txt.gz")
        print "done."
    #join the two tables compression='gzip'
        #get dataframes from the two files
    print "loading variation.txt.gz into DF"
    pd3=pd.read_csv('./variation.txt.gz',names=['variation_id','source_id','rs_name','validation_status','ancestral_allele','flipped','class_attrib_id','somatic','minor_allele','minor_allele_freq','minor_allele_count','clinical_significance','evidence'],compression='gzip', sep='\t')
    print "done"
    print "loading variation_genename.txt.gz into DF"
    pd4=pd.read_csv('./variation_genename.txt.gz',names=['variation_id','gene_name'],compression='gzip', sep='\t')
    print "done"
    # #merge on id
    print "merging variation_genename and variation on variation id , to get gene names with rsids"
    pd34=pd.merge(pd3, pd4, on='variation_id', how='inner')
    print "done."

if args.rsid_2_gene_table_test:
    #join the two tables compression='gzip'
        #get dataframes from the two files
    print "loading variation_10n into DF"
    pd3=pd.read_csv('./variation_10n',names=['variation_id','source_id','rs_name','validation_status','ancestral_allele','flipped','class_attrib_id','somatic','minor_allele','minor_allele_freq','minor_allele_count','clinical_significance','evidence'], sep='\t')
    print "done"
    print "loading variation_genename_10n into DF"
    pd4=pd.read_csv('./variation_genename_10n',names=['variation_id','gene_name'], sep='\t')
    print "done"
    # #merge on id
    print "merging variation_genename and variation on variation id , to get gene names with rsids"
    pd34=pd.merge(pd3, pd4, on='variation_id', how='inner')
    print "done."
    print '\nrsid_2_gene_table_test :\n'
    print(pd34.to_string())


if args.get_peptides_table:
    if not os.path.exists("./snp141CodingDbSnp.txt.gz"):
        print "getting snp141CodingDbSnp.txt.gz from ucsc..."
        urllib.urlretrieve('http://hgdownload.cse.ucsc.edu/goldenpath/hg19/database/snp141CodingDbSnp.txt.gz', "snp141CodingDbSnp.txt.gz")
        print "done."
        print "loading snp141CodingDbSnp into DF"
        #here :
        pd3=pd.read_csv('./snp141CodingDbSnp.txt.gz',names=['smallint','chrom','chromStart','chromEnd','name','transcript','frame','alleleCount',
    'funcCodes','alleles','codons','peptides','peptide1','peptide2','peptide3'],compression='gzip', sep='\t')
        print "done"


if args.nih3_merge_genemnames:
    #merge on rs_name of pd34 and ID of pd2
    pd234=pd.merge(pd2, pd34, left_on='ID',right_on='rs_name', how='inner')
    print '\nnih3_merge_genemnames :\n'
    print(pd234.to_string())

if args.nih3_make_ml_table:
    #SZPABR0002
    print'ml table\n\n'
    print(pd234['ID'])
    print(pd234['gene_name'])
    print(pd234.groupby('gene_name')['ID'].apply(list))
    pd6=pd234.groupby('gene_name')['ID'].apply(list)
    pd7=pd234.add(pd6)
    print(pd7.to_string())

if args.nih3_merge_peptides:
    pass


if args.to_csv:
    pd234.to_csv('out.csv',index=False)


#add ok rows to empty dataframe
#next join pd.merge between rsid_nih3 and the rsid_gene
#pd.merge(df_a, df_b, on='rsid', how='inner')