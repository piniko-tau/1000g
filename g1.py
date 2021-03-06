#!/usr/bin/env python


import sys
import re
import multiprocessing as mp

#__author__ = 'piniko'

'''

#on the psql shell
#create role pyuser login password 'pyuser';
#create database pydb owner pyuser;
#\l \dg
'''

import psycopg2
from psycopg2.extensions import AsIs
import argparse
import logging
from progressbar import AnimatedMarker, Bar, BouncingBar, Counter, ETA, FileTransferSpeed, FormatLabel, Percentage, ProgressBar, ReverseBar, RotatingMarker, SimpleProgress, Timer
import datetime

parser = argparse.ArgumentParser(prog='psql_1000g_loader',usage='psql_1000g_loader \
 [-t table_name prefix -f file_input or -list file_input_list]\
 [-ucsc_snpf file name -ucsc_snp table_name] (optional add a annotated ucsc_snp table from file ) \
 [-dbname database_name -dbuser database_user -dbpass database_pass] \
 [-a_ucsc chr table to be annotated by ucsc ]\
 [-ensembl_variation_snpf file name -ensembl_variation_genename_snpf file name] (optional add a annotated ensembl tables from files) \
 [-a_ensembl chr table to be annotated by ensemble ] \
 [-sort_by_gene_and_pos ann_table]\
 [-update_table_allel2peptide create all to peptide table] \
 [-remove_dup_allele remove duplicate alleles from table] \
 [-add_gene_peptide_string add gene_peptide_string fileds to table] \
 [-create_uniq_pepstring_num create a table with unique number to each group of peptide strings ordered by descending] \
 [-add_uniq_pepstring_num add unique pepstring number to specified table] \
 [-export_sample_2file export 100 lines> of each table to file] \
 [-export_fulldataset_2file export dataset in full to file name] \
 [-create_ml_dataset_table create dataset for machine learning by patients table]\
 [-export_ml_full_dataset export dataset for machine learning by patients ] \
 [-mind_data_preprocess]\
 [-load_mind_data_f load mind dataset file] \
 [-load_mind_data_t load mind dataset table prefix] \
 [-load_mind_rsids load the mind rsids file to mind_rsids table] \
 [-join_mind_with_with_anno_ucsc_snp annotate mind table with peptide data] \
 [-join_mind_rsids table to join mind data with rsids] \
 [-mind_a_ensembl annotate mind table with ensemble] \
 [-mind_sort_by_gene_and_pos ann_table]\
 [-mind_prepare_ucsc_4_mind_annotations table]\
 [-mind_update_table_allel2peptide create all to peptide table] \
 [-s show all tables]\
 [-load_DBIdb_tables_preprocess]\
 [-create_drugs_genes_table]\
 [-create_drugs_genes_table_extended_gene_interactions]\
 [-filter_mind_table_by_drugs]\
 [-filter_mind_table_by_drugs_extended_gene_interactions]\
 [-mind_export_ml export a ml dataset of mind data]\
 [-mind_export_ml_with_drugs export a ml dataset of mind data with drug fields]\
 [-mind_export_ml_with_drugs_u NOT FINISHED export a ml dataset of mind data with drug fields and unique alt genes]\
 [-mind_export_ml_with_drugs_alt export a ml dataset of mind data with alt_drug fields]\
 [-add_meta add tables metadata]',\




 description='Load annotated snp database & Create a 1000G sql table from all Chromosomes - using a connection to a postgresql DB.')


# dbname=pydb user=pyuser password=pyuser
# postgresql credentials
credentialsparser.add_argument("-dbname",required=True,help='name of psql database',metavar='DBNAME')
parser.add_argument("-dbuser",required=True,help='name of psql database user',metavar='DBUSER')
parser.add_argument("-dbpass",required=True,help='psql database user pass',metavar='DBPASS')


#1000g actions
parser.add_argument("-f",help='vcf file to load into psql database',metavar='VCF_FILE')
parser.add_argument("-t",help='destination vcf table prefix on the psql database',metavar='PSQL_TABLE_PREFIX')
parser.add_argument("-list",help='file containing a list of files to load into psql database',metavar='VCF_FILE_LIST')

#UCSC's snp141CodingDbSnp annotated sr's actions: add amino acid annotation
parser.add_argument("-ucsc_snpf",help='a ucsc file to load into snp annotated table',metavar='UCSC_FILE')
parser.add_argument("-ucsc_snpt",help='destination table name for the snp annotated table',metavar='UCSC_PSQL_TABLE')

#add tables metadata : source , version of each table
parser.add_argument("-add_meta",help='add tables metadata : source , version of each table',action="store_true")


#ENSEMBL variation_  annotated sr's actions : combine gene_name and clinical attribute with rsid
parser.add_argument("-ensembl_variation_snpf",help='variation ensembl file to combine into ensembl snp annotated table',metavar='ENSEMBL_VARIATION_FILE')
parser.add_argument("-ensembl_variation_genename_snpf",help='variation genename ensembl file to combine into ensembl snp annotated table',metavar='ENSEMBL_VARIATION_GENENAME_FILE')

#add show tables option
parser.add_argument("-s", "--show_tables", help="show any existing tables",action="store_true")

#add ucsc annotation option with snp table input and chr table input
parser.add_argument("-a_ucsc",help='chr table to be annotated',metavar='ANN_TABLES_UCSC')

#sort specifiedn annotated table by chromStart - chrom position and create a new table
parser.add_argument("-sort",help='annotated table to be sorted by position',metavar='SORT_TABLES')

#sort specifiedn annotated table by chromStart - chrom position and create a new table
parser.add_argument("-sort_by_gene_and_pos",help='annotated table to be sorted by gene name and position',metavar='SORT_TABLES')

#add ensembl annotation option with snp table input and chr table input
parser.add_argument("-a_ensembl",help='chr table to be annotated',metavar='ANN_TABLES_ENSEMBL')

parser.add_argument("-update_table_allel2peptide",help=' create all to peptide table',metavar='UPDATE_ALLELE2PEP')

parser.add_argument("-remove_dup_allele",help='remove duplicate alleles from table',metavar='REMOVE_DUP_ALLELE')

parser.add_argument("-add_gene_peptide_string",help=' add gene_peptide_string fileds to table',metavar='ADD_GENE_PEPTIDE_STRING')

parser.add_argument("-create_uniq_pepstring_num",help=' create a table with unique number to each group of peptide strings ordered by descending',action="store_true")

parser.add_argument("-add_uniq_pepstring_num",help='add unique pepstring number to specified table',metavar='ADD_UNIQ_PEPSTRING_NUM')


parser.add_argument("-multi_core_num",help='use multiple cores number or \"max\"',metavar='MULTI_CORE_NUM')

parser.add_argument("-export_sample_2file",help='export sample to file name',metavar='export_sample_2file')

parser.add_argument("-export_fulldataset_2file",help='export dataset in full to file name',metavar='export_datasetfull_2file')

parser.add_argument("-create_ml_dataset_table", help="create dataset for machine learning by patients table",metavar='create_ml_dataset_table')

parser.add_argument("-export_ml_full_dataset", help="export dataset for machine learning by patients ",metavar='export_ml_full_dataset')

parser.add_argument("-load_mind_data", help="load mind dataset file" , metavar='load_mind_data')

parser.add_argument("-mind_data_preprocess",help="preprocess mind how to",metavar='mind_data_preprocess')

parser.add_argument("-load_mind_data_f", help="load mind dataset file" , metavar='load_mind_data_f')

parser.add_argument("-load_mind_data_t",help=" load mind dataset table prefix",metavar='load_mind_data_t')

parser.add_argument("-join_mind_with_with_anno_ucsc_snp",help=" annotate mind table with peptide data",metavar='join_mind_with_with_anno_ucsc_snp')

parser.add_argument("-load_mind_rsids",help=" load the mind rsids file to mind_rsids table",metavar='load_mind_rsids')

parser.add_argument("-join_mind_rsids",help=" table to annotate join mind data with rsids",metavar='join_mind_rsids')

parser.add_argument("-mind_a_ensembl",help=" annotate mind table with ensemble ",metavar='mind_a_ensembl')

parser.add_argument("-mind_sort_by_gene_and_pos",help='annotated table to be sorted by gene name and position',metavar='SORT_TABLES')

# mind_prepare_ucsc_4_mind_annotations
parser.add_argument("-mind_prepare_ucsc_4_mind_annotations",help='preprocess ucsc table for mind ann',metavar='mind_prepare_ucsc_4_mind_annotations')

parser.add_argument("-load_DBIdb_tables_preprocess",help='list how to load_DBIdb_tables_preprocess',metavar='load_DBIdb_tables_preprocess')

parser.add_argument("-create_drugs_genes_table",help='create_drugs_genes_table',action="store_true")

parser.add_argument("-create_drugs_genes_table_extended_gene_interactions",help='create_drugs_genes_table_extended_gene_interactions',action="store_true")

parser.add_argument("-filter_mind_table_by_drugs",help='filter_mind_table_by_drugs',metavar='filter_mind_table_by_drugs')

parser.add_argument("-filter_mind_table_by_drugs_extended_gene_interactions",help='filter_mind_table_by_drugs_extended_gene_interactions',metavar='filter_mind_table_by_drugs_extended_gene_interactions')

parser.add_argument("-mind_export_ml",help='export a ml dataset of mind data',metavar='mind_export_ml')

parser.add_argument("-mind_export_ml_with_drugs",help='export a ml dataset of mind data with drug fields',metavar='mind_export_ml_with_drugs')

parser.add_argument("-mind_export_ml_with_drugs_u",help='NOT FINISHED export a ml dataset of mind data with drug fields',metavar='mind_export_ml_with_drugs_u')

parser.add_argument("-mind_export_ml_with_drugs_alt",help='export a ml dataset of mind data with alt_drug fields',metavar='mind_export_ml_with_drugs_alt')

parser.add_argument("-o", "--overwrite_tables", help="overwrites any existing tables",action="store_true")
parser.add_argument("-v", "--verbose", help="increase output verbosity",action="store_true")
args = parser.parse_args()

#setup logging option
if args.verbose:
    logging.basicConfig(level=logging.DEBUG)




# DB vars :
dbname = args.dbname
dbuser = args.dbuser
dbpass = args.dbpass


#connection stuff
conn = psycopg2.connect("dbname="+dbname+" user="+dbuser+" password="+dbpass+" host=localhost")
cur = conn.cursor()

#variable to check table override
skip_next_table_creation = False

#setup ovwrwrite option
if args.overwrite_tables:
    overwrite_tables_is_set = True
    print("overwrite_tables_is_set")
else:
    overwrite_tables_is_set = False
    # print("overwrite_tables_is_NOT_set")

#counter col
column_limit = 20
column_limit_counter = 0
column_variable_counter = 0
#############################################################################################################################################

def insert_snp_table():


    print cur.mogrify("CREATE TABLE \"%s\" (\"bin\" smallint NOT NULL,\"chrom\" varchar(255) NOT NULL,\"chromStart\" int  NOT NULL,\"chromEnd\" int  NOT NULL, \"name\" text NOT NULL,\"transcript\" varchar(255) NOT NULL,\"frame\" varchar(255) NOT NULL,\"alleleCount\" int NOT NULL,\"funcCodes\" text NOT NULL,\"alleles\" text NOT NULL,\"codons\" text NOT NULL,\"peptides\" text NOT NULL)",(AsIs(snptable),))
    cur.execute("CREATE TABLE \"%s\" (\"bin\" smallint NOT NULL,\"chrom\" varchar(255) NOT NULL,\"chromStart\" int  NOT NULL,\"chromEnd\" int  NOT NULL, \"name\" text NOT NULL,\"transcript\" varchar(255) NOT NULL,\"frame\" varchar(255) NOT NULL,\"alleleCount\" int NOT NULL,\"funcCodes\" text NOT NULL,\"alleles\" text NOT NULL,\"codons\" text NOT NULL,\"peptides\" text NOT NULL)",(AsIs(snptable),))
    print cur.mogrify("COPY \"%s\" FROM \'%s\'",(AsIs(snptable),AsIs(snpfile),))
    cur.copy_from(fsnpfile, snptable)
    print cur.mogrify("CREATE TABLE %s AS SELECT * FROM \"%s\" WHERE frame NOT LIKE \"n/a\"",(AsIs(snptablena),AsIs(snptable),))
    cur.execute("CREATE TABLE %s AS SELECT * FROM \"%s\" WHERE frame NOT LIKE \'n/a\'",(AsIs(snptablena),AsIs(snptable),))
    print  cur.mogrify("CREATE TABLE %s AS SELECT * FROM %s WITH NO DATA",(AsIs(snptablenasma),AsIs(snptablena),))
    cur.execute("CREATE TABLE %s AS SELECT * FROM %s WITH NO DATA",(AsIs(snptablenasma),AsIs(snptablena),))
    print  cur.mogrify("ALTER TABLE %s "
                       "UMN peptide1 char(1), add column peptide2 char(2), add column peptide3 char(3)",(AsIs(snptablenasma),))
    cur.execute("ALTER TABLE %s ADD COLUMN peptide1 char(1), add column peptide2 char(2), add column peptide3 char(3)",(AsIs(snptablenasma),))
    print  cur.mogrify("INSERT INTO %s SELECT * FROM ( SELECT * , SUBSTR(peptides,1 , 1) AS peptide1 , SUBSTR(peptides,3 , 1) AS peptide2 , SUBSTR(peptides,5 , 1) AS peptide3 FROM %s ) AS ptt WHERE (peptide1!=peptide2) OR (peptide1!=peptide3) OR (peptide2!=peptide3)",(AsIs(snptablenasma),AsIs(snptablena),))
    cur.execute("INSERT INTO %s SELECT * FROM ( SELECT * , SUBSTR(peptides,1 , 1) AS peptide1 , SUBSTR(peptides,3 , 1) AS peptide2 , SUBSTR(peptides,5 , 1) AS peptide3 FROM %s ) AS ptt WHERE (peptide1!=peptide2) OR (peptide1!=peptide3) OR (peptide2!=peptide3)",(AsIs(snptablenasma),AsIs(snptablena),))
    print  cur.mogrify("CREATE TABLE %s AS SELECT * FROM %s ORDER BY codons",(AsIs(snptable_s_c),AsIs(snptablenasma),))
    cur.execute("CREATE TABLE %s AS SELECT * FROM %s ORDER BY codons",(AsIs(snptable_s_c),AsIs(snptablenasma),))
    print  cur.mogrify("CREATE TABLE %s AS SELECT * FROM %s ORDER BY \"chromStart\"",(AsIs(snptable_s_ch_filtered_final),AsIs(snptable_s_c),))
    cur.execute("CREATE TABLE %s AS SELECT * FROM %s ORDER BY \"chromStart\"",(AsIs(snptable_s_ch_filtered_final),AsIs(snptable_s_c),))
    conn.commit()

def uscs_split_allels(ucsc_table):

    global ucsc_table_split_alleles
    ucsc_table_split_alleles=ucsc_table+"_spltal"
    check_overwrite_table(ucsc_table_split_alleles)

    print (cur.mogrify("create table %s as SELECT *  FROM ( SELECT * , SUBSTR(alleles,1 , 1) AS allele1 , SUBSTR(alleles,3 , 1) AS allele2 , SUBSTR(alleles,5 , 1) AS allele3 FROM %s) as t;",(AsIs(ucsc_table_split_alleles),AsIs(ucsc_table),)))
    cur.execute("create table %s as SELECT *  FROM ( SELECT * , SUBSTR(alleles,1 , 1) AS allele1 , SUBSTR(alleles,3 , 1) AS allele2 , SUBSTR(alleles,5 , 1) AS allele3 FROM %s) as t;",(AsIs(ucsc_table_split_alleles),AsIs(ucsc_table),))
    conn.commit()

def ucsc_add_op_allele_strand(ucsc_table2):

    print (cur.mogrify("ALTER TABLE %s ADD COLUMN opallele1 text, add column opallele2 text, add column opallele3 text",(AsIs(ucsc_table2),)))
    cur.execute("ALTER TABLE %s ADD COLUMN opallele1 text, add column opallele2 text, add column opallele3 text",(AsIs(ucsc_table2),))
    conn.commit()

    print (cur.mogrify("update %s set opallele1 = (case when (allele1='G') then  'C' when (allele1='C') then  'G' when (allele1='A') then  'T' when (allele1='T') then  'A' end),opallele2 = (case when (allele2='G') then  'C' when (allele2='C') then  'G' when (allele2='A') then  'T' when (allele2='T') then  'A' end),opallele3 = (case when (allele3='G') then  'C' when (allele3='C') then  'G' when (allele3='A') then  'T' when (allele3='T') then  'A'  end)",(AsIs(ucsc_table2),)))
    cur.execute("update %s set opallele1 = (case when (allele1='G') then  'C' when (allele1='C') then  'G' when (allele1='A') then  'T' when (allele1='T') then  'A' end),opallele2 = (case when (allele2='G') then  'C' when (allele2='C') then  'G' when (allele2='A') then  'T' when (allele2='T') then  'A' end),opallele3 = (case when (allele3='G') then  'C' when (allele3='C') then  'G' when (allele3='A') then  'T' when (allele3='T') then  'A'  end)",(AsIs(ucsc_table2),))
    conn.commit()

def cleanup_err_tables():

    del_tables_list = (snptable,snptablena,snptablenasma,snptable_s_c,snptable_s_ch_filtered_final)
    for table in del_tables_list:
        if check_table_exists(table):
            print(cur.mogrify("drop table "+table+";"))
            cur.execute("drop table "+table+";")
            conn.commit()


    for table in table1000g_list:
        print("table to be deleted "+table)
        if check_table_exists(table):
            print(cur.mogrify("drop table "+table+";"))
            cur.execute("drop table "+table+";")
            conn.commit()

def cleanup_snp_tables():

    snp_del_tables = (snptable,snptablena,snptablenasma,snptable_s_c)
    for table in snp_del_tables:
        check_table_exists(table)
        print(cur.mogrify("drop table "+table+";"))
        cur.execute("drop table "+table+";")
        conn.commit()


def check_table_exists(table1):
    cur.execute("select * from information_schema.tables where table_name=\'%s\'",(AsIs(table1),))
    return bool(cur.fetchall())

def delete_table(table3):
    print(cur.mogrify("drop table "+table3+";"))
    cur.execute("drop table "+table3+";")
    conn.commit()

def check_empty_table(table2):
    cur.execute("select * from %s limit 1;" , (AsIs(table2),))
    return str(bool(cur.fetchall()))

def create_table(table4):

    ##delete if overwrite_tables_is_set is set here!
    print(cur.mogrify("create table "+table4+"();"))
    cur.execute("create table "+table4+"();")
    conn.commit()

def addcol_2table (column_name,table_name):
   logging.debug(cur.mogrify("alter table %s add column %s text;", (AsIs(table_name),AsIs(column_name),)))
   cur.execute("alter table %s add column %s text;", (AsIs(table_name),AsIs(column_name),))
   conn.commit()

def addcol (column_name):
   logging.debug(cur.mogrify("alter table "+table1000g+" add column %s text;", (AsIs(column_name),)))
   cur.execute("alter table "+table1000g+" add column %s text;", (AsIs(column_name),))
   conn.commit()


def load_1000g():

    global column_variable_counter
    global column_limit_counter
    #define progress bar object
    widgets = ['database upload -> '+table1000g+' :', Percentage(), ' ', Bar(marker=RotatingMarker()),' ', ETA(), ' ', FileTransferSpeed()]

    pbar = ProgressBar(widgets=widgets, maxval=10000000).start()

    with open(myfile) as f:

       # print "loading : "+table1000g+"...."
       ##latest change to file and remove readlines
       for line in pbar(f):
            logging.debug("whole line :"+line)
            # find columns row and set it as column names

            #skip if table is initialized with colums
    #       option to have all files in single table
    #         if not check_empty_table(table1000g):
            ##add tab

            if re.match("^#(?!#)",line):
                logging.debug("found1#!")
                col_words = line.split()
                col_counter = len(col_words)
                logging.debug("column list length")
                logging.debug(col_counter)

                for word in line.split():

                  #skip after 20 column

                  if column_limit_counter >= column_limit:
                    column_limit_counter=0
                    break
                  column_limit_counter+=1


                  if word.startswith('#'):
                    logging.debug(word.replace("#","")+"word"+str(col_counter))
                    wo = word.replace("#","")
                    addcol(wo)
                  else :
                    logging.debug(word+" word"+str(col_counter))
                    addcol(word)

            # first check the line length and compare to columns number
            #find and load variable lines
            if (not line.startswith('#')) and ("CNV" not in line):
                col_words2 = line.split()
                word_counter = len(col_words2)
                if word_counter == col_counter:
                    logging.debug("variables list length :")
                    logging.debug(word_counter)
                    logging.debug("column list length :")
                    logging.debug(col_counter)
                    linequoted = ""

                    for word in line.split():

                        #skip after 20 column
                        if column_variable_counter >= column_limit:
                            column_variable_counter=0
                            break

                        column_variable_counter+=1


                        wordquoted='\''+word+'\''','
                        logging.debug(wordquoted)
                        linequoted += wordquoted

                    logging.debug(linequoted)
                    insertline=linequoted[:-1]

                    insert_values(insertline)

def file_first_row_length(file_2length):
    with open (file_2length) as f:

        for line in f:

            return len(line.split())
            break


def load_md2sql():

    mind_firstline = True

    global column_variable_counter
    global column_limit_counter
    #define progress bar object
    widgets = ['database upload -> '+table_mind+' :', Percentage(), ' ', Bar(marker=RotatingMarker()),' ', ETA(), ' ', FileTransferSpeed()]

    pbar = ProgressBar(widgets=widgets, maxval=10000000).start()

    with open(args.load_mind_data_f) as f:


       for line in pbar(f):
            logging.debug("whole line :"+line)
            # find columns row and set it as column names

            # if first line with column names create table with column lines:
            if mind_firstline:
                logging.debug("mind data first line!")
                col_words = line.split()
                col_counter = len(col_words)
                logging.debug("column list length")
                logging.debug(col_counter)

                create_table(table_mind)

                for word in line.split():

                    logging.debug(word+" word"+str(col_counter))

                    addcol_2table(word,table_mind)

            mind_firstline = False

            # first check the line length and compare to columns number
            #find and load variable lines
            if not line.startswith('idnum'):
                col_words2 = line.split()
                word_counter = len(col_words2)
                if word_counter == col_counter:
                    logging.debug("variables list length :")
                    logging.debug(word_counter)
                    logging.debug("column list length :")
                    logging.debug(col_counter)
                    linequoted = ""

                    for word in line.split():

                        #skip after 20 column
                        # if column_variable_counter >= column_limit:
                        #     column_variable_counter=0
                        #     break
                        #
                        # column_variable_counter+=1


                        wordquoted='\''+word+'\''','
                        logging.debug(wordquoted)
                        linequoted += wordquoted

                    logging.debug(linequoted)
                    insertline=linequoted[:-1]

                    insert_values_2table(insertline,table_mind)


def load_mind_rsids2sql():

    mind_firstline = True

    global column_variable_counter
    global column_limit_counter
    #define progress bar object
    widgets = ['database upload -> mind_rsids  :', Percentage(), ' ', Bar(marker=RotatingMarker()),' ', ETA(), ' ', FileTransferSpeed()]

    pbar = ProgressBar(widgets=widgets, maxval=10000000).start()

    with open(args.load_mind_rsids) as f:


       for line in pbar(f):
            logging.debug("whole line :"+line)
            # find columns row and set it as column names

            # if first line with column names create table with column lines:
            if mind_firstline:
                logging.debug("mind rs first line!")
                col_words = line.split()
                col_counter = len(col_words)
                logging.debug("column list length")
                logging.debug(col_counter)

                create_table(varmindrsids_table)

                for word in line.split():

                    logging.debug(word+" word"+str(col_counter))

                    addcol_2table(word,varmindrsids_table)

            mind_firstline = False

            # first check the line length and compare to columns number
            #find and load variable lines
            if "numid" not in line:
                col_words2 = line.split()
                word_counter = len(col_words2)
                if word_counter == col_counter:
                    logging.debug("variables list length :")
                    logging.debug(word_counter)
                    logging.debug("column list length :")
                    logging.debug(col_counter)
                    linequoted = ""

                    for word in line.split():

                        #skip after 20 column
                        # if column_variable_counter >= column_limit:
                        #     column_variable_counter=0
                        #     break
                        #
                        # column_variable_counter+=1


                        wordquoted='\''+word+'\''','
                        logging.debug(wordquoted)
                        linequoted += wordquoted

                    logging.debug(linequoted)
                    insertline=linequoted[:-1]

                    insert_values_2table(insertline,varmindrsids_table)


def insert_values(line):
    logging.debug(cur.mogrify("insert into "+table1000g+" values (%s);", (AsIs(line),)))
    cur.execute("insert into "+table1000g+" values (%s);", (AsIs(line),))
    conn.commit()

def insert_values_2table(line,table_name):
    logging.debug(cur.mogrify("insert into %s values (%s);", (AsIs(table_name),AsIs(line),)))
    cur.execute("insert into %s values (%s);", (AsIs(table_name),AsIs(line),))
    conn.commit()

def check_1000g_table():
    if check_table_exists(table1000g):

        print "Table "+table1000g+" exists !"

        ans = (raw_input("Are you sure you want to reset this table ? (yes/no)"))
        if ans == "yes":

            delete_table(table1000g)
            create_table(table1000g)

        elif ans == "no":

            global abort_1000g_table
            abort_1000g_table = "true"
        else:
            print "please answear yes or no ..."
            check_1000g_table()
    else:
        create_table(table1000g)

def check_overwrite_table(table8):


    time_it()

    if check_table_exists(table8):
            print "Table "+table8+" exists !"
            ans = (raw_input("Are you sure you want to reset this table ? (yes/no)"))
            if ans == "yes":
                delete_table(table8)

            elif ans == "no":
                print "skipped..table create"

            else:
                print "please answear yes or no ..."
                check_overwrite_table()


def force_check_overwrite_table(table8):


    time_it()

    if check_table_exists(table8):

        delete_table(table8)


def sort_annotated(anntable):

    sortedtable = args.sort+"sorted"

    cur.mogrify("create table %s as select * from %s order by \"chromStart\";",(AsIs(sortedtable),AsIs(anntable),))
    cur.execute("create table %s as select * from %s order by \"chromStart\";",(AsIs(sortedtable),AsIs(anntable),))
    conn.commit()

def sort_by_gene_and_pos(anntable2):

    sortedtable2 = args.sort_by_gene_and_pos+"sorted_by_gene_pos"
    cur.mogrify("create table %s as select * from %s order by \"chromStart\" , \"gene_name\" ;",(AsIs(sortedtable2),AsIs(anntable2),))
    cur.execute("create table %s as select * from %s order by \"chromStart\" , \"gene_name\" ;",(AsIs(sortedtable2),AsIs(anntable2),))
    conn.commit()

def mind_sort_by_gene_and_pos(anntable2):

    sortedtable2 = args.mind_sort_by_gene_and_pos+"sorted_by_gene_pos"
    check_overwrite_table(sortedtable2)
    cur.mogrify("create table %s as select * from %s order by \"coordinate\" , \"gene_name\" ;",(AsIs(sortedtable2),AsIs(anntable2),))
    cur.execute("create table %s as select * from %s order by \"coordinate\" , \"gene_name\" ;",(AsIs(sortedtable2),AsIs(anntable2),))
    conn.commit()

def check_snp_table():

        if check_table_exists(snptable_s_ch_filtered_final):
            print "Table "+snptable_s_ch_filtered_final+" exists !"
            ans = (raw_input("Are you sure you want to reset this table ? (yes/no)"))
            if ans == "yes":

                delete_table(snptable_s_ch_filtered_final)
            elif ans == "no":

                global abort_snp_table
                abort_snp_table = "true"
            else:
                print "please answear yes or no ..."
                check_snp_table()


# def join_chr_with_anno_snp(chrtable):
#join with ensemble snp
def join_chr_with_anno_ucsc_snp():

    cur.execute("SELECT tablename FROM pg_catalog.pg_tables where tableowner='pyuser' and tablename like '%filtered_final'")
    snpname=str(cur.fetchall())
    if not snpname:
        print "no snp annotated table present"
        sys.exit()
    else:

        annsnp = snpname[3:-4]
        annsnptemp = annsnp+"temp"

    chr2bann = args.a_ucsc+"ann"

    #check overwrite or errstop

    check_overwrite_table(chr2bann)
    # check_overwrite_table(annsnptemp)

    if not check_table_exists(annsnptemp):

        #first copy the snp table without the chrom column

        print(cur.mogrify("create table %s as select * from %s ",(AsIs(annsnptemp),AsIs(annsnp))))
        cur.execute("create table %s as select * from %s ",(AsIs(annsnptemp),AsIs(annsnp)))
        conn.commit()

        print(cur.mogrify("alter table %s drop column chrom", (AsIs(annsnptemp),)))
        cur.execute("alter table %s drop column chrom", (AsIs(annsnptemp),))
        conn.commit()

    # then inner join the annsnp table with the chrtable
    print(cur.mogrify("CREATE TABLE %s AS SELECT * FROM %s inner join %s on (%s.name = %s.id)",(AsIs(chr2bann),AsIs(annsnptemp),AsIs(args.a_ucsc),AsIs(annsnptemp),AsIs(args.a_ucsc),)))
    cur.execute("CREATE TABLE %s AS SELECT * FROM %s inner join %s on (%s.name = %s.id)",(AsIs(chr2bann),AsIs(annsnptemp),AsIs(args.a_ucsc),AsIs(annsnptemp),AsIs(args.a_ucsc),))
    conn.commit()

    # #clean up temp table
#    print(cur.mogrify("drop table if exists %s",(AsIs(annsnptemp),)))
 #   cur.execute("drop table if exists %s",(AsIs(annsnptemp),))
  #  conn.commit()

def join_mind_with_with_anno_ucsc_snp():

    cur.execute("SELECT tablename FROM pg_catalog.pg_tables where tableowner='pyuser' and tablename like '%filtered_final_spltal'")
    snpname=str(cur.fetchall())
    if not snpname:
        print "no snp annotated table present"
        sys.exit()
    else:

        annsnp = snpname[3:-4]
        annsnptemp = annsnp+"temp"

    mind2bann = args.join_mind_with_with_anno_ucsc_snp+"ann"

    #check overwrite or errstop

    check_overwrite_table(mind2bann)
    # check_overwrite_table(annsnptemp)

    if not check_table_exists(annsnptemp):

        #first copy the snp table without the chrom column

        print(cur.mogrify("create table %s as select * from %s ",(AsIs(annsnptemp),AsIs(annsnp))))
        cur.execute("create table %s as select * from %s ",(AsIs(annsnptemp),AsIs(annsnp)))
        conn.commit()

        print(cur.mogrify("alter table %s drop column chrom", (AsIs(annsnptemp),)))
        cur.execute("alter table %s drop column chrom", (AsIs(annsnptemp),))
        conn.commit()

    # then inner join the annsnp table with the chrtable
    print(cur.mogrify("CREATE TABLE %s AS SELECT * FROM %s inner join %s on (%s.name = %s.rsid)",(AsIs(mind2bann),AsIs(annsnptemp),AsIs(args.join_mind_with_with_anno_ucsc_snp),AsIs(annsnptemp),AsIs(args.join_mind_with_with_anno_ucsc_snp),)))
    cur.execute("CREATE TABLE %s AS SELECT * FROM %s inner join %s on (%s.name = %s.rsid)",(AsIs(mind2bann),AsIs(annsnptemp),AsIs(args.join_mind_with_with_anno_ucsc_snp),AsIs(annsnptemp),AsIs(args.join_mind_with_with_anno_ucsc_snp),))
    conn.commit()

    # #clean up temp table
#    print(cur.mogrify("drop table if exists %s",(AsIs(annsnptemp),)))
 #   cur.execute("drop table if exists %s",(AsIs(annsnptemp),))
  #  conn.commit()


#load 2 tables from ensembl , one with gana_name and the other with clinical segnificanse, add them together
def add_ann_ensembl():

    e1 = "ensembl_variation"
    e2 = "ensembl_variation_genename"

    if not check_table_exists(e1):
        print cur.mogrify("CREATE TABLE ensembl_variation (\"variation_id\" int ,\"source_id\" int ,\"rs_name\" text ,\"validation_status\" text ,\"ancestral_allele\" text ,\"flipped\" boolean ,\"class_attrib_id\" int ,\"somatic\" boolean ,\"minor_allele\" text ,\"minor_allele_freq\" real ,\"minor_allele_count\" int ,\"clinical_significance\" text , \"evidence\" text)")
        cur.execute("CREATE TABLE ensembl_variation (\"variation_id\" int ,\"source_id\" int ,\"rs_name\" text ,\"validation_status\" text ,\"ancestral_allele\" text ,\"flipped\" boolean ,\"class_attrib_id\" int ,\"somatic\" boolean ,\"minor_allele\" text ,\"minor_allele_freq\" real ,\"minor_allele_count\" int ,\"clinical_significance\" text , \"evidence\" text)")
        conn.commit()


    if not check_table_exists(e2):
        print cur.mogrify("CREATE TABLE ensembl_variation_genename (\"variation_id2\" int ,  \"gene_name\" text)")
        cur.execute("CREATE TABLE ensembl_variation_genename (\"variation_id2\" int ,  \"gene_name\" text)")
        conn.commit()

    #copy data to the tables

    print ("COPY ensembl_variation FROM "+args.ensembl_variation_snpf)
    cur.copy_from(ensembl_variation_file, "ensembl_variation")
    conn.commit()
    print ("COPY ensembl_variation FROM "+args.ensembl_variation_genename_snpf)
    cur.copy_from(ensembl_variation_genename_file, "ensembl_variation_genename")
    conn.commit()
    #join them to a new table : variation_genename_4ann

    print cur.mogrify("CREATE TABLE variation_genename_4ann AS SELECT * FROM ensembl_variation inner join ensembl_variation_genename on (ensembl_variation.variation_id = ensembl_variation_genename.variation_id2)")
    cur.execute("CREATE TABLE variation_genename_4ann AS SELECT * FROM ensembl_variation inner join ensembl_variation_genename on (ensembl_variation.variation_id = ensembl_variation_genename.variation_id2)")
    conn.commit()

    print cur.mogrify("alter table variation_genename_4ann drop column variation_id2")
    print cur.execute("alter table variation_genename_4ann drop column variation_id2")
    conn.commit()


#add tables meta data , this is a static table
def add_meta():
    if not check_table_exists("tables_meta"):
        print (cur.mogrify("create table tables_meta (\"source\" text, \"Genome\" text, \"Reference_Consortium_Human_Reference\" text, \"table_name\" text, \"schema_link\" text, \"table link\" text, \"documentation_link\" text, \"date_downloaded\" text) "))
        cur.execute("create table tables_meta (\"source\" text, \"Genome\" text, \"Reference_Consortium_Human_Reference\" text, \"table_name\" text, \"schema_link\" text, \"table link\" text, \"documentation_link\" text, \"date_downloaded\" text) ")
        conn.commit()
        print ("insert into tables_meta values(\'ucsc\', \'GRCh37\', \'snp141CodingDbSnp\', \'http://hgdownload.cse.ucsc.edu/goldenpath/hg19/database/snp141CodingDbSnp.sql\', \'http://hgdownload.cse.ucsc.edu/goldenpath/hg19/database/snp141CodingDbSnp.txt.gz\', \'http://hgdownload.cse.ucsc.edu/goldenpath/hg19/database/\', \'10.2015\')")
        cur.execute("insert into tables_meta values(\'ucsc\', \'GRCh37\', \'snp141CodingDbSnp\', \'http://hgdownload.cse.ucsc.edu/goldenpath/hg19/database/snp141CodingDbSnp.sql\', \'http://hgdownload.cse.ucsc.edu/goldenpath/hg19/database/snp141CodingDbSnp.txt.gz\', \'http://hgdownload.cse.ucsc.edu/goldenpath/hg19/database/\', \'10.2015\')")
        conn.commit()
        print("insert into tables_meta values(\'ensembl\', \'GRCh37\', \'variation,  variation_genename\', \'ftp://ftp.ensembl.org/pub/release-75/mysql/homo_sapiens_variation_75_37/homo_sapiens_variation_75_37.sql.gz\', \'ftp://ftp.ensembl.org/pub/release-75/mysql/homo_sapiens_variation_75_37/variation.txt.gz,  ftp://ftp.ensembl.org/pub/release-75/mysql/homo_sapiens_variation_75_37/variation_genename.txt.gz\', \'http://grch37.ensembl.org/info/docs/api/variation/variation_schema.html#variation\', \'8.2015\')")
        cur.execute("insert into tables_meta values(\'ensembl\', \'GRCh37\', \'variation,  variation_genename\', \'ftp://ftp.ensembl.org/pub/release-75/mysql/homo_sapiens_variation_75_37/homo_sapiens_variation_75_37.sql.gz\', \'ftp://ftp.ensembl.org/pub/release-75/mysql/homo_sapiens_variation_75_37/variation.txt.gz,  ftp://ftp.ensembl.org/pub/release-75/mysql/homo_sapiens_variation_75_37/variation_genename.txt.gz\', \'http://grch37.ensembl.org/info/docs/api/variation/variation_schema.html#variation\', \'8.2015\')")
        conn.commit()
        print("insert into tables_meta values(\'1000genomes\', \'GRCh37\', \'ALL.chrXX.phase3\', \'ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/release/20130502/\', \'ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/release/20130502/\', \'http://www.1000genomes.org/faq\', \'7.2015\')")
        cur.execute("insert into tables_meta values(\'1000genomes\', \'GRCh37\', \'ALL.chrXX.phase3\', \'ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/release/20130502/\', \'ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/release/20130502/\', \'http://www.1000genomes.org/faq\', \'7.2015\')")
        conn.commit()
        print("insert into tables_meta values(\'DGIdb\', \'all\', \'all\', \'DGIdb_tables drug genes\', \'http://dgidb.genome.wustl.edu/downloads\', \'https://github.com/genome/dgi-db/blob/master/db/structure.sql\', \'5.2016\')")
        cur.execute("insert into tables_meta values(\'DGIdb\', \'all\', \'all\', \'DGIdb_tables drug genes\', \'http://dgidb.genome.wustl.edu/downloads\', \'https://github.com/genome/dgi-db/blob/master/db/structure.sql\', \'5.2016\')")
        conn.commit()


def join_chr_with_anno_ensembl_snp():
    if not check_table_exists("variation_genename_4ann"):
        print "no snp annotated table present"
        sys.exit()

    #new annotation table to be created
    chr2bann_ensembl = args.a_ensembl+"2"

    check_overwrite_table(chr2bann_ensembl)

    #  inner join the ensembl table with the chranntable
    print(cur.mogrify("CREATE TABLE %s AS SELECT * FROM %s inner join variation_genename_4ann on (%s.name = variation_genename_4ann.rs_name)",(AsIs(chr2bann_ensembl),AsIs(args.a_ensembl),AsIs(args.a_ensembl),)))

    cur.execute("CREATE TABLE %s AS SELECT * FROM %s inner join variation_genename_4ann on (%s.name = variation_genename_4ann.rs_name)",(AsIs(chr2bann_ensembl),AsIs(args.a_ensembl),AsIs(args.a_ensembl),))
    conn.commit()

def join_mind_table_with_anno_ensembl_snp():
    if not check_table_exists("variation_genename_4ann"):
        print "no snp annotated table present"
        sys.exit()

    #new annotation table to be created
    mind2bann_ensembl = args.mind_a_ensembl+"_en"

    check_overwrite_table(mind2bann_ensembl)

    #  inner join the ensembl table with the chranntable
    print(cur.mogrify("CREATE TABLE %s AS SELECT * FROM %s inner join variation_genename_4ann on (%s.rsid = variation_genename_4ann.rs_name)",(AsIs(mind2bann_ensembl),AsIs(args.mind_a_ensembl),AsIs(args.mind_a_ensembl),)))

    cur.execute("CREATE TABLE %s AS SELECT * FROM %s inner join variation_genename_4ann on (%s.rsid = variation_genename_4ann.rs_name)",(AsIs(mind2bann_ensembl),AsIs(args.mind_a_ensembl),AsIs(args.mind_a_ensembl),))
    conn.commit()

def join_mind_data_with_rsids(mind_table):
    if not check_table_exists("mind_rsids"):
        print "no mind_rsids table present"
        sys.exit()

    # new annotation table to be created
    varmind_table_rs = mind_table+"_rs"

    check_overwrite_table(varmind_table_rs)

    #  inner join the ensembl table with the chranntable
    print(cur.mogrify("CREATE TABLE %s AS SELECT * FROM %s full outer join mind_rsids on (%s.idnum = mind_rsids.numid)",(AsIs(varmind_table_rs),AsIs(mind_table),AsIs(mind_table),)))

    cur.execute("CREATE TABLE %s AS SELECT * FROM %s full outer join mind_rsids on (%s.idnum = mind_rsids.numid)",(AsIs(varmind_table_rs),AsIs(mind_table),AsIs(mind_table),))
    conn.commit()

def multifunc(arg):
    print arg, "print"
    return arg,"return"

def multiquery(num):

    conn2 = psycopg2.connect("dbname="+dbname+" user="+dbuser+" password="+dbpass+" host=localhost")
    conn2.set_session(autocommit=True)
    cur2 = conn2.cursor()


    cur2.execute("create table bla%s();", (AsIs(num),))
    # cur2.execute("drop table bla%s;", (AsIs(num),))
    # cur2.mogrify("drop table bla%s;", (AsIs(num),))
    cur2.mogrify("create table bla%s();", (AsIs(num),))
    print str(cur.fetchall())



def isInt(s):
    try:
        int(s)
        return True
    except ValueError:
        return False

def query2list():
    global hglist
    hglist = []
    va2all_query = cur.fetchall()
    for i3 in va2all_query:
        for index,i2 in enumerate(i3):

            if index == len(i3) - 1:
                word12 = ''.join(i2)
                # print(word12)
            hglist.extend([word12])
            # print hglist
    return hglist

def query2list2():
    global hglist2
    hglist2 = []
    va2all_query = cur.fetchall()
    for i3 in va2all_query:
        for index,i2 in enumerate(i3):

            if index == len(i3) - 1:
                word12 = ''.join(i2)
                # print(word12)
            hglist2.extend([word12])
            # print hglist
    return hglist2


def gethg():

    # varhgstr = "%peptide_string"
    varhgstr = "hg_____"
    varg22tblpepstr = "%g1000chr1%pepstr"

    print(cur.mogrify("select column_name from information_schema.columns where table_name like \'%s\' and column_name like \'%s\';",(AsIs(varg22tblpepstr),AsIs(varhgstr),)))
    cur.execute("select column_name from information_schema.columns where table_name like \'%s\' and column_name like \'%s\';",(AsIs(varg22tblpepstr),AsIs(varhgstr),))

    global hglist
    hglist = []
    va2all_query = cur.fetchall()
    for i3 in va2all_query:
        for index,i2 in enumerate(i3):

            if index == len(i3) - 1:
                word12 = ''.join(i2)
                # print(word12)
            hglist.extend([word12])
            # print hglist
    return hglist



# initialise variables
table_exists = ""
table1000g = ""
Fileinput = ""
file_list = ""
myfile = ""
snptable = ""
snptablena = ""
snptablenasma = ""
snptable_s_c = ""
snptable_s_ch_filtered_final = ""
snpfile = ""
fsnpfile = ""

abort_snp_table = ""
abort_1000g_table = ""

table1000g_list = []

# check what the user wanted to do :
# load a 1000g taable
# load a snp annotated table
# both
#start##################################################################################################################

#optional clean / overwrite tables

# if overwrite_tables_is_set:
#     cleanup_err_tables()


try:


    if args.ucsc_snpt and args.ucsc_snpf:
        #snp anno. vars
        snptable = args.ucsc_snpt
        snptablena = args.ucsc_snpt+"na"
        snptablenasma = args.ucsc_snpt+"sma"
        snptable_s_c = args.ucsc_snpt+"_s_c"
        snptable_s_ch_filtered_final = args.ucsc_snpt+"_s_ch_filtered_final"
        snpfile = args.ucsc_snpf
        fsnpfile = open(snpfile)

        #check if exists
        check_snp_table()
        if abort_snp_table == "true":
            pass
        else:
            #load snp annot
            insert_snp_table()
            # do clean up
            cleanup_snp_tables()

    elif (not args.ucsc_snpt and args.ucsc_snpf) or (not args.ucsc_snpf and args.ucsc_snpt):
        print "argument missing !"
        parser.print_help()
        sys.exit()


# list path vs file path

    if args.list:


    #if the user gave a list containing 1000g file list
        if args.list and args.t:
             with open(args.list) as file_list:
                for file_list_line in file_list.readlines():

                    Fileinput = file_list_line.rstrip()
                    # 1000g vars
                    ch_name = re.search('chr.[0-9]|chr.', str(file_list_line))

                    if not ch_name:
                        print "\n ERROR : problem : wrong file name for 1000g file \n please use original vcf file names (containing \"chr\" string)"
                        print "error in file name :"+str(file_list_line)
                        sys.exit()

                    table1000g = args.t+ch_name.group(0)
                    #add to list for cleanup
                    table1000g_list.append(table1000g)
                    print("tablename = "+table1000g)
                    #set the 1000gfile
                    myfile = str(Fileinput)
                    #check if exists
                    #if overwrite is set delete and recreate table
                    if overwrite_tables_is_set == True:
                        if  check_table_exists(table1000g):
                            delete_table(table1000g)
                        create_table(table1000g)
                        load_1000g()
                    else:
                        check_1000g_table()
                        if abort_1000g_table == "true":
                            abort_1000g_table = "false"
                            pass
                        else:
                            #load 1000g table
                            load_1000g()
        elif (not args.t and args.list) or (not args.list and args.t):
            print "argument missing !"
            parser.print_help()
            sys.exit()

    elif not args.list:
#file not list
    #if the user gave a single 1000g file list

        if args.t and args.f:

            # 1000g vars
            ch_name = re.search('chr.[0-9]|chr.', args.f)
            if not ch_name:
                print "\n ERROR : problem : wrong file name for 1000g file \n please use original vcf file names (containing \"chr\" string)"
                print "error in file name :"+str(args.f)
                sys.exit()

            Fileinput = args.f
            table1000g = args.t+ch_name.group(0).lower()
            #set the 1000gfile
            myfile = str(Fileinput)
            #check if exists
            #if overwrite is set delete and recreate table
            if overwrite_tables_is_set == True:
                delete_table(table1000g)
                create_table(table1000g)
            else:
                check_1000g_table()
                if abort_1000g_table == "true":
                    pass
                else:
                    #load 1000g table
                    load_1000g()

        elif (not args.t and args.f) or (not args.f and args.t):
            print "argument missing !"
            parser.print_help()
            sys.exit()

    #show tables option
    if args.show_tables:
        # cur.execute("SELECT tablename FROM pg_catalog.pg_tables where tableowner='%s'",(AsIs(dbuser)),)
        cur.execute("SELECT tablename FROM pg_catalog.pg_tables where tableowner='"+dbuser+"'")
        print str(cur.fetchall())


    if args.a_ucsc:
        join_chr_with_anno_ucsc_snp()

    if args.sort:
        sort_annotated(args.sort)

    if args.sort_by_gene_and_pos:
        sort_by_gene_and_pos(args.sort_by_gene_and_pos)

    if args.add_meta:
        add_meta()

#ensembl tables block
    if args.ensembl_variation_snpf and args.ensembl_variation_genename_snpf:
        #ensembl snp anno. vars
        ensembl_variation_file = open(args.ensembl_variation_snpf)
        ensembl_variation_genename_file = open(args.ensembl_variation_genename_snpf)

        enslist = ["ensembl_variation","ensembl_variation_genename","variation_genename_4ann"]

        for enstable in enslist:
            if check_table_exists(enstable):
                evans = raw_input("Are you sure you want to reset "+enstable+" table ? (yes/no)")
                if evans == "yes":
                    print "reseting "+enstable+" table"
                    delete_table(enstable)

        #enslist loop
        add_ann_ensembl()


    elif (not args.ensembl_variation_genename_snpf and args.ensembl_variation_snpf) or (not args.ensembl_variation_snpf and args.ensembl_variation_genename_snpf):
        print "argument missing !"
        parser.print_help()
        sys.exit()

# add ensembl annotation to chr
    if args.a_ensembl:
        join_chr_with_anno_ensembl_snp()
#old export table:
#export to file sample of 100 from each table
    # if args.export_sample_2file:
    #    with open (args.export_sample_2file, "a") as export_file:
    #
    #        cur.execute("select tablename from pg_tables where tableowner='pyuser';")
    #        for i in cur.fetchall():
    #            sample_table = ''.join(i)
    #            if sample_table.endswith("sorted_by_gene_pos"):
    #                print "\n"
    #                print "table : ", sample_table
    #                print "\n"
    #                #  select * from g1000chr1ann2sorted_by_gene_pos where false
    #                cur.execute("select column_name from information_schema.columns where table_name = '%s';;",(AsIs(sample_table),))
    #                # print(cur.fetchall())
    #
    #                print(re.sub('(\,\))|(\()|(\[)|(\])|(\))','',str(cur.fetchall())))
    #                # print "\n"
    #                # export_file.write(cur.fetchall())
    #                cur.execute("select * from %s limit 100;",(AsIs(sample_table),))
    #                # print(cur.fetchall())
    #                for i2 in cur.fetchall():
    #                    print "\n"
    #                    print(re.sub('(\,\))|(\()|(\[)|(\])|(\))','',str(i2)))
    #                # export_file.write(cur.fetchall())

    #export to file sample of 100 from each table
    if args.export_sample_2file:
       firstline = True
       with open(args.export_sample_2file,"a") as export_file:

           cur.execute("select tablename from pg_tables where tableowner='pyuser';")
           for i in cur.fetchall():
               sample_table = ''.join(i)
               if sample_table.endswith("strnum"):

                    if firstline == True:

                        cur.execute("select column_name from information_schema.columns where table_name = '%s';",(AsIs(sample_table),))
                        row12 = cur.fetchall()
                        for index,i2 in enumerate(row12):
                            if index == len(row12) - 1 :
                                word12 = ''.join(i2)
                                export_file.write(word12)
                            else:
                                word12 = ''.join(i2) + ","
                                export_file.write(word12)
                        export_file.write("\n")

                        firstline = False

                    cur.execute("select * from %s limit 100;",(AsIs(sample_table),))
                    row12 = cur.fetchall()
                    for i3 in row12:
                        for index,i2 in enumerate(i3):

                            if index == len(i3) - 1:
                                word12 = ''.join(i2)
                                export_file.write(word12)
                            else:
                                if str(i2).isspace():
                                    i2 = re.sub('\s+','',str(i2))
                                    word12 = str(i2) + ","
                                    export_file.write(word12)
                                elif str(i2).isdigit():
                                    i2 = str(i2).strip()
                                    word12 = "\""+str(i2)+"\""
                                    word12 = str(i2) + ","
                                    export_file.write(word12)
                                elif not str(i2).isdigit():
                                    if str(i2) == "None":
                                        i2 = " "
                                    i2 = str(i2).strip()
                                    i2 = re.sub(',$','',i2)
                                    i2= re.sub(',',';',i2)
                                    word12 = str(i2) + "\',"
                                    export_file.write("\'"+word12)
                        export_file.write("\n")



##new full export

    #export to file sample of 100 from each table
    if args.export_fulldataset_2file:
       firstline = True
       with open(args.export_fulldataset_2file,"a") as export_file:

           cur.execute("select tablename from pg_tables where tableowner='pyuser';")
           for i in cur.fetchall():
               sample_table = ''.join(i)
               if sample_table.endswith("strnum"):

                    print "now exporting :   "+sample_table

                    if firstline == True:

                        cur.execute("select column_name from information_schema.columns where table_name = '%s';",(AsIs(sample_table),))
                        row12 = cur.fetchall()
                        for index,i2 in enumerate(row12):
                            if index == len(row12) - 1 :
                                word12 = ''.join(i2)
                                export_file.write(word12)
                            else:
                                word12 = ''.join(i2) + ","
                                export_file.write(word12)
                        export_file.write("\n")

                        firstline = False

                    cur.execute("select * from %s;",(AsIs(sample_table),))
                    row12 = cur.fetchall()
                    for i3 in row12:
                        for index,i2 in enumerate(i3):

                            if index == len(i3) - 1:
                                word12 = ''.join(i2)
                                export_file.write(word12)
                            else:
                                if str(i2).isspace():
                                    i2 = re.sub('\s+','',str(i2))
                                    word12 = str(i2) + ","
                                    export_file.write(word12)
                                elif str(i2).isdigit():
                                    i2 = str(i2).strip()
                                    word12 = "\""+str(i2)+"\""
                                    word12 = str(i2) + ","
                                    export_file.write(word12)
                                elif not str(i2).isdigit():
                                    if str(i2) == "None":
                                        i2 = " "
                                    i2 = str(i2).strip()
                                    i2 = re.sub(',$','',i2)
                                    i2= re.sub(',',';',i2)
                                    word12 = str(i2) + "\',"
                                    export_file.write("\'"+word12)
                        export_file.write("\n")



# add variation peptide scetion :
#
    if args.update_table_allel2peptide:
        var2all_table = args.update_table_allel2peptide + "al2p"
        check_overwrite_table(var2all_table)
        # if not check_table_exists(var2all_table):

        print(cur.mogrify("create table %s as select * from %s ",(AsIs(var2all_table),AsIs(args.update_table_allel2peptide),)))
        cur.execute("create table %s as select * from %s ",(AsIs(var2all_table),AsIs(args.update_table_allel2peptide),))
        conn.commit()

        hg = "hg%"

        print(cur.mogrify("select column_name from information_schema.columns where table_name = '%s' and column_name like \'%s\';",(AsIs(var2all_table),AsIs(hg),)))
        cur.execute("select column_name from information_schema.columns where table_name = '%s' and column_name like \'%s\';",(AsIs(var2all_table),AsIs(hg),))

        for hg2 in query2list():

            print(cur.mogrify("update %s set %s = (case when (%s='0') then  peptide1 when (%s='0|0') then  peptide1||peptide1 when (%s='0|1') then peptide1||peptide2 when (%s='0|2') then  peptide1||peptide3 when (%s='1') then  peptide2 when (%s='1|0') then  peptide2||peptide1 when (%s='1|1') then  peptide2||peptide2 when (%s='1|2') then  peptide2||peptide3 when (%s='2') then  peptide3 when (%s='2|0') then  peptide3||peptide1 when (%s='2|1') then  peptide3||peptide2 when (%s='2|2') then  peptide3||peptide3 end)",(AsIs(var2all_table),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),)))
            cur.execute("update %s set %s = (case when (%s='0') then  peptide1 when (%s='0|0') then  peptide1||peptide1 when (%s='0|1') then peptide1||peptide2 when (%s='0|2') then  peptide1||peptide3 when (%s='1') then  peptide2 when (%s='1|0') then  peptide2||peptide1 when (%s='1|1') then  peptide2||peptide2 when (%s='1|2') then  peptide2||peptide3 when (%s='2') then  peptide3 when (%s='2|0') then  peptide3||peptide1 when (%s='2|1') then  peptide3||peptide2 when (%s='2|2') then  peptide3||peptide3 end)",(AsIs(var2all_table),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),))
            conn.commit()
            print hg2



    def mind_export_ml():
        #get diagnosis, patient name,peptides string,gene in single line and string_agg it , output to file , for each of the four tables, for each patient

        # print(cur.mogrify("select column_name from information_schema.columns where table_name = \'%s\' and ( column_name ~ \'^sz.*[1-9]\' or column_name ~ \'^cg.*[1-9]\' or column_name ~ \'^el.*[1-9]\' or column_name ~ \'^gc.*[1-9]\' );",(AsIs(args.mind_export_ml),)))

        rstable = args.mind_export_ml.replace("_ensorted_by_gene_posann","")



        widgets = ['processing query -> '+table1000g+' :', Percentage(), ' ', Bar(marker=RotatingMarker()),' ', ETA(), ' ', FileTransferSpeed()]

        pbar = ProgressBar(widgets=widgets, maxval=10000000).start()

        with open('exported_mind.csv',"a") as export_file:

            export_file.write("'patient','diagnosis',")
            cur.execute("select gene_name||' | '||rsids from %s_dist_header ;",(AsIs(args.mind_export_ml),))
            for i2 in cur.fetchall():
                    # export_file.write(str(i2))
                    export_file.write(re.sub('(\()|(\[)|(\])|(\))','',str(i2)))
            export_file.write("\n")

            cur.execute("select column_name from information_schema.columns where table_name = \'%s\' and ( column_name ~ \'^sz.*[1-9]\' or column_name ~ \'^cg.*[1-9]\' or column_name ~ \'^el.*[1-9]\' or column_name ~ \'^gc.*[1-9]\' );",(AsIs(args.mind_export_ml),))

            for hg2 in pbar(query2list()):


                export_file.write('\''+hg2+'\'')
                export_file.write(',')
                # # mind_data_4_rs_ensorted_by_gene_posann 

                # print(cur.mogrify("select %s from %s where idnum='1' limit 1;",(AsIs(hg2),AsIs(rstable),)))
                cur.execute("select %s from %s where idnum='1' limit 1;",(AsIs(hg2),AsIs(rstable),))



                # # if cur.fetchall():
                # row12 = cur.fetchall()
                # for index,i2 in enumerate(row12):
                #
                #
                #     if index == len(row12) - 1 :
                #         word12 = ''.join(i2)
                #         export_file.write(word12)
                #     else:
                #         word12 = ''.join(i2) + ","
                #         export_file.write(word12)



                #
                for i2 in cur.fetchall():
                    # export_file.write(str(i2))
                    export_file.write(re.sub('(\()|(\[)|(\])|(\))','',str(i2)))



                # cur.execute("select t1||t2||t3 from (select case when substr(%s,1,1) = allele1 then '+'||peptide1 when substr(%s,1,1) = allele2 then '+'||peptide2 when substr(%s,1,1) = allele3 then '+'||peptide3 when substr(%s,1,1) = opallele1 then '-'||peptide1 when substr(%s,1,1) = opallele2 then '-'||peptide2 when substr(%s,1,1) = opallele3 then '-'||peptide3 end as t1 , case when substr(%s,3,1) = allele1 then '+'||peptide1 when substr(%s,3,1) = allele2 then '+'||peptide2 when substr(%s,3,1) = allele3 then '+'||peptide3 when substr(%s,3,1) = opallele1 then '-'||peptide1 when substr(%s,3,1) = opallele2 then '-'||peptide2 when substr(%s,3,1) = opallele3 then '-'||peptide3 end as t2 ,case when substr(%s,5,1) = '' then '' when substr(%s,5,1) = allele1 then '+'||peptide1 when substr(%s,5,1) = allele2 then '+'||peptide2 when substr(%s,5,1) = allele3 then '+'||peptide3 when substr(%s,5,1) = opallele1 then '-'||peptide1 when substr(%s,5,1) = opallele2 then '-'||peptide2 when substr(%s,5,1) = opallele3 then '-'||peptide3 end as t3 from %s) as mytable;",(AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(args.mind_export_ml),))

                cur.execute("select peptid_group from (select gene_name , string_agg(rsid,',' order by rsid) as rsids, string_agg(peptid_group,'') as peptid_group from (select distinct gene_name,rsid,mytable.t1||mytable.t2||mytable.t3  as peptid_group from (select gene_name,rsid,%s, case when substr(%s,1,1) = allele1 then '+'||peptide1 when substr(%s,1,1) = allele2 then '+'||peptide2 when substr(%s,1,1) = allele3 then '+'||peptide3 when substr(%s,1,1) = opallele1 then '-'||peptide1 when substr(%s,1,1) = opallele2 then '-'||peptide2 when substr(%s,1,1) = opallele3 then '-'||peptide3 end as t1 , case when substr(%s,3,1) = allele1 then '+'||peptide1 when substr(%s,3,1) = allele2 then '+'||peptide2 when substr(%s,3,1) = allele3 then '+'||peptide3 when substr(%s,3,1) = opallele1 then '-'||peptide1 when substr(%s,3,1) = opallele2 then '-'||peptide2 when substr(%s,3,1) = opallele3 then '-'||peptide3 end as t2 ,case when substr(%s,5,1) = '' then '' when substr(%s,5,1) = allele1 then '+'||peptide1 when substr(%s,5,1) = allele2 then '+'||peptide2 when substr(%s,5,1) = allele3 then '+'||peptide3 when substr(%s,5,1) = opallele1 then '-'||peptide1 when substr(%s,5,1) = opallele2 then '-'||peptide2 when substr(%s,5,1) = opallele3 then '-'||peptide3 end as t3 from  %s) as mytable )as t1 group by t1.gene_name order by t1.gene_name) as t5;",(AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(args.mind_export_ml),))

                # for vart in query2list2():
                #     export_file.write(vart)
                #     export_file.write(',')
                # row12 = cur.fetchall()
                # for i3 in row12:
                #     for index,i2 in enumerate(i3):
                #
                #         if index == len(i3) - 1:
                #             word12 = ''.join(i2)
                #             export_file.write(word12)
                #         else:
                #             if str(i2).isspace():
                #                 i2 = re.sub('\s+','',str(i2))
                #                 word12 = str(i2) + ","
                #                 export_file.write(word12)
                #             elif str(i2).isdigit():
                #                 i2 = str(i2).strip()
                #                 word12 = "\""+str(i2)+"\""
                #                 word12 = str(i2) + ","
                #                 export_file.write(word12)
                #             elif not str(i2).isdigit():
                #                 if str(i2) == "None":
                #                     i2 = " "
                #                 i2 = str(i2).strip()
                #                 i2 = re.sub(',$','',i2)
                #                 i2= re.sub(',',',',i2)
                #                 word12 = str(i2) + "\',"
                #                 export_file.write("\'"+word12)
                # export_file.write("\n")

                # row12 = cur.fetchall()
                # for index,i2 in enumerate(row12):
                #
                #     if index == len(row12) - 1 :
                #         word12 = ''.join(i2)
                #         export_file.write(word12)
                #     else:
                #         word12 = ''.join(i2) + ","
                #         export_file.write(word12)
                for i2 in cur.fetchall():
                    # export_file.write(str(i2))
                   export_file.write(re.sub('(\()|(\[)|(\])|(\))','',str(i2)))

                export_file.write("\n")

#
# #create new tables without duplicate alleles
#     if args.remove_dup_allele:
#
#         var_nodup_table = args.remove_dup_allele+ "nodup"
#         check_overwrite_table(var_nodup_table)
#
#
#         print(cur.mogrify("create table %s as select * from %s WHERE peptide1!=peptide2 and peptide1!=peptide3 and peptide2!=peptide3",(AsIs(var_nodup_table),AsIs(args.remove_dup_allele),)))
#         cur.execute("create table %s as select * from %s WHERE peptide1!=peptide2 and peptide1!=peptide3 and peptide2!=peptide3",(AsIs(var_nodup_table),AsIs(args.remove_dup_allele),))
#         conn.commit()
#_ensorted_by_gene_posann_by_drug 


    def mind_export_ml_with_drugs():


        #get diagnosis, patient name,peptides string,gene in single line and string_agg it , output to file , for each of the four tables, for each patient


        # pydb=> select gene_name , string_agg(rsid,' ' order by rsid) as rsids ,string_agg(drugs_info,' ' order by drugs_info) as gene_drugs from (select distinct gene_name,rsid,drugs_info from mind_data_1_rs_ensorted_by_gene_posann_by_drug) as t1 group by t1.gene_name order by t1.gene_name; 

# pydb=> select gene_name ,string_agg(drugs_info,' ' order by drugs_info) as gene_drugs from (select distinct gene_name,drugs_info from mind_data_1_rs_ensorted_by_gene_posann_by_drug) as t1 group by t1.gene_name order by t1.gene_name; 


        table_mind_export_ml_with_drugs_header_rsids = args.mind_export_ml_with_drugs + "_header_rsids"

        table_mind_export_ml_with_drugs_header_drugs = args.mind_export_ml_with_drugs + "_h_d"

        table_mind_export_ml_with_drugs_header_rsids_and_drugs = args.mind_export_ml_with_drugs + "_h_r_d"



        rstable = args.mind_export_ml_with_drugs.replace("_ensorted_by_gene_posann_by_drug_alt","")
        rstable = args.mind_export_ml_with_drugs.replace("_ensorted_by_gene_posann_by_drug","")


        force_check_overwrite_table(table_mind_export_ml_with_drugs_header_rsids)

        # create dist header for ml with rsids here .....
        print(cur.mogrify("CREATE TABLE %s AS select gene_name , string_agg(rsid,' ' order by rsid) as rsids from (select distinct gene_name,rsid from %s) as t1 group by t1.gene_name order by t1.gene_name; ",(AsIs(table_mind_export_ml_with_drugs_header_rsids),AsIs(args.mind_export_ml_with_drugs),)))
        cur.execute("CREATE TABLE %s AS select gene_name , string_agg(rsid,' ' order by rsid) as rsids from (select distinct gene_name,rsid from %s) as t1 group by t1.gene_name order by t1.gene_name; ",(AsIs(table_mind_export_ml_with_drugs_header_rsids),AsIs(args.mind_export_ml_with_drugs),))
        conn.commit()

        force_check_overwrite_table(table_mind_export_ml_with_drugs_header_drugs)

        # create dist header with drugs for ml here .....
        print(cur.mogrify("CREATE TABLE %s AS select gene_name as gene_name2 ,string_agg(drugs_info,' ' order by drugs_info) as gene_drugs from (select distinct gene_name,drugs_info from %s) as t1 group by t1.gene_name order by t1.gene_name; ",(AsIs(table_mind_export_ml_with_drugs_header_drugs),AsIs(args.mind_export_ml_with_drugs),)))
        cur.execute("CREATE TABLE %s AS select gene_name as gene_name2,string_agg(drugs_info,' ' order by drugs_info) as gene_drugs from (select distinct gene_name,drugs_info from %s) as t1 group by t1.gene_name order by t1.gene_name; ",(AsIs(table_mind_export_ml_with_drugs_header_drugs),AsIs(args.mind_export_ml_with_drugs),))
        conn.commit()

        force_check_overwrite_table(table_mind_export_ml_with_drugs_header_rsids_and_drugs)

        #join the previouse tables into one final header table
        print(cur.mogrify("CREATE TABLE %s AS SELECT * FROM %s inner join %s on (%s.gene_name2 = %s.gene_name)",(AsIs(table_mind_export_ml_with_drugs_header_rsids_and_drugs),AsIs(table_mind_export_ml_with_drugs_header_rsids),AsIs(table_mind_export_ml_with_drugs_header_drugs),AsIs(table_mind_export_ml_with_drugs_header_drugs),AsIs(table_mind_export_ml_with_drugs_header_rsids),)))
        cur.execute("CREATE TABLE %s AS SELECT * FROM %s inner join %s on (%s.gene_name2 = %s.gene_name)",(AsIs(table_mind_export_ml_with_drugs_header_rsids_and_drugs),AsIs(table_mind_export_ml_with_drugs_header_rsids),AsIs(table_mind_export_ml_with_drugs_header_drugs),AsIs(table_mind_export_ml_with_drugs_header_drugs),AsIs(table_mind_export_ml_with_drugs_header_rsids),))
        conn.commit()


        widgets = ['processing query -> '+table1000g+' :', Percentage(), ' ', Bar(marker=RotatingMarker()),' ', ETA(), ' ', FileTransferSpeed()]

        pbar = ProgressBar(widgets=widgets, maxval=10000000).start()

        with open('exported_mind_by_drugs.csv',"a") as export_file:

            #all 4 files are appended
            #I delete the header line manually for the 3 last files
            # this is useful for quality control


             #add |drugs_info here after "||rsids"
            export_file.write("'patient','diagnosis',")

            cur.execute("select gene_name||' | '||rsids||' | '||gene_drugs from %s order by gene_name;;",(AsIs(table_mind_export_ml_with_drugs_header_rsids_and_drugs),))


            for i2 in cur.fetchall():

            #output this line with only commas between columns !

                    i2_filter1 = re.sub('(\()|(\[)|(\])|(\))','',str(i2))

                    i2_filter2 = re.sub(',','',i2_filter1)

                    export_file.write(i2_filter2)

                    export_file.write(',')

            export_file.write("\n")

            cur.execute("select column_name from information_schema.columns where table_name = \'%s\' and ( column_name ~ \'^sz.*[1-9]\' or column_name ~ \'^cg.*[1-9]\' or column_name ~ \'^el.*[1-9]\' or column_name ~ \'^gc.*[1-9]\' );",(AsIs(args.mind_export_ml_with_drugs),))

            for hg2 in pbar(query2list()):


                export_file.write('\''+hg2+'\'')
                export_file.write(',')

                #add correct diagnosis for each patient , idnum=1 is the row with diagnosis for each patient column
                cur.execute("select %s from %s where idnum='1' limit 1;",(AsIs(hg2),AsIs(rstable),))

                for i2 in cur.fetchall():
                    export_file.write(re.sub('(\()|(\[)|(\])|(\))','',str(i2)))


                cur.execute("select peptid_group from (select gene_name , string_agg(rsid,',' order by rsid) as rsids, string_agg(peptid_group,'') as peptid_group from (select distinct gene_name,rsid,mytable.t1||mytable.t2||mytable.t3  as peptid_group from (select gene_name,rsid,%s, case when substr(%s,1,1) = allele1 then '+'||peptide1 when substr(%s,1,1) = allele2 then '+'||peptide2 when substr(%s,1,1) = allele3 then '+'||peptide3 when substr(%s,1,1) = opallele1 then '-'||peptide1 when substr(%s,1,1) = opallele2 then '-'||peptide2 when substr(%s,1,1) = opallele3 then '-'||peptide3 end as t1 , case when substr(%s,3,1) = allele1 then '+'||peptide1 when substr(%s,3,1) = allele2 then '+'||peptide2 when substr(%s,3,1) = allele3 then '+'||peptide3 when substr(%s,3,1) = opallele1 then '-'||peptide1 when substr(%s,3,1) = opallele2 then '-'||peptide2 when substr(%s,3,1) = opallele3 then '-'||peptide3 end as t2 ,case when substr(%s,5,1) = '' then '' when substr(%s,5,1) = allele1 then '+'||peptide1 when substr(%s,5,1) = allele2 then '+'||peptide2 when substr(%s,5,1) = allele3 then '+'||peptide3 when substr(%s,5,1) = opallele1 then '-'||peptide1 when substr(%s,5,1) = opallele2 then '-'||peptide2 when substr(%s,5,1) = opallele3 then '-'||peptide3 end as t3 from  %s) as mytable )as t1 group by t1.gene_name order by t1.gene_name) as t5;",(AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(args.mind_export_ml_with_drugs),))




                for i2 in cur.fetchall():
                   export_file.write(re.sub('(\()|(\[)|(\])|(\))','',str(i2)))

                export_file.write("\n")

    def time_it():
        print "time: " + datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')


##new export_u
    def mind_export_ml_with_drugs_u():

        # get diagnosis, patient name,peptides string,gene in single line and string_agg it , output to file , for each of the four tables, for each patient


        # pydb=> select gene_name , string_agg(rsid,' ' order by rsid) as rsids ,string_agg(drugs_info,' ' order by drugs_info) as gene_drugs from (select distinct gene_name,rsid,drugs_info from mind_data_1_rs_ensorted_by_gene_posann_by_drug) as t1 group by t1.gene_name order by t1.gene_name;

        # pydb=> select gene_name ,string_agg(drugs_info,' ' order by drugs_info) as gene_drugs from (select distinct gene_name,drugs_info from mind_data_1_rs_ensorted_by_gene_posann_by_drug) as t1 group by t1.gene_name order by t1.gene_name;


        table_mind_export_ml_with_drugs_header_rsids = args.mind_export_ml_with_drugs + "_header_rsids"

        table_mind_export_ml_with_drugs_header_drugs = args.mind_export_ml_with_drugs + "_header_drugs"

        table_mind_export_ml_with_drugs_header_rsids_and_drugs = args.mind_export_ml_with_drugs + "_h_rsids_drugs"

        rstable = args.mind_export_ml_with_drugs.replace("_ensorted_by_gene_posann_by_drug_alt", "")

        check_overwrite_table(table_mind_export_ml_with_drugs_header_rsids)

        # create dist header for ml with rsids here .....
        print(cur.mogrify(
            "CREATE TABLE %s AS select gene_name , string_agg(rsid,' ' order by rsid) as rsids from (select distinct gene_name,rsid from %s) as t1 group by t1.gene_name order by t1.gene_name; ",
            (AsIs(table_mind_export_ml_with_drugs_header_rsids), AsIs(args.mind_export_ml_with_drugs),)))
        cur.execute(
            "CREATE TABLE %s AS select gene_name , string_agg(rsid,' ' order by rsid) as rsids from (select distinct gene_name,rsid from %s) as t1 group by t1.gene_name order by t1.gene_name; ",
            (AsIs(table_mind_export_ml_with_drugs_header_rsids), AsIs(args.mind_export_ml_with_drugs),))
        conn.commit()

        check_overwrite_table(table_mind_export_ml_with_drugs_header_drugs)

        # create dist header with drugs for ml here .....
        print(cur.mogrify(
            "CREATE TABLE %s AS select gene_name as gene_name2 ,string_agg(drugs_info,' ' order by drugs_info) as gene_drugs from (select distinct gene_name,drugs_info from %s) as t1 group by t1.gene_name order by t1.gene_name; ",
            (AsIs(table_mind_export_ml_with_drugs_header_drugs), AsIs(args.mind_export_ml_with_drugs),)))
        cur.execute(
            "CREATE TABLE %s AS select gene_name as gene_name2,string_agg(drugs_info,' ' order by drugs_info) as gene_drugs from (select distinct gene_name,drugs_info from %s) as t1 group by t1.gene_name order by t1.gene_name; ",
            (AsIs(table_mind_export_ml_with_drugs_header_drugs), AsIs(args.mind_export_ml_with_drugs),))
        conn.commit()

        check_overwrite_table(table_mind_export_ml_with_drugs_header_rsids_and_drugs)

        # join the previouse tables into one final header table
        print(
        cur.mogrify("CREATE TABLE %s AS SELECT * FROM %s inner join %s on (%s.gene_name2 = %s.gene_name)", (
        AsIs(table_mind_export_ml_with_drugs_header_rsids_and_drugs),
        AsIs(table_mind_export_ml_with_drugs_header_rsids), AsIs(table_mind_export_ml_with_drugs_header_drugs),
        AsIs(table_mind_export_ml_with_drugs_header_drugs),
        AsIs(table_mind_export_ml_with_drugs_header_rsids),)))
        cur.execute("CREATE TABLE %s AS SELECT * FROM %s inner join %s on (%s.gene_name2 = %s.gene_name)", (
        AsIs(table_mind_export_ml_with_drugs_header_rsids_and_drugs),
        AsIs(table_mind_export_ml_with_drugs_header_rsids), AsIs(table_mind_export_ml_with_drugs_header_drugs),
        AsIs(table_mind_export_ml_with_drugs_header_drugs),
        AsIs(table_mind_export_ml_with_drugs_header_rsids),))
        conn.commit()

        widgets = ['processing query -> ' + table1000g + ' :', Percentage(), ' ', Bar(marker=RotatingMarker()),
                   ' ', ETA(), ' ', FileTransferSpeed()]

        pbar = ProgressBar(widgets=widgets, maxval=10000000).start()

        with open('exported_mind_by_drugs_alt_u.csv', "a") as export_file:

            # all 4 files are appended
            # I delete the header line manually for the 3 last files
            # this is useful for quality control


            # add |drugs_info here after "||rsids"
            export_file.write("'patient','diagnosis',")

            cur.execute("select gene_name||' | '||rsids||' | '||gene_drugs from %s order by gene_name;;",
                        (AsIs(table_mind_export_ml_with_drugs_header_rsids_and_drugs),))

            for i2 in cur.fetchall():
                # output this line with only commas between columns !

                i2_filter1 = re.sub('(\()|(\[)|(\])|(\))', '', str(i2))

                i2_filter2 = re.sub(',', '', i2_filter1)

                export_file.write(i2_filter2)

                export_file.write(',')

            export_file.write("\n")

            cur.execute(
                "select column_name from information_schema.columns where table_name = \'%s\' and ( column_name ~ \'^sz.*[1-9]\' or column_name ~ \'^cg.*[1-9]\' or column_name ~ \'^el.*[1-9]\' or column_name ~ \'^gc.*[1-9]\' );",
                (AsIs(args.mind_export_ml_with_drugs),))

            for hg2 in pbar(query2list()):

                export_file.write('\'' + hg2 + '\'')
                export_file.write(',')

                # add correct diagnosis for each patient , idnum=1 is the row with diagnosis for each patient column
                cur.execute("select %s from %s where idnum='1' limit 1;", (AsIs(hg2), AsIs(rstable),))

                for i2 in cur.fetchall():
                    export_file.write(re.sub('(\()|(\[)|(\])|(\))', '', str(i2)))

                cur.execute(
                    "select peptid_group from (select gene_name , string_agg(rsid,',' order by rsid) as rsids, string_agg(peptid_group,'') as peptid_group from (select distinct gene_name,rsid,mytable.t1||mytable.t2||mytable.t3  as peptid_group from (select gene_name,rsid,%s, case when substr(%s,1,1) = allele1 then '+'||peptide1 when substr(%s,1,1) = allele2 then '+'||peptide2 when substr(%s,1,1) = allele3 then '+'||peptide3 when substr(%s,1,1) = opallele1 then '-'||peptide1 when substr(%s,1,1) = opallele2 then '-'||peptide2 when substr(%s,1,1) = opallele3 then '-'||peptide3 end as t1 , case when substr(%s,3,1) = allele1 then '+'||peptide1 when substr(%s,3,1) = allele2 then '+'||peptide2 when substr(%s,3,1) = allele3 then '+'||peptide3 when substr(%s,3,1) = opallele1 then '-'||peptide1 when substr(%s,3,1) = opallele2 then '-'||peptide2 when substr(%s,3,1) = opallele3 then '-'||peptide3 end as t2 ,case when substr(%s,5,1) = '' then '' when substr(%s,5,1) = allele1 then '+'||peptide1 when substr(%s,5,1) = allele2 then '+'||peptide2 when substr(%s,5,1) = allele3 then '+'||peptide3 when substr(%s,5,1) = opallele1 then '-'||peptide1 when substr(%s,5,1) = opallele2 then '-'||peptide2 when substr(%s,5,1) = opallele3 then '-'||peptide3 end as t3 from  %s) as mytable )as t1 group by t1.gene_name order by t1.gene_name) as t5;",
                    (AsIs(hg2), AsIs(hg2), AsIs(hg2), AsIs(hg2), AsIs(hg2), AsIs(hg2), AsIs(hg2), AsIs(hg2),
                     AsIs(hg2), AsIs(hg2), AsIs(hg2), AsIs(hg2), AsIs(hg2), AsIs(hg2), AsIs(hg2), AsIs(hg2),
                     AsIs(hg2), AsIs(hg2), AsIs(hg2), AsIs(hg2), AsIs(args.mind_export_ml_with_drugs),))

                for i2 in cur.fetchall():
                    export_file.write(re.sub('(\()|(\[)|(\])|(\))', '', str(i2)))

                export_file.write("\n")


    def mind_export_ml_with_drugs_alt():

        #get diagnosis, patient name,peptides string,gene in single line and string_agg it , output to file , for each of the four tables, for each patient


        # pydb=> select gene_name , string_agg(rsid,' ' order by rsid) as rsids ,string_agg(drugs_info,' ' order by drugs_info) as gene_drugs from (select distinct gene_name,rsid,drugs_info from mind_data_1_rs_ensorted_by_gene_posann_by_drug) as t1 group by t1.gene_name order by t1.gene_name;

# pydb=> select gene_name ,string_agg(drugs_info,' ' order by drugs_info) as gene_drugs from (select distinct gene_name,drugs_info from mind_data_1_rs_ensorted_by_gene_posann_by_drug) as t1 group by t1.gene_name order by t1.gene_name;


        table_mind_export_ml_with_drugs_alt_header_rsids = args.mind_export_ml_with_drugs_alt + "_h_r"

        table_mind_export_ml_with_drugs_alt_header_drugs = args.mind_export_ml_with_drugs_alt + "_h_d"

        table_mind_export_ml_with_drugs_alt_header_altgene = args.mind_export_ml_with_drugs_alt + "_h_ag"

        table_mind_export_ml_with_drugs_alt_header_rsids_and_drugs = args.mind_export_ml_with_drugs_alt + "_h_r_d"

        table_mind_export_ml_with_drugs_alt_header_rsids_and_drugs_and_altgene = args.mind_export_ml_with_drugs_alt + "_h_r_d_ag"


        rstable = args.mind_export_ml_with_drugs_alt.replace("_ensorted_by_gene_posann_by_drug_alt","")

        force_check_overwrite_table(table_mind_export_ml_with_drugs_alt_header_rsids)

        # create dist header for ml with rsids here .....
        print(cur.mogrify("CREATE TABLE %s AS select gene_name , string_agg(rsid,' ' order by rsid) as rsids from (select distinct gene_name,rsid from %s) as t1 group by t1.gene_name order by t1.gene_name; ",(AsIs(table_mind_export_ml_with_drugs_alt_header_rsids),AsIs(args.mind_export_ml_with_drugs_alt),)))
        cur.execute("CREATE TABLE %s AS select gene_name , string_agg(rsid,' ' order by rsid) as rsids from (select distinct gene_name,rsid from %s) as t1 group by t1.gene_name order by t1.gene_name; ",(AsIs(table_mind_export_ml_with_drugs_alt_header_rsids),AsIs(args.mind_export_ml_with_drugs_alt),))
        conn.commit()

        force_check_overwrite_table(table_mind_export_ml_with_drugs_alt_header_drugs)

        # create dist header with drugs for ml here .....
        print(cur.mogrify("CREATE TABLE %s AS select gene_name as gene_name2 ,string_agg(drugs_info,' ' order by drugs_info) as gene_drugs from (select distinct gene_name,drugs_info from %s) as t1 group by t1.gene_name order by t1.gene_name; ",(AsIs(table_mind_export_ml_with_drugs_alt_header_drugs),AsIs(args.mind_export_ml_with_drugs_alt),)))
        cur.execute("CREATE TABLE %s AS select gene_name as gene_name2,string_agg(drugs_info,' ' order by drugs_info) as gene_drugs from (select distinct gene_name,drugs_info from %s) as t1 group by t1.gene_name order by t1.gene_name; ",(AsIs(table_mind_export_ml_with_drugs_alt_header_drugs),AsIs(args.mind_export_ml_with_drugs_alt),))
        conn.commit()

        force_check_overwrite_table(table_mind_export_ml_with_drugs_alt_header_altgene)

        # create dist header with alt genes for ml here .....
        print(cur.mogrify("CREATE TABLE %s AS select gene_name as gene_name3 ,string_agg(interactive_gene_name,' ' order by interactive_gene_name) as interacting_genes from (select distinct gene_name,interactive_gene_name from %s) as t1 group by t1.gene_name order by t1.gene_name; ",(AsIs(table_mind_export_ml_with_drugs_alt_header_altgene),AsIs(args.mind_export_ml_with_drugs_alt),)))
        cur.execute("CREATE TABLE %s AS select gene_name as gene_name3 ,string_agg(interactive_gene_name,' ' order by interactive_gene_name) as interacting_genes from (select distinct gene_name,interactive_gene_name from %s) as t1 group by t1.gene_name order by t1.gene_name; ",(AsIs(table_mind_export_ml_with_drugs_alt_header_altgene),AsIs(args.mind_export_ml_with_drugs_alt),))
        conn.commit()

        force_check_overwrite_table(table_mind_export_ml_with_drugs_alt_header_rsids_and_drugs)

        #join the previouse tables into one final header table
        print(cur.mogrify("CREATE TABLE %s AS SELECT * FROM %s inner join %s on (%s.gene_name2 = %s.gene_name)",(AsIs(table_mind_export_ml_with_drugs_alt_header_rsids_and_drugs),AsIs(table_mind_export_ml_with_drugs_alt_header_rsids),AsIs(table_mind_export_ml_with_drugs_alt_header_drugs),AsIs(table_mind_export_ml_with_drugs_alt_header_drugs),AsIs(table_mind_export_ml_with_drugs_alt_header_rsids),)))
        cur.execute("CREATE TABLE %s AS SELECT * FROM %s inner join %s on (%s.gene_name2 = %s.gene_name) ",(AsIs(table_mind_export_ml_with_drugs_alt_header_rsids_and_drugs),AsIs(table_mind_export_ml_with_drugs_alt_header_rsids),AsIs(table_mind_export_ml_with_drugs_alt_header_drugs),AsIs(table_mind_export_ml_with_drugs_alt_header_drugs),AsIs(table_mind_export_ml_with_drugs_alt_header_rsids),))
        conn.commit()

#join with alt
        force_check_overwrite_table(table_mind_export_ml_with_drugs_alt_header_rsids_and_drugs_and_altgene)

        #join the previouse tables into one final header table
        print(cur.mogrify("CREATE TABLE %s AS SELECT * FROM %s inner join %s on (%s.gene_name2 = %s.gene_name3)",(AsIs(table_mind_export_ml_with_drugs_alt_header_rsids_and_drugs_and_altgene),AsIs(table_mind_export_ml_with_drugs_alt_header_rsids_and_drugs),AsIs(table_mind_export_ml_with_drugs_alt_header_altgene),AsIs(table_mind_export_ml_with_drugs_alt_header_rsids_and_drugs),AsIs(table_mind_export_ml_with_drugs_alt_header_altgene),)))
        cur.execute("CREATE TABLE %s AS SELECT * FROM %s inner join %s on (%s.gene_name2 = %s.gene_name3)",(AsIs(table_mind_export_ml_with_drugs_alt_header_rsids_and_drugs_and_altgene),AsIs(table_mind_export_ml_with_drugs_alt_header_rsids_and_drugs),AsIs(table_mind_export_ml_with_drugs_alt_header_altgene),AsIs(table_mind_export_ml_with_drugs_alt_header_rsids_and_drugs),AsIs(table_mind_export_ml_with_drugs_alt_header_altgene),))
        conn.commit()

        widgets = ['processing query -> '+table1000g+' :', Percentage(), ' ', Bar(marker=RotatingMarker()),' ', ETA(), ' ', FileTransferSpeed()]

        pbar = ProgressBar(widgets=widgets, maxval=10000000).start()

        with open('exported_mind_by_drugs_alt.csv',"a") as export_file:

            #all 4 files are appended
            #I delete the header line manually for the 3 last files
            # this is useful for quality control


             #add |drugs_info here after "||rsids"
            export_file.write("'patient','diagnosis',")

            print(cur.mogrify("select 'gene_name: '||gene_name||' | rsids: '||rsids||' | gene_drugs: '||gene_drugs||' | interacting_genes: '||interacting_genes from %s order by gene_name;",(AsIs(table_mind_export_ml_with_drugs_alt_header_rsids_and_drugs_and_altgene),)))
            cur.execute("select 'gene_name: '||gene_name||' | rsids: '||rsids||' | gene_drugs: '||gene_drugs||' | interacting_genes: '||interacting_genes from %s order by gene_name;",(AsIs(table_mind_export_ml_with_drugs_alt_header_rsids_and_drugs_and_altgene),))


            for i2 in cur.fetchall():

            #output this line with only commas between columns !

                    i2_filter1 = re.sub('(\()|(\[)|(\])|(\))','',str(i2))

                    i2_filter2 = re.sub(',','',i2_filter1)

                    export_file.write(i2_filter2)

                    export_file.write(',')

            export_file.write("\n")

            cur.execute("select column_name from information_schema.columns where table_name = \'%s\' and ( column_name ~ \'^sz.*[1-9]\' or column_name ~ \'^cg.*[1-9]\' or column_name ~ \'^el.*[1-9]\' or column_name ~ \'^gc.*[1-9]\' );",(AsIs(args.mind_export_ml_with_drugs_alt),))

            for hg2 in pbar(query2list()):


                export_file.write('\''+hg2+'\'')
                export_file.write(',')

                #add correct diagnosis for each patient , idnum=1 is the row with diagnosis for each patient column
                cur.execute("select %s from %s where idnum='1' limit 1;",(AsIs(hg2),AsIs(rstable),))

                for i2 in cur.fetchall():
                    export_file.write(re.sub('(\()|(\[)|(\])|(\))','',str(i2)))

                # print(cur.mogrify("select peptid_group from (select gene_name , string_agg(rsid,',' order by rsid) as rsids, string_agg(peptid_group,'') as peptid_group from (select distinct gene_name,rsid,mytable.t1||mytable.t2||mytable.t3  as peptid_group from (select gene_name,rsid,%s, case when substr(%s,1,1) = allele1 then '+'||peptide1 when substr(%s,1,1) = allele2 then '+'||peptide2 when substr(%s,1,1) = allele3 then '+'||peptide3 when substr(%s,1,1) = opallele1 then '-'||peptide1 when substr(%s,1,1) = opallele2 then '-'||peptide2 when substr(%s,1,1) = opallele3 then '-'||peptide3 end as t1 , case when substr(%s,3,1) = allele1 then '+'||peptide1 when substr(%s,3,1) = allele2 then '+'||peptide2 when substr(%s,3,1) = allele3 then '+'||peptide3 when substr(%s,3,1) = opallele1 then '-'||peptide1 when substr(%s,3,1) = opallele2 then '-'||peptide2 when substr(%s,3,1) = opallele3 then '-'||peptide3 end as t2 ,case when substr(%s,5,1) = '' then '' when substr(%s,5,1) = allele1 then '+'||peptide1 when substr(%s,5,1) = allele2 then '+'||peptide2 when substr(%s,5,1) = allele3 then '+'||peptide3 when substr(%s,5,1) = opallele1 then '-'||peptide1 when substr(%s,5,1) = opallele2 then '-'||peptide2 when substr(%s,5,1) = opallele3 then '-'||peptide3 end as t3 from  %s) as mytable )as t1 group by t1.gene_name order by t1.gene_name) as t5;",(AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(args.mind_export_ml_with_drugs_alt),)))

                cur.execute("select peptid_group from (select gene_name , string_agg(rsid,',' order by rsid) as rsids, string_agg(peptid_group,'') as peptid_group from (select distinct gene_name,rsid,mytable.t1||mytable.t2||mytable.t3  as peptid_group from (select gene_name,rsid,%s, case when substr(%s,1,1) = allele1 then '+'||peptide1 when substr(%s,1,1) = allele2 then '+'||peptide2 when substr(%s,1,1) = allele3 then '+'||peptide3 when substr(%s,1,1) = opallele1 then '-'||peptide1 when substr(%s,1,1) = opallele2 then '-'||peptide2 when substr(%s,1,1) = opallele3 then '-'||peptide3 end as t1 , case when substr(%s,3,1) = allele1 then '+'||peptide1 when substr(%s,3,1) = allele2 then '+'||peptide2 when substr(%s,3,1) = allele3 then '+'||peptide3 when substr(%s,3,1) = opallele1 then '-'||peptide1 when substr(%s,3,1) = opallele2 then '-'||peptide2 when substr(%s,3,1) = opallele3 then '-'||peptide3 end as t2 ,case when substr(%s,5,1) = '' then '' when substr(%s,5,1) = allele1 then '+'||peptide1 when substr(%s,5,1) = allele2 then '+'||peptide2 when substr(%s,5,1) = allele3 then '+'||peptide3 when substr(%s,5,1) = opallele1 then '-'||peptide1 when substr(%s,5,1) = opallele2 then '-'||peptide2 when substr(%s,5,1) = opallele3 then '-'||peptide3 end as t3 from  %s) as mytable )as t1 group by t1.gene_name order by t1.gene_name) as t5;",(AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(hg2),AsIs(args.mind_export_ml_with_drugs_alt),))




                for i2 in cur.fetchall():
                   export_file.write(re.sub('(\()|(\[)|(\])|(\))','',str(i2)))

                export_file.write("\n")



    if args.add_gene_peptide_string:

        varpepstr_table = args.add_gene_peptide_string + "pepstr"
        varpepstr_temp_table = args.add_gene_peptide_string + "temp"

        check_overwrite_table(varpepstr_table)



#get a list of patients

        hg = "hg%"

        print(cur.mogrify("select column_name from information_schema.columns where table_name = '%s' and column_name like \'%s\';",(AsIs(args.add_gene_peptide_string),AsIs(hg),)))
        cur.execute("select column_name from information_schema.columns where table_name = '%s' and column_name like \'%s\';",(AsIs(args.add_gene_peptide_string),AsIs(hg),))

#load ersults into list :

        query2list()
#create table with gene and pep list :

        if not check_table_exists(varpepstr_temp_table):

            print(cur.mogrify("create table %s as select gene_name as gene,string_agg(%s,'' order by %s) as %speptide_string,string_agg(%s,'' order by %s) as %speptide_string,string_agg(%s,'' order by %s) as %speptide_string,string_agg(%s,'' order by %s) as %speptide_string,string_agg(%s,'' order by %s) as %speptide_string,string_agg(%s,'' order by %s) as %speptide_string,string_agg(%s,'' order by %s) as %speptide_string,string_agg(%s,'' order by %s) as %speptide_string,string_agg(%s,'' order by %s) as %speptide_string,string_agg(%s,'' order by %s) as %speptide_string,string_agg(%s,'' order by %s) as %speptide_string from %s group by gene_name;",(AsIs(varpepstr_temp_table),AsIs(hglist[0]),AsIs(hglist[0]),AsIs(hglist[0]),AsIs(hglist[1]),AsIs(hglist[1]),AsIs(hglist[1]),AsIs(hglist[2]),AsIs(hglist[2]),AsIs(hglist[2]),AsIs(hglist[3]),AsIs(hglist[3]),AsIs(hglist[3]),AsIs(hglist[4]),AsIs(hglist[4]),AsIs(hglist[4]),AsIs(hglist[5]),AsIs(hglist[5]),AsIs(hglist[5]),AsIs(hglist[6]),AsIs(hglist[6]),AsIs(hglist[6]),AsIs(hglist[7]),AsIs(hglist[7]),AsIs(hglist[7]),AsIs(hglist[8]),AsIs(hglist[8]),AsIs(hglist[8]),AsIs(hglist[9]),AsIs(hglist[9]),AsIs(hglist[9]),AsIs(hglist[10]),AsIs(hglist[10]),AsIs(hglist[10]),AsIs(args.add_gene_peptide_string),)))
            cur.execute("create table %s as select gene_name as gene,string_agg(%s,'' order by %s) as %speptide_string,string_agg(%s,'' order by %s) as %speptide_string,string_agg(%s,'' order by %s) as %speptide_string,string_agg(%s,'' order by %s) as %speptide_string,string_agg(%s,'' order by %s) as %speptide_string,string_agg(%s,'' order by %s) as %speptide_string,string_agg(%s,'' order by %s) as %speptide_string,string_agg(%s,'' order by %s) as %speptide_string,string_agg(%s,'' order by %s) as %speptide_string,string_agg(%s,'' order by %s) as %speptide_string,string_agg(%s,'' order by %s) as %speptide_string from %s group by gene_name;",(AsIs(varpepstr_temp_table),AsIs(hglist[0]),AsIs(hglist[0]),AsIs(hglist[0]),AsIs(hglist[1]),AsIs(hglist[1]),AsIs(hglist[1]),AsIs(hglist[2]),AsIs(hglist[2]),AsIs(hglist[2]),AsIs(hglist[3]),AsIs(hglist[3]),AsIs(hglist[3]),AsIs(hglist[4]),AsIs(hglist[4]),AsIs(hglist[4]),AsIs(hglist[5]),AsIs(hglist[5]),AsIs(hglist[5]),AsIs(hglist[6]),AsIs(hglist[6]),AsIs(hglist[6]),AsIs(hglist[7]),AsIs(hglist[7]),AsIs(hglist[7]),AsIs(hglist[8]),AsIs(hglist[8]),AsIs(hglist[8]),AsIs(hglist[9]),AsIs(hglist[9]),AsIs(hglist[9]),AsIs(hglist[10]),AsIs(hglist[10]),AsIs(hglist[10]),AsIs(args.add_gene_peptide_string),))

        conn.commit()


#join peplistst table into new table create new table

        # create table test1 as select * from g1000chr1ann2sorted_by_gene_posal2p inner join g1000chr1ann2sorted_by_gene_posal2ptemp on (g1000chr1ann2sorted_by_gene_posal2p.gene_name = g1000chr1ann2sorted_by_gene_posal2ptemp.gene);

        print(cur.mogrify("create table %s as select * from %s inner join %s on (%s.gene_name = %s.gene)",(AsIs(varpepstr_table),AsIs(args.add_gene_peptide_string),AsIs(varpepstr_temp_table),AsIs(args.add_gene_peptide_string),AsIs(varpepstr_temp_table),)))
        cur.execute("create table %s as select * from %s inner join %s on (%s.gene_name = %s.gene)",(AsIs(varpepstr_table),AsIs(args.add_gene_peptide_string),AsIs(varpepstr_temp_table),AsIs(args.add_gene_peptide_string),AsIs(varpepstr_temp_table),))
        conn.commit()

    #cleap up temp table

        print(cur.mogrify("drop table %s",(AsIs(varpepstr_temp_table),)))
        cur.execute("drop table %s",(AsIs(varpepstr_temp_table),))
        conn.commit()




#join peplistst table into new table create new table

        # create table test1 as select * from g1000chr1ann2sorted_by_gene_posal2p inner join g1000chr1ann2sorted_by_gene_posal2ptemp on (g1000chr1ann2sorted_by_gene_posal2p.gene_name = g1000chr1ann2sorted_by_gene_posal2ptemp.gene);

        print(cur.mogrify("create table %s as select * from %s inner join %s on (%s.gene_name = %s.gene)",(AsIs(varpepstr_table),AsIs(args.mind_add_gene_peptide_string),AsIs(varpepstr_temp_table),AsIs(args.mind_add_gene_peptide_string),AsIs(varpepstr_temp_table),)))
        cur.execute("create table %s as select * from %s inner join %s on (%s.gene_name = %s.gene)",(AsIs(varpepstr_table),AsIs(args.mind_add_gene_peptide_string),AsIs(varpepstr_temp_table),AsIs(args.mind_add_gene_peptide_string),AsIs(varpepstr_temp_table),))
        conn.commit()

    #cleap up temp table

        print(cur.mogrify("drop table %s",(AsIs(varpepstr_temp_table),)))
        cur.execute("drop table %s",(AsIs(varpepstr_temp_table),))
        conn.commit()



    if args.create_uniq_pepstring_num:

        varallchpepstr = "allchpepstr"
        check_overwrite_table(varallchpepstr)

        pepvar ="%str"

        varallchpepstrcount="allchpepstrcount"
        check_overwrite_table(varallchpepstrcount)


        varallchpepstrcountsum="allchpepstrcountsum"
        check_overwrite_table(varallchpepstrcountsum)

 #create one big table from latets
        cur.execute("select table_name from information_schema.tables where table_name like \'%s\'",(AsIs(pepvar),))

        query2list()


        print(cur.mogrify("create table allchpepstr as select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s",(AsIs(hglist[0]), AsIs(hglist[1]), AsIs(hglist[2]), AsIs(hglist[3]), AsIs(hglist[4]), AsIs(hglist[5]), AsIs(hglist[6]), AsIs(hglist[7]), AsIs(hglist[8]), AsIs(hglist[9]), AsIs(hglist[10]), AsIs(hglist[11]), AsIs(hglist[12]), AsIs(hglist[13]), AsIs(hglist[14]), AsIs(hglist[15]), AsIs(hglist[16]), AsIs(hglist[17]), AsIs(hglist[18]), AsIs(hglist[19]), AsIs(hglist[20]), AsIs(hglist[21]), AsIs(hglist[22]), AsIs(hglist[23]),)))

        cur.execute("create table allchpepstr as select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s",(AsIs(hglist[0]), AsIs(hglist[1]), AsIs(hglist[2]), AsIs(hglist[3]), AsIs(hglist[4]), AsIs(hglist[5]), AsIs(hglist[6]), AsIs(hglist[7]), AsIs(hglist[8]), AsIs(hglist[9]), AsIs(hglist[10]), AsIs(hglist[11]), AsIs(hglist[12]), AsIs(hglist[13]), AsIs(hglist[14]), AsIs(hglist[15]), AsIs(hglist[16]), AsIs(hglist[17]), AsIs(hglist[18]), AsIs(hglist[19]), AsIs(hglist[20]), AsIs(hglist[21]), AsIs(hglist[22]), AsIs(hglist[23]),))
        conn.commit()


#run distinct pep string query on the big table

        varhgstr = "%peptide_string"
        varg22tblpepstr = "%g1000chr22%pepstr"

# select column_name from information_schema.columns where table_name like '%g1000chr22%pepstr' and column_name like '%peptide_string';

        print(cur.mogrify("select column_name from information_schema.columns where table_name like \'%s\' and column_name like \'%s\';",(AsIs(varg22tblpepstr),AsIs(varhgstr),)))
        cur.execute("select column_name from information_schema.columns where table_name like \'%s\' and column_name like \'%s\';",(AsIs(varg22tblpepstr),AsIs(varhgstr),))
#load ersults into list :

        query2list()


        print(cur.mogrify("create table allchpepstrcount as select %s as pepstr,count(%s),chrom from allchpepstr group by %s,chrom union all select %s,count(%s),chrom from allchpepstr group by %s,chrom union all select %s,count(%s),chrom from allchpepstr group by %s,chrom union all select %s,count(%s),chrom from allchpepstr group by %s,chrom union all select %s,count(%s),chrom from allchpepstr group by %s,chrom union all select %s,count(%s),chrom from allchpepstr group by %s,chrom union all select %s,count(%s),chrom from allchpepstr group by %s,chrom union all select %s,count(%s),chrom from allchpepstr group by %s,chrom union all select %s,count(%s),chrom from allchpepstr group by %s,chrom union all select %s,count(%s),chrom from allchpepstr group by %s,chrom union all select %s,count(%s),chrom from allchpepstr group by %s,chrom ;",(AsIs(hglist[0]),AsIs(hglist[0]),AsIs(hglist[0]), AsIs(hglist[1]),AsIs(hglist[1]),AsIs(hglist[1]), AsIs(hglist[2]),AsIs(hglist[2]),AsIs(hglist[2]), AsIs(hglist[3]),AsIs(hglist[3]),AsIs(hglist[3]), AsIs(hglist[4]),AsIs(hglist[4]),AsIs(hglist[4]), AsIs(hglist[5]),AsIs(hglist[5]),AsIs(hglist[5]), AsIs(hglist[6]),AsIs(hglist[6]),AsIs(hglist[6]), AsIs(hglist[7]),AsIs(hglist[7]),AsIs(hglist[7]), AsIs(hglist[8]),AsIs(hglist[8]),AsIs(hglist[8]), AsIs(hglist[9]),AsIs(hglist[9]),AsIs(hglist[9]), AsIs(hglist[10]),AsIs(hglist[10]),AsIs(hglist[10]),)))

        cur.execute("create table allchpepstrcount as select %s as pepstr,count(%s),chrom from allchpepstr group by %s,chrom union all select %s,count(%s),chrom from allchpepstr group by %s,chrom union all select %s,count(%s),chrom from allchpepstr group by %s,chrom union all select %s,count(%s),chrom from allchpepstr group by %s,chrom union all select %s,count(%s),chrom from allchpepstr group by %s,chrom union all select %s,count(%s),chrom from allchpepstr group by %s,chrom union all select %s,count(%s),chrom from allchpepstr group by %s,chrom union all select %s,count(%s),chrom from allchpepstr group by %s,chrom union all select %s,count(%s),chrom from allchpepstr group by %s,chrom union all select %s,count(%s),chrom from allchpepstr group by %s,chrom union all select %s,count(%s),chrom from allchpepstr group by %s,chrom ;",(AsIs(hglist[0]),AsIs(hglist[0]),AsIs(hglist[0]), AsIs(hglist[1]),AsIs(hglist[1]),AsIs(hglist[1]), AsIs(hglist[2]),AsIs(hglist[2]),AsIs(hglist[2]), AsIs(hglist[3]),AsIs(hglist[3]),AsIs(hglist[3]), AsIs(hglist[4]),AsIs(hglist[4]),AsIs(hglist[4]), AsIs(hglist[5]),AsIs(hglist[5]),AsIs(hglist[5]), AsIs(hglist[6]),AsIs(hglist[6]),AsIs(hglist[6]), AsIs(hglist[7]),AsIs(hglist[7]),AsIs(hglist[7]), AsIs(hglist[8]),AsIs(hglist[8]),AsIs(hglist[8]), AsIs(hglist[9]),AsIs(hglist[9]),AsIs(hglist[9]), AsIs(hglist[10]),AsIs(hglist[10]),AsIs(hglist[10]),))
        conn.commit()


#create table allchpepstrcountsum as select pepstr,sum(count) from allchpepstrcount group by pepstr;

        print(cur.mogrify("create table %s as select row_number() over() as str_rank,pepstr,strsum from (select pepstr,sum(count) as strsum from %s group by pepstr order by strsum desc) as table1;",(AsIs(varallchpepstrcountsum),AsIs(varallchpepstrcount),)))
        cur.execute("create table %s as select row_number() over() as str_rank,pepstr,strsum from (select pepstr,sum(count) as strsum from %s group by pepstr order by strsum desc) as table1;",(AsIs(varallchpepstrcountsum),AsIs(varallchpepstrcount),))
        conn.commit()
# create table test1 as select row_number() over() as str_rank,pepstr,strsum from (select pepstr,sum(count) as strsum from allchpepstrcount group by pepstr order by strsum desc) as table1;

    if args.add_uniq_pepstring_num:

        varallchpepstrcountsum="allchpepstrcountsum"
        varnumtable = args.add_uniq_pepstring_num+"num"

# create new table , alter it and add new columns  , update them with new info.

        check_overwrite_table(varnumtable)

        print(cur.mogrify("create table %s as select * from %s ",(AsIs(varnumtable),AsIs(args.add_uniq_pepstring_num))))
        cur.execute("create table %s as select * from %s ",(AsIs(varnumtable),AsIs(args.add_uniq_pepstring_num)))
        conn.commit()

        varhgstr = "%peptide_string"

        print(cur.mogrify("select column_name from information_schema.columns where table_name = \'%s\' and column_name like \'%s\';",(AsIs(args.add_uniq_pepstring_num),AsIs(varhgstr),)))
        cur.execute("select column_name from information_schema.columns where table_name = \'%s\' and column_name like \'%s\';",(AsIs(args.add_uniq_pepstring_num),AsIs(varhgstr),))
#load ersults into list :

        for hg1 in query2list():

            varhg1num = hg1+"_num"

            print(cur.mogrify("alter table %s add column %s text;",(AsIs(varnumtable),AsIs(varhg1num),)))
            cur.execute("alter table %s add column %s text;",(AsIs(varnumtable),AsIs(varhg1num),))
            conn.commit()


            print(cur.mogrify("update %s set %s = str_rank from allchpepstrcountsum where %s.%s = allchpepstrcountsum.pepstr;",(AsIs(varnumtable),AsIs(varhg1num),AsIs(varnumtable),AsIs(hg1),)))
            cur.execute("update %s set %s = str_rank from allchpepstrcountsum where %s.%s = allchpepstrcountsum.pepstr;",(AsIs(varnumtable),AsIs(varhg1num),AsIs(varnumtable),AsIs(hg1),))
            conn.commit()



    if args.create_ml_dataset_table:

      #add hg patient fields with all values = hg name

        varml_table = args.create_ml_dataset_table+"ml"
        varml_agg_table = args.create_ml_dataset_table+"mlagg"

        check_overwrite_table(varml_table)

        print(cur.mogrify("create table %s as select * from %s",(AsIs(varml_table),AsIs(args.create_ml_dataset_table),)))
        cur.execute("create table %s as select * from %s",(AsIs(varml_table),AsIs(args.create_ml_dataset_table),))
        conn.commit()


        for hg in gethg():

            print(cur.mogrify("alter table %s add column %s_name text",(AsIs(varml_table),AsIs(hg),)))
            cur.execute("alter table %s add column %s_name text",(AsIs(varml_table),AsIs(hg),))
            conn.commit()

            # update test2 set testcol1='testcol1';
            print(cur.mogrify("update %s set %s_name=\'%s\'",(AsIs(varml_table),AsIs(hg),AsIs(hg),)))
            cur.execute("update %s set %s_name=\'%s\'",(AsIs(varml_table),AsIs(hg),AsIs(hg),))
            conn.commit()


        #creat mlagg table , this table will hold the aggregated string for all of the chr hg's
        check_overwrite_table(varml_agg_table)

        print(cur.mogrify("create table %s (hg_patient text,peptide_string_num text,gene text)",(AsIs(varml_agg_table),)))
        cur.execute("create table %s (hg_patient text,peptide_string_num text,gene text)",(AsIs(varml_agg_table),))
        conn.commit()


        for hg in gethg():

            #create distinct hg ml table

            print(cur.mogrify("create table %s_mldistinct as select distinct %s_name,%speptide_string_num,gene from %s;",(AsIs(hg),AsIs(hg),AsIs(hg),AsIs(varml_table),)))
            cur.execute("create table %s_mldistinct as select distinct %s_name,%speptide_string_num,gene from %s;",(AsIs(hg),AsIs(hg),AsIs(hg),AsIs(varml_table),))
            conn.commit()

            #create string agg with comma del all pepstr numbs with row id as patient and insert into mlchr table
            print(cur.mogrify("with ml as (select %s_name,string_agg(%speptide_string_num,',' order by %speptide_string_num) as peptide_string_num ,string_agg(gene,',' order by gene) as gene from %s_mldistinct group by %s_name) insert into %s select * from ml;",(AsIs(hg),AsIs(hg),AsIs(hg),AsIs(hg),AsIs(hg),AsIs(varml_agg_table),)))
            cur.execute("with ml as (select %s_name,string_agg(%speptide_string_num,',' order by %speptide_string_num) as peptide_string_num ,string_agg(gene,',' order by gene) as gene from %s_mldistinct group by %s_name) insert into %s select * from ml;",(AsIs(hg),AsIs(hg),AsIs(hg),AsIs(hg),AsIs(hg),AsIs(varml_agg_table),))
            conn.commit()

            print(cur.mogrify("drop table %s_mldistinct ",(AsIs(hg),)))
            cur.execute("drop table %s_mldistinct ",(AsIs(hg),))
            conn.commit()


#add thisbefore exporting : with ml as (select * from test9 union all select * from test8) select hg_patient,string_agg(peptide_string_num,','),string_agg(gene,',') from ml group by hg_patient;



    #export to file sample of 100 from each table
    if args.export_ml_full_dataset:

#agg all mlagg tables into one

       varmlout = "mlout"
       check_overwrite_table(varmlout)
       varmlagg = "%mlagg"
        #create one big table from latets
       cur.execute("select table_name from information_schema.tables where table_name like \'%s\'",(AsIs(varmlagg),))

       query2list()

       print(cur.mogrify("create table mlout as select * from (with ml as (select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s) select hg_patient,string_agg(peptide_string_num,',') as peptide_string_num,string_agg(gene,',') as gene from ml group by hg_patient)as ml1;",(AsIs(hglist[0]), AsIs(hglist[1]), AsIs(hglist[2]), AsIs(hglist[3]), AsIs(hglist[4]), AsIs(hglist[5]), AsIs(hglist[6]), AsIs(hglist[7]), AsIs(hglist[8]), AsIs(hglist[9]), AsIs(hglist[10]), AsIs(hglist[11]), AsIs(hglist[12]), AsIs(hglist[13]), AsIs(hglist[14]), AsIs(hglist[15]), AsIs(hglist[16]), AsIs(hglist[17]), AsIs(hglist[18]), AsIs(hglist[19]), AsIs(hglist[20]), AsIs(hglist[21]), AsIs(hglist[22]), AsIs(hglist[23]),)))

       cur.execute("create table mlout as select * from (with ml as (select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s union all select * from %s) select hg_patient,string_agg(peptide_string_num,',') as peptide_string_num,string_agg(gene,',') as gene from ml group by hg_patient)as ml1;",(AsIs(hglist[0]), AsIs(hglist[1]), AsIs(hglist[2]), AsIs(hglist[3]), AsIs(hglist[4]), AsIs(hglist[5]), AsIs(hglist[6]), AsIs(hglist[7]), AsIs(hglist[8]), AsIs(hglist[9]), AsIs(hglist[10]), AsIs(hglist[11]), AsIs(hglist[12]), AsIs(hglist[13]), AsIs(hglist[14]), AsIs(hglist[15]), AsIs(hglist[16]), AsIs(hglist[17]), AsIs(hglist[18]), AsIs(hglist[19]), AsIs(hglist[20]), AsIs(hglist[21]), AsIs(hglist[22]), AsIs(hglist[23]),))
       conn.commit()

       firstline = True

       with open(args.export_ml_full_dataset,"a") as export_file:

           cur.execute("select tablename from pg_tables where tableowner='pyuser';")
           for i in cur.fetchall():
               sample_table = ''.join(i)
               if sample_table.endswith("mlout"):

                    print "now exporting :   "+sample_table

                    if firstline == True:

                        cur.execute("select column_name from information_schema.columns where table_name = '%s';",(AsIs(sample_table),))
                        row12 = cur.fetchall()
                        for index,i2 in enumerate(row12):
                            if index == len(row12) - 1 :
                                word12 = ''.join(i2)
                                export_file.write(word12)
                            else:
                                word12 = ''.join(i2) + ","
                                export_file.write(word12)
                        export_file.write("\n")

                        firstline = False

                    cur.execute("select * from %s;",(AsIs(sample_table),))
                    row12 = cur.fetchall()
                    for i3 in row12:
                        for index,i2 in enumerate(i3):

                            if index == len(i3) - 1:
                                word12 = ''.join(i2)
                                export_file.write(word12)
                            else:
                                if str(i2).isspace():
                                    i2 = re.sub('\s+','',str(i2))
                                    word12 = str(i2) + ","
                                    export_file.write(word12)
                                elif str(i2).isdigit():
                                    i2 = str(i2).strip()
                                    word12 = "\""+str(i2)+"\""
                                    word12 = str(i2) + ","
                                    export_file.write(word12)
                                elif not str(i2).isdigit():
                                    if str(i2) == "None":
                                        i2 = " "
                                    i2 = str(i2).strip()
                                    i2 = re.sub(',$','',i2)
                                    i2= re.sub(',',',',i2)
                                    word12 = str(i2) + "\',"
                                    export_file.write("\'"+word12)
                        export_file.write("\n")

    if args.mind_data_preprocess:

        print " see the folowing steps :"
        print '#! /bin/bash'
        print 'echo "split -l 100 --verbose -d t1"'
        print 'for i in `ls |grep x`;do  echo "cat -A $i|sed \'s/\^I/ /g\'|sed \'s/\$//g\'>$i.s";done'
        print 'for i in `ls |grep x.*s`;do  echo "transposer -i $i -o $i.t -d \" \"";done'
        print '"paste `ls|grep \'\.t\'|sort`>>fint"'
        print 'awk \'{if(NR>1)print}\' fint >fint2; cat -n fint2 > fint22'
        print 'head -n 1 fint >fint3; cat -n fint3 |sed \'s/ 1 /numid/g\'> fint33'
        print 'cat fint22 >>fint33'
        print 'cat -A fint33|sed \'s/^ *//g\'|sed \'s/\^M/ /g\'|sed \'s/\^I/ /g\'>fint333'
        print 'cut -d " " -f1-900 fint333>fint_1;cut -d " " -f1,901-1800 fint333>fint_2;cut -d " " -f1,1801-2700 fint333>fint_3;cut -d " " -f1,2701- fint333>fint_4;'
        # cut - " "\
        # echo "done"\                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     '



    if args.load_mind_data_f and args.load_mind_data_t:


        table_mind = args.load_mind_data_t
        mind_column_limit = 1000
        #check if exists
        #if overwrite is set delete and recreate table
        check_overwrite_table(table_mind)
        load_md2sql()

    elif (not args.load_mind_data_t and args.load_mind_data_f) or (not args.load_mind_data_f and args.load_mind_data_t):
        print "argument missing !"
        parser.print_help()
        sys.exit()





    if args.load_mind_rsids:

        global varmindrsids_table
        varmindrsids_table="mind_rsids"
        check_overwrite_table(varmindrsids_table)
        load_mind_rsids2sql()

    if args.join_mind_with_with_anno_ucsc_snp:
        join_mind_with_with_anno_ucsc_snp()
    
    if args.join_mind_rsids:
        join_mind_data_with_rsids(args.join_mind_rsids)

    if args.mind_a_ensembl:
         join_mind_table_with_anno_ensembl_snp()

    if args.mind_sort_by_gene_and_pos:
        mind_sort_by_gene_and_pos(args.mind_sort_by_gene_and_pos)

    if args.mind_prepare_ucsc_4_mind_annotations:
        uscs_split_allels(args.mind_prepare_ucsc_4_mind_annotations)
        ucsc_add_op_allele_strand(ucsc_table_split_alleles)

    if args.mind_export_ml:
        mind_export_ml()

    if args.mind_export_ml_with_drugs:
        mind_export_ml_with_drugs()

    if args.mind_export_ml_with_drugs_u:
        mind_export_ml_with_drugs_u()


    if args.mind_export_ml_with_drugs_alt:
        mind_export_ml_with_drugs_alt()

    if args.load_DBIdb_tables_preprocess:

        print "DBidb load whole database preprocess how to: "
        print "D:\DGIdb>D:\postgresql9.4\bin\psql.exe -U pyuser -d pydb -f DGIdb_schema.sql"
        print  "D:\DGIdb>D:\postgresql9.4\bin\psql.exe -U pyuser -d pydb -f DGIdb_dump.sql"

#add genes to drugs names here to a new table named drugs_genes_names;
    if args.create_drugs_genes_table:

        table_drug_name_and_claim_id = "drug_name_and_claim_id"
        table_gene_name_and_claim_id = "gene_name_and_claim_id"
        table_drug_claim_and_gene_name_1_intermediate = "drug_claim_and_gene_name_1_intermediate"
        table_gene_name_and_drug_name = "gene_name_and_drug_name"
        table_gene_name_and_drug_name_and_category = "gene_name_and_drug_name_and_category"
        table_gene_name_and_drug_name_and_category_filtered = "gene_name_and_drug_name_and_category_filtered"
        table_gene_name_and_drug_name_and_category_aggcat = "gene_name_and_drug_name_and_category_aggcat"
        table_gene_name_and_drug_name_and_category_aggcat_aggdrug = "gene_name_and_drug_name_and_category_aggcat_aggdrug"

        # join tables drugs and drug claim aliases - > creating name and drug claim id = drug_name_and_claim_id

        check_overwrite_table(table_drug_name_and_claim_id)
        print(cur.mogrify("CREATE TABLE %s AS SELECT name as drug_name,drug_claim_id FROM drugs inner join drug_claims_drugs on (drugs.id = drug_claims_drugs.drug_id)",(AsIs(table_drug_name_and_claim_id),)))
        cur.execute("CREATE TABLE %s AS SELECT name as drug_name,drug_claim_id FROM drugs inner join drug_claims_drugs on (drugs.id = drug_claims_drugs.drug_id)",(AsIs(table_drug_name_and_claim_id),))
        conn.commit()

        #join tables genes and gene_claims_genes -> creating name and gene_claim_id = gene_name_and_claim_id

        check_overwrite_table(table_gene_name_and_claim_id)
        print(cur.mogrify("CREATE TABLE %s AS SELECT name as gene_name,gene_claim_id FROM genes inner join gene_claims_genes on (genes.id = gene_claims_genes.gene_id)",(AsIs(table_gene_name_and_claim_id),)))
        cur.execute("CREATE TABLE %s AS SELECT name as gene_name,gene_claim_id FROM genes inner join gene_claims_genes on (genes.id = gene_claims_genes.gene_id)",(AsIs(table_gene_name_and_claim_id),))
        conn.commit()

        #join tables gene_name_and_claim_id and interaction_claims creating -> gene_name , gene_claim_id and drug_claim_id = drug_claim_and_gene_name_1_intermediate

        check_overwrite_table(table_drug_claim_and_gene_name_1_intermediate)
        print(cur.mogrify("CREATE TABLE %s AS SELECT gene_name,gene_name_and_claim_id.gene_claim_id,drug_claim_id FROM gene_name_and_claim_id inner join interaction_claims on (gene_name_and_claim_id.gene_claim_id = interaction_claims.gene_claim_id)",(AsIs(table_drug_claim_and_gene_name_1_intermediate),)))
        cur.execute("CREATE TABLE %s AS SELECT gene_name,gene_name_and_claim_id.gene_claim_id,drug_claim_id FROM gene_name_and_claim_id inner join interaction_claims on (gene_name_and_claim_id.gene_claim_id = interaction_claims.gene_claim_id)",(AsIs(table_drug_claim_and_gene_name_1_intermediate),))
        conn.commit()

        #join tables  gene_name_and_claim_id and interaction_claims creating -> gene_name and drug_name = drug_name_gene_name

        check_overwrite_table(table_gene_name_and_drug_name)
        print(cur.mogrify("CREATE TABLE %s AS SELECT gene_name,drug_claim_and_gene_name_1_intermediate.gene_claim_id,drug_claim_and_gene_name_1_intermediate.drug_claim_id,drug_name FROM drug_claim_and_gene_name_1_intermediate inner join drug_name_and_claim_id on (drug_name_and_claim_id.drug_claim_id = drug_claim_and_gene_name_1_intermediate.drug_claim_id)",(AsIs(table_gene_name_and_drug_name),)))
        cur.execute("CREATE TABLE %s AS SELECT gene_name,drug_claim_and_gene_name_1_intermediate.gene_claim_id,drug_claim_and_gene_name_1_intermediate.drug_claim_id,drug_name FROM drug_claim_and_gene_name_1_intermediate inner join drug_name_and_claim_id on (drug_name_and_claim_id.drug_claim_id = drug_claim_and_gene_name_1_intermediate.drug_claim_id)",(AsIs(table_gene_name_and_drug_name),))
        conn.commit()

          #create gene_name_and_drug_name_and_category table join table_gene_name_and_drug_name with drug_claim_attributes = gene_name,drug_name,drug_claim_id, value,name

        check_overwrite_table(table_gene_name_and_drug_name_and_category)

        print(cur.mogrify("CREATE TABLE %s AS SELECT gene_name,gene_claim_id,gene_name_and_drug_name.drug_claim_id,drug_name,drug_claim_attributes.value ,drug_claim_attributes.name FROM drug_claim_attributes inner join gene_name_and_drug_name on (drug_claim_attributes.drug_claim_id = gene_name_and_drug_name.drug_claim_id)",(AsIs(table_gene_name_and_drug_name_and_category),)))
        cur.execute("CREATE TABLE %s AS SELECT gene_name,gene_claim_id,gene_name_and_drug_name.drug_claim_id,drug_name,drug_claim_attributes.value ,drug_claim_attributes.name FROM drug_claim_attributes inner join gene_name_and_drug_name on (drug_claim_attributes.drug_claim_id = gene_name_and_drug_name.drug_claim_id)",(AsIs(table_gene_name_and_drug_name_and_category),))
        conn.commit()

#add column "drug_function_categories" string_agg of gene | drug categories...

        check_overwrite_table(table_gene_name_and_drug_name_and_category_aggcat)

        print(cur.mogrify("CREATE TABLE %s AS select gene_name,gene_claim_id,drug_name,drug_claim_id,string_agg(value,'/') as drug_categories from (select distinct gene_name,gene_claim_id,drug_name,drug_claim_id,name,value from gene_name_and_drug_name_and_category) as gdc where name='Drug Categories' group by drug_claim_id,gene_name,gene_claim_id,drug_name; ",(AsIs(table_gene_name_and_drug_name_and_category_aggcat),)))
        cur.execute("CREATE TABLE %s AS select gene_name,gene_claim_id,drug_name,drug_claim_id,string_agg(value,'/') as drug_categories from (select distinct gene_name,gene_claim_id,drug_name,drug_claim_id,name,value from gene_name_and_drug_name_and_category) as gdc where name='Drug Categories' group by drug_claim_id,gene_name,gene_claim_id,drug_name; ",(AsIs(table_gene_name_and_drug_name_and_category_aggcat),))
        conn.commit()


        check_overwrite_table(table_gene_name_and_drug_name_and_category_aggcat_aggdrug)

        print(cur.mogrify("CREATE TABLE %s AS select gene_name,string_agg(dg1.drug_name2,' ') as drugs_info from (select distinct gene_name,drug_name||'{'||drug_categories||'}' as drug_name2 from gene_name_and_drug_name_and_category_aggcat) as dg1 group by gene_name;  ",(AsIs(table_gene_name_and_drug_name_and_category_aggcat_aggdrug),)))
        cur.execute("CREATE TABLE %s AS select gene_name,string_agg(dg1.drug_name2,' ') as drugs_info from (select distinct gene_name,drug_name||'{'||drug_categories||'}' as drug_name2 from gene_name_and_drug_name_and_category_aggcat) as dg1 group by gene_name;  ",(AsIs(table_gene_name_and_drug_name_and_category_aggcat_aggdrug),))
        conn.commit()

    if args.create_drugs_genes_table_extended_gene_interactions:
        #create extended genes tables

        ##we want the gene_name to be derived from interactive_gene_id and the grnr_claim_id from gene_name_id

        table_drug_name_and_claim_id_alt = "drug_name_and_claim_id_alt"
        table_gene_name_alt = "gene_name_alt"
        table_gene_name_and_interactive_gene_name_alt = "gene_name_and_interactive_gene_name_alt"
        table_gene_name_and_claim_id_alt = "gene_name_and_claim_id_alt"
        table_drug_claim_and_gene_name_1_intermediate_alt = "drug_claim_and_gene_name_1_intermediate_alt"
        table_gene_name_and_drug_name_alt = "gene_name_and_drug_name_alt"
        table_gene_name_and_drug_name_and_category_alt = "gene_name_and_drug_name_and_category_alt"
        table_gene_name_and_drug_name_and_category_filtered_alt = "gene_name_and_drug_name_and_category_filtered_alt"
        table_gene_name_and_drug_name_and_category_aggcat_alt= "gene_name_and_drug_name_and_category_aggcat_alt"
        table_gene_and_drug_name_category_agg_cat_drug_alt = "gene_and_drug_name_category_agg_cat_drug_alt"
        table_gene_and_drug_name_category_agg_cat_drug_int_alt = "gene_and_drug_name_category_agg_cat_drug_int_alt"

        # join tables drugs and drug claim aliases - > creating name and drug claim id = drug_name_and_claim_id_alt

        check_overwrite_table(table_drug_name_and_claim_id_alt)
        print(cur.mogrify("CREATE TABLE %s AS SELECT name as drug_name,drug_claim_id FROM drugs inner join drug_claims_drugs on (drugs.id = drug_claims_drugs.drug_id)",(AsIs(table_drug_name_and_claim_id_alt),)))
        cur.execute("CREATE TABLE %s AS SELECT name as drug_name,drug_claim_id FROM drugs inner join drug_claims_drugs on (drugs.id = drug_claims_drugs.drug_id)",(AsIs(table_drug_name_and_claim_id_alt),))
        conn.commit()

        #create join table of alt genes and their names gene_id,gene_id_alt,name.

        check_overwrite_table(table_gene_name_alt)
        print(cur.mogrify("CREATE TABLE %s AS SELECT name as gene_name,gene_id,interacting_gene_id as gene_id_alt FROM genes inner join gene_gene_interaction_claims on (genes.id = gene_gene_interaction_claims.interacting_gene_id)",(AsIs(table_gene_name_alt),)))
        cur.execute("CREATE TABLE %s AS SELECT name as gene_name,gene_id,interacting_gene_id as gene_id_alt FROM genes inner join gene_gene_interaction_claims on (genes.id = gene_gene_interaction_claims.interacting_gene_id)",(AsIs(table_gene_name_alt),))
        conn.commit()

        #create join table of alt genes and their names gene_id,gene_id_alt,name.

        check_overwrite_table(table_gene_name_and_interactive_gene_name_alt)
        print(cur.mogrify("CREATE TABLE %s AS SELECT name as interactive_gene_name,gene_name,gene_id,gene_id_alt as gene_id_alt FROM genes inner join gene_name_alt on (genes.id = gene_name_alt.gene_id)",(AsIs(table_gene_name_and_interactive_gene_name_alt),)))
        cur.execute("CREATE TABLE %s AS SELECT name as interactive_gene_name,gene_name,gene_id,gene_id_alt as gene_id_alt FROM genes inner join gene_name_alt on (genes.id = gene_name_alt.gene_id)",(AsIs(table_gene_name_and_interactive_gene_name_alt),))
        conn.commit()

        #join tables genes and gene_claims_genes -> creating name and gene_claim_id = gene_name_and_claim_id_alt

        check_overwrite_table(table_gene_name_and_claim_id_alt)
        print(cur.mogrify("CREATE TABLE %s AS SELECT gene_id_alt,gene_name,gene_claim_id FROM gene_name_alt inner join gene_claims_genes on (gene_name_alt.gene_id = gene_claims_genes.gene_id)",(AsIs(table_gene_name_and_claim_id_alt),)))
        cur.execute("CREATE TABLE %s AS SELECT gene_id_alt,gene_name,gene_claim_id FROM gene_name_alt inner join gene_claims_genes on (gene_name_alt.gene_id = gene_claims_genes.gene_id)",(AsIs(table_gene_name_and_claim_id_alt),))
        conn.commit()

        #join tables gene_name_and_claim_id_alt and interaction_claims creating -> gene_name , gene_claim_id and drug_claim_id = drug_claim_and_gene_name_1_intermediate_alt

        check_overwrite_table(table_drug_claim_and_gene_name_1_intermediate_alt)
        print(cur.mogrify("CREATE TABLE %s AS SELECT gene_id_alt,gene_name,gene_name_and_claim_id_alt.gene_claim_id,drug_claim_id FROM gene_name_and_claim_id_alt inner join interaction_claims on (gene_name_and_claim_id_alt.gene_claim_id = interaction_claims.gene_claim_id)",(AsIs(table_drug_claim_and_gene_name_1_intermediate_alt),)))
        cur.execute("CREATE TABLE %s AS SELECT gene_id_alt,gene_name,gene_name_and_claim_id_alt.gene_claim_id,drug_claim_id FROM gene_name_and_claim_id_alt inner join interaction_claims on (gene_name_and_claim_id_alt.gene_claim_id = interaction_claims.gene_claim_id)",(AsIs(table_drug_claim_and_gene_name_1_intermediate_alt),))
        conn.commit()

        #join tables  gene_name_and_claim_id_alt and interaction_claims creating -> gene_name and drug_name = drug_name_gene_name

        check_overwrite_table(table_gene_name_and_drug_name_alt)
        print(cur.mogrify("CREATE TABLE %s AS SELECT gene_id_alt,gene_name,drug_claim_and_gene_name_1_intermediate_alt.gene_claim_id,drug_claim_and_gene_name_1_intermediate_alt.drug_claim_id,drug_name FROM drug_claim_and_gene_name_1_intermediate_alt inner join drug_name_and_claim_id_alt on (drug_name_and_claim_id_alt.drug_claim_id = drug_claim_and_gene_name_1_intermediate_alt.drug_claim_id)",(AsIs(table_gene_name_and_drug_name_alt),)))
        cur.execute("CREATE TABLE %s AS SELECT gene_id_alt,gene_name,drug_claim_and_gene_name_1_intermediate_alt.gene_claim_id,drug_claim_and_gene_name_1_intermediate_alt.drug_claim_id,drug_name FROM drug_claim_and_gene_name_1_intermediate_alt inner join drug_name_and_claim_id_alt on (drug_name_and_claim_id_alt.drug_claim_id = drug_claim_and_gene_name_1_intermediate_alt.drug_claim_id)",(AsIs(table_gene_name_and_drug_name_alt),))
        conn.commit()

          #create gene_name_and_drug_name_and_category_alt table join table_gene_name_and_drug_name_alt with drug_claim_attributes = gene_name,drug_name,drug_claim_id, value,name

        check_overwrite_table(table_gene_name_and_drug_name_and_category_alt)

        print(cur.mogrify("CREATE TABLE %s AS SELECT gene_id_alt,gene_name,gene_claim_id,gene_name_and_drug_name_alt.drug_claim_id,drug_name,drug_claim_attributes.value ,drug_claim_attributes.name FROM drug_claim_attributes inner join gene_name_and_drug_name_alt on (drug_claim_attributes.drug_claim_id = gene_name_and_drug_name_alt.drug_claim_id)",(AsIs(table_gene_name_and_drug_name_and_category_alt),)))
        cur.execute("CREATE TABLE %s AS SELECT gene_id_alt,gene_name,gene_claim_id,gene_name_and_drug_name_alt.drug_claim_id,drug_name,drug_claim_attributes.value ,drug_claim_attributes.name FROM drug_claim_attributes inner join gene_name_and_drug_name_alt on (drug_claim_attributes.drug_claim_id = gene_name_and_drug_name_alt.drug_claim_id)",(AsIs(table_gene_name_and_drug_name_and_category_alt),))
        conn.commit()

#add column "drug_function_categories" string_agg of gene | drug categories...

        check_overwrite_table(table_gene_name_and_drug_name_and_category_aggcat_alt)

        print(cur.mogrify("CREATE TABLE %s AS select gene_name,gene_claim_id,drug_name,drug_claim_id,string_agg(value,'/') as drug_categories from (select distinct gene_name,gene_claim_id,drug_name,drug_claim_id,name,value from gene_name_and_drug_name_and_category_alt) as gdc where name='Drug Categories' group by drug_claim_id,gene_name,gene_claim_id,drug_name; ",(AsIs(table_gene_name_and_drug_name_and_category_aggcat_alt),)))
        cur.execute("CREATE TABLE %s AS select gene_name,gene_claim_id,drug_name,drug_claim_id,string_agg(value,'/') as drug_categories from (select distinct gene_name,gene_claim_id,drug_name,drug_claim_id,name,value from gene_name_and_drug_name_and_category_alt) as gdc where name='Drug Categories' group by drug_claim_id,gene_name,gene_claim_id,drug_name; ",(AsIs(table_gene_name_and_drug_name_and_category_aggcat_alt),))
        conn.commit()


        check_overwrite_table(table_gene_and_drug_name_category_agg_cat_drug_alt)

        print(cur.mogrify("CREATE TABLE %s AS select gene_name,string_agg(dg1.drug_name2,' ') as drugs_info from (select distinct gene_name,drug_name||'{'||drug_categories||'}' as drug_name2 from gene_name_and_drug_name_and_category_aggcat_alt) as dg1 group by gene_name;  ", (AsIs(table_gene_and_drug_name_category_agg_cat_drug_alt),)))
        cur.execute("CREATE TABLE %s AS select gene_name,string_agg(dg1.drug_name2,' ') as drugs_info from (select distinct gene_name,drug_name||'{'||drug_categories||'}' as drug_name2 from gene_name_and_drug_name_and_category_aggcat_alt) as dg1 group by gene_name;  ", (AsIs(table_gene_and_drug_name_category_agg_cat_drug_alt),))
        conn.commit()

        # add gene_name_and_interactive_gene_name_alt annotation with :  | interactive_gene_name

        check_overwrite_table(table_gene_and_drug_name_category_agg_cat_drug_int_alt)
        print(cur.mogrify(
            "CREATE TABLE %s AS SELECT gene_and_drug_name_category_agg_cat_drug_alt.gene_name ,drugs_info, interactive_gene_name FROM gene_and_drug_name_category_agg_cat_drug_alt inner join gene_name_and_interactive_gene_name_alt on (gene_and_drug_name_category_agg_cat_drug_alt.gene_name = gene_name_and_interactive_gene_name_alt.gene_name)",
            (AsIs(table_gene_and_drug_name_category_agg_cat_drug_int_alt),)))
        cur.execute(
            "CREATE TABLE %s AS SELECT gene_and_drug_name_category_agg_cat_drug_alt.gene_name ,drugs_info, interactive_gene_name FROM gene_and_drug_name_category_agg_cat_drug_alt inner join gene_name_and_interactive_gene_name_alt on (gene_and_drug_name_category_agg_cat_drug_alt.gene_name = gene_name_and_interactive_gene_name_alt.gene_name)",
            (AsIs(table_gene_and_drug_name_category_agg_cat_drug_int_alt),))
        conn.commit()

    if args.filter_mind_table_by_drugs:
#filter mind_data_1-4_rs_ensorted_by_gene_posann tables by : join with table gene_name_and_drug_name on gene_name
        table_mind_data_n_rs_ensorted_by_gene_posann = args.filter_mind_table_by_drugs
        table_mind_data_n_rs_ensorted_by_gene_posann_by_drug = args.filter_mind_table_by_drugs+"_by_drug"
        table_gene_name_and_drug_name_and_category_aggcat_aggdrug2 = "gene_name_and_drug_name_and_category_aggcat_aggdrug2"


        check_overwrite_table(table_gene_name_and_drug_name_and_category_aggcat_aggdrug2)

        #copy and alter drugs table gene_name column to gene_name2 for easy joining
        print(cur.mogrify("CREATE TABLE %s AS SELECT * FROM gene_name_and_drug_name_and_category_aggcat_aggdrug",(AsIs(table_gene_name_and_drug_name_and_category_aggcat_aggdrug2),)))
        cur.execute("CREATE TABLE %s AS SELECT * FROM gene_name_and_drug_name_and_category_aggcat_aggdrug",(AsIs(table_gene_name_and_drug_name_and_category_aggcat_aggdrug2),))
        conn.commit()

        #copy and alter drugs table gene_name column to gene_name2 for easy joining
        print(cur.mogrify("alter table  %s rename column gene_name to gene_name2",(AsIs(table_gene_name_and_drug_name_and_category_aggcat_aggdrug2),)))
        cur.execute("alter table  %s rename column gene_name to gene_name2",(AsIs(table_gene_name_and_drug_name_and_category_aggcat_aggdrug2),))
        conn.commit()

        check_overwrite_table(table_mind_data_n_rs_ensorted_by_gene_posann_by_drug)

        # join mind table with table gene_name_and_drug_name on gene_name
        print(cur.mogrify("CREATE TABLE %s AS SELECT * FROM %s inner join %s on (%s.gene_name2 = %s.gene_name)",(AsIs(table_mind_data_n_rs_ensorted_by_gene_posann_by_drug),AsIs(table_gene_name_and_drug_name_and_category_aggcat_aggdrug2),AsIs(table_mind_data_n_rs_ensorted_by_gene_posann),AsIs(table_gene_name_and_drug_name_and_category_aggcat_aggdrug2),AsIs(table_mind_data_n_rs_ensorted_by_gene_posann),)))
        cur.execute("CREATE TABLE %s AS SELECT * FROM %s inner join %s on (%s.gene_name2 = %s.gene_name)",(AsIs(table_mind_data_n_rs_ensorted_by_gene_posann_by_drug),AsIs(table_gene_name_and_drug_name_and_category_aggcat_aggdrug2),AsIs(table_mind_data_n_rs_ensorted_by_gene_posann),AsIs(table_gene_name_and_drug_name_and_category_aggcat_aggdrug2),AsIs(table_mind_data_n_rs_ensorted_by_gene_posann),))
        conn.commit()



    if args.filter_mind_table_by_drugs_extended_gene_interactions:
#filter mind_data_1-4_rs_ensorted_by_gene_posann tables by : join with table gene_name_and_drug_name on gene_name

        filter_mind_table_by_drugs_extended_gene_interactions_alt = args.filter_mind_table_by_drugs_extended_gene_interactions
        filter_mind_table_by_drugs_extended_gene_interactions_alt_by_drug = args.filter_mind_table_by_drugs_extended_gene_interactions+"_by_drug_alt"
        table_gene_drug_name_and_category_aggcat_aggdrug2_alt = "gene_and_drug_name_category_agg_cat_drug_int_alt2"
        table_gene_drug_name_and_category_aggcat_aggdrug2_a_u="gene_and_drug_name_category_agg_cat_drug_int_a_u"


        check_overwrite_table(table_gene_drug_name_and_category_aggcat_aggdrug2_alt)

        #copy and alter drugs table gene_name column to gene_name2 for easy joining
        print(cur.mogrify("CREATE TABLE %s AS SELECT * FROM gene_and_drug_name_category_agg_cat_drug_int_alt",(AsIs(table_gene_drug_name_and_category_aggcat_aggdrug2_alt),)))
        cur.execute("CREATE TABLE %s AS SELECT * FROM gene_and_drug_name_category_agg_cat_drug_int_alt",(AsIs(table_gene_drug_name_and_category_aggcat_aggdrug2_alt),))
        conn.commit()

        #copy and alter drugs table gene_name column to gene_name2 for easy joining
        print(cur.mogrify("alter table  %s rename column gene_name to gene_name2",(AsIs(table_gene_drug_name_and_category_aggcat_aggdrug2_alt),)))
        cur.execute("alter table  %s rename column gene_name to gene_name2",(AsIs(table_gene_drug_name_and_category_aggcat_aggdrug2_alt),))
        conn.commit()

#method fix :
#create uniq alt table without interacting genes since it is creating too big of a table

        check_overwrite_table(table_gene_drug_name_and_category_aggcat_aggdrug2_a_u)

        print(cur.mogrify("create table %s as select distinct gene_name2,drugs_info from gene_and_drug_name_category_agg_cat_drug_int_alt2",(AsIs(table_gene_drug_name_and_category_aggcat_aggdrug2_a_u),)))
        cur.execute("create table %s as select distinct gene_name2,drugs_info from gene_and_drug_name_category_agg_cat_drug_int_alt2",(AsIs(table_gene_drug_name_and_category_aggcat_aggdrug2_a_u),))
        conn.commit()

        check_overwrite_table(filter_mind_table_by_drugs_extended_gene_interactions_alt_by_drug)
        #method fix : changed to join from the a_u table

        # join mind table with table gene_name_and_drug_name on gene_name
        print(cur.mogrify("CREATE TABLE %s AS SELECT * FROM %s inner join %s on (%s.gene_name2 = %s.gene_name)",(AsIs(filter_mind_table_by_drugs_extended_gene_interactions_alt_by_drug),AsIs(table_gene_drug_name_and_category_aggcat_aggdrug2_a_u),AsIs(filter_mind_table_by_drugs_extended_gene_interactions_alt),AsIs(table_gene_drug_name_and_category_aggcat_aggdrug2_a_u),AsIs(filter_mind_table_by_drugs_extended_gene_interactions_alt),)))

        cur.execute("CREATE TABLE %s AS SELECT * FROM %s inner join %s on (%s.gene_name2 = %s.gene_name)",(AsIs(filter_mind_table_by_drugs_extended_gene_interactions_alt_by_drug),AsIs(table_gene_drug_name_and_category_aggcat_aggdrug2_a_u),AsIs(filter_mind_table_by_drugs_extended_gene_interactions_alt),AsIs(table_gene_drug_name_and_category_aggcat_aggdrug2_a_u),AsIs(filter_mind_table_by_drugs_extended_gene_interactions_alt),))
        conn.commit()



# # *************************************************************8
# #multi core section
# #select core num and execute function in pool
# #function should be in top level so disable cache exeption
#
#     # if args.multi_core_num:
#     #
#     #     if __name__ == '__main__':
#     #         logger = mp.log_to_stderr()
#     #         logger.setLevel(logging.DEBUG)
#     #
#     #         if args.multi_core_num == "max":
#     #
#     #             max_cpu = mp.cpu_count()
#     #             pool = mp.Pool(processes=max_cpu)
#     #             print "cpu count", int(max_cpu)
#     #             # results = [pool.apply_async(multiquery, (setting,)) for setting in range(2)]
#     #             for setting in range(2):
#     #                 pool.apply_async(multiquery,(setting,))
#     #             # pool.apply_async(multiquery(1))
#     #
#     #         if isInt(args.multi_core_num):
#     #             pool = mp.Pool(processes=int(args.multi_core_num))
#     #             print "int ok", args.multi_core_num
#     #             results = [pool.apply_async(multifunc, (setting,)) for setting in range(2)]
#
#         # print 'Ordered results using pool.apply_async():'
#
#     # for result in results:
#     #     # print '\t', result.get()
#     #     # print '\t', result.ready()
#     #     print '\t', result.successful()
#             # with open(settings_file) as f:
#                 for line in f:
# #         config = script2p+" "+line
# #         print "command :", config
# #         settings_list.append(config)






#end of program execution
except (KeyboardInterrupt, SystemExit):
    print "\n Program interrupted  ! \n Clean up in progress..."
    interrans = raw_input("Are you sure you want to cleanup stuff ? (yes/no)")
    if interrans == "yes":
        print "ok doing cleanup stuff, clean up stuff disabled for now"
        # cleanup_err_tables()
    else:
        print "ok , leaving all untouched.."
        sys.exit()

print "All done."



