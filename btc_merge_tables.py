#!/usr/bin/python
"""
This application converts the JSON file containing Bitcoin transactions 
to a CSV format. It merges all the Quandl Bitcoin metrics tables and 
joins it with the Bitcoin transactions table using the transaction date
field. It uses the top 100 popular bitcoin addresses CSV file to mark each
input/output address used in the transaction as a ordinary or popular address.
The final CSV file is then ingested into a MapD table.
"""
__author__ = 'veda.shankar@mapd.com (Veda Shankar)'

import argparse
import sys
import json
import csv
import string
import datetime
import os
import re
import pandas as pd
import numpy as np
from pprint import pprint

# MAPD utility
from mapd_utils import *

# Quandl Bitcoin Metrics files
csv_list = ['./data/BCHAIN-TRFUS.csv', './data/BCHAIN-ETRVU.csv', './data/BCHAIN-DIFF.csv', './data/BCHAIN-MKPRU.csv', './data/BCHAIN-NADDU.csv', './data/BCHAIN-NTREP.csv', './data/BCHAIN-NTRAT.csv']

# Convert Bitcoin transactions from JSON to CSV
def btc_json2csv():
  filepath = 'chunk.json'  
  tran_data = open('btc_chunk.csv', 'w')
  csvwriter = csv.writer(tran_data)
  header = ["transaction_datetime","transaction_date","transaction_id","block_id","prev_block_id","input_sequence","input_pubkey","output_pubkey","output_satoshis","output_btc"]
  csvwriter.writerow(header)
  with open(filepath) as fp:  
    line = fp.readline()
    while line:
      data = json.loads(line)
      num_inputs = len(data['inputs'])
      for inputs in data['inputs']:
        if 'input_pubkey_base58' in inputs:
          for outputs in data['outputs']:  
            if 'output_pubkey_base58' in outputs:
              btc = float(outputs['output_satoshis'])/100000000.0
              btc = btc / float(num_inputs)
              writeline = '%s %s %s %s %s %s %s %s %s %f' % (
              data['timestamp'], data['timestamp'], data['transaction_id'], data['block_id'], data['previous_block'], inputs['input_sequence_number'] ,inputs['input_pubkey_base58'], outputs['output_pubkey_base58'], outputs['output_satoshis'], btc)
              list = writeline.split()
              list[0] = '%s' % (datetime.datetime.fromtimestamp(int(data['timestamp'])/1000).strftime('%m/%d/%Y %H:%M:%S'))
              list[1] = '%s' % (datetime.datetime.fromtimestamp(int(data['timestamp'])/1000).strftime('%m/%d/%Y'))
              csvwriter.writerow(list)
      line = fp.readline()
  fp.close()
  tran_data.close()

# Merge all the Quandl Bitcoin Metric files and finally with 
# the Bitcoin transactions file.
def merge_tables():
  for i in xrange(0,len(csv_list),1):
    print csv_list[i]
    if i == (len(csv_list) - 1):
      print "exiting for loop ..."
      break
  
    if i == 0:
      df1 = pd.read_csv(csv_list[0])
      df2 = pd.read_csv(csv_list[1])
    else:
      df1 = pd.read_csv('./btc_quandl.csv')
      os.system("rm -f ./btc_quandl.csv")
      df2 = pd.read_csv(csv_list[i+1])
  
    combo = pd.merge(df1, df2, how='left')
    df = pd.DataFrame(combo)
    df = df.fillna("None")
    df.to_csv('./btc_quandl.csv', index=False)
    del df1
    del df2
    del combo
  
  df1 = pd.read_csv('./btc_quandl.csv')
  df1.transaction_date = df1.transaction_date.apply(str)
  df1['transaction_date'] = df1['transaction_date'].str.replace(r'(\d\d\d\d)-(\d\d)-(\d\d)', r'\2/\3/\1')
  df2 = pd.read_csv('btc_chunk.csv')
  df = df1.merge(df2, left_on='transaction_date', right_on='transaction_date', how='inner')

  del df1
  del df2
  df.insert(16, 'input_pubkey_type', df['input_pubkey'])
  df.insert(17, 'output_pubkey_type', df['output_pubkey'])
  df.to_csv("btc_chunk_merged.csv", index=False)

btc_json2csv()
merge_tables()
df1 = pd.read_csv('./data/popular_btc_addresses.csv')
populars = df1.popular_addresses.tolist()

def set_popular(x):
  try:
    n = populars.index(x)
    x = 'popular'
  except ValueError:
    x = 'ordinary'
  return x

df2 = pd.read_csv("btc_chunk_merged.csv")
df2['input_pubkey_type'] = df2.loc[:,'input_pubkey_type'].apply(set_popular)
df2['output_pubkey_type'] = df2.loc[:,'output_pubkey_type'].apply(set_popular)
df2['output_btc'] = df2['output_btc'].multiply(df2['btc_price_usd'], axis="index")
df2.to_csv("btc_chunk_final.csv", index=False)  

os.system("gzip btc_chunk_final.csv")
fullpath = "%s/btc_chunk_final.csv.gz" % (os.getcwd())
connect_to_mapd("mapd", "HyperInteractive", "localhost", "mapd")
load_to_mapd("btc_merged_table", fullpath, "none", "none")

