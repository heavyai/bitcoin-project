#!/usr/bin/python
  
"""
  Module for connecting to MapD database and creating tables with the provided data.
"""

__author__ = 'veda.shankar@gmail.com (Veda Shankar)'

if __name__ == "__main__":
  import argparse
  import sys
  import string
  import csv

import os
import pandas as pd
from pymapd import connect

connection = "NONE"

# Connect to the DB
def connect_to_mapd(str_user, str_password, str_host, str_dbname):
  global connection
  connection = connect(user=str_user, password=str_password, host=str_host, dbname=str_dbname)
  print connection

def drop_table_mapd(table_name):
  command = 'drop table if exists %s' % (table_name)
  print command
  connection.execute(command)

# Load CSV to Table
def load_to_mapd(table_name, csv_file, mapd_host, mapd_user):
  global connection
  create_table_str = 'CREATE TABLE IF NOT EXISTS %s (transaction_date DATE, transaction_fees_usd FLOAT, transaction_volume_usd FLOAT, btc_difficulty FLOAT, btc_price_usd FLOAT, btc_addresses_used FLOAT, transactions_exclude_popular FLOAT, total_transactions FLOAT, transaction_datetime TIMESTAMP, transaction_id TEXT ENCODING DICT(32), block_id TEXT ENCODING DICT(32), prev_block_id TEXT ENCODING DICT(32), input_sequence BIGINT, input_pubkey TEXT ENCODING DICT(32), output_pubkey TEXT ENCODING DICT(32), output_satoshis FLOAT, input_pubkey_type TEXT ENCODING DICT(32), output_pubkey_type TEXT ENCODING DICT(32), output_btc FLOAT)' % (table_name)

  print create_table_str
  connection.execute(create_table_str)
  server_csv_file = '%s' % (csv_file)
  query = 'COPY %s from \'%s\' WITH (nulls = \'None\')' % (table_name, server_csv_file)
  print query
  connection.execute(query)
  print connection.get_table_details(table_name)
  connection.close()

