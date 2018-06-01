#!/bin/sh
# Author: veda.shankar@mapd.com (Veda Shankar)
# A script to copy objects (compressed bitcoin transactions) from Google 
# Storage Cloud Bucket, uncompress it and call the Python application to
# process it.
#

# Make sure the script is called with the bucket path as argument
if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <Storage Cloud Bucket Path>" 
  echo "Example: $0 gs://vedabc" 
  exit 1
fi
bucket_name=$1

# Make sure the utility gsutil is in the PATH
if ! [ -x "$(command -v gsutil)" ]; then
  echo 'Error: gsutil is not installed or not in the PATH' >&2
  exit 1
fi

# Main loop for iterating over all the objects in the bucket
for i in `gsutil ls $bucket_name`
do
  echo $i
  rm -f chunk.json btc_chunk.csv btc_quandl.csv btc_chunk_merged.csv btc_chunk_final.csv.gz
  gsutil cp $i chunk.json.gz
  if ! [ -e chunk.json.gz ];then
    echo "Error: could not find object $i" >&2
    exit 1
  fi
  gunzip chunk.json.gz
  python btc_merge_tables.py
done
