# Bitcoin Project
This application converts the JSON file containing Bitcoin transactions to a CSV format. It merges all the Quandl Bitcoin metrics tables and joins it with the Bitcoin transactions table using the transaction date field. It uses the top 100 popular bitcoin addresses CSV file to mark each input/output address used in the transaction as a ordinary or popular address.  The final CSV file is then ingested into a MapD table.

# Install Software Packages
- pip install matplotlib pandas python-gflags
- pip install pymapd
- git clone https://github.com/mapd/bitcoin-project.git (this repo)
- Follow the instructions for installing gsutil from the blog

# Usage
Kickoff the workflow by calling the shell script bigquery2mapd.sh and pass the name of the Google Storage Cloud bucket containing the transactions as an argument.
- Example: ./bigquery2mapd.sh gs://vedabc/

# Workflow
The Python application performs the following steps for each object in the Storage Cloud bucket:
- Copy Object from Storage Cloud to chunk.json.gz
- Gunzip chunk.json.gz
- Execute btc_json2csv() to extract transactions from JSON format to CSV format
- Created btc_chunk.csv
- Read btc_chunk.csv and btc_quandl.csv (merged tables of all BTC metrics) into Pandas data frames 
- Perform an inner merge on the two data frames using the transaction_date field
- Insert columns input_pubkey_type & output_pubkey_type
- Set the address type to either ordinary or popular based on whether the Bitcoin address in the transaction belong to the Top 100 popular Bitcoin address list (popular_btc_addresses.csv)
- Write out the data frame to a CSV file btc_chunk_final.csv
- Connect to MapD
- Create MapD table if it does not exist
- Using COPY FROM command load data into MapD table 
- Close connection to MapD

