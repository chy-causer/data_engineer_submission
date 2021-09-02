import mysql.connector
import csv
import requests
import gzip
from getpass import getpass, getuser

infile = 'unique_devices_per_domain_daily-2021-06-19.gz'
url = f"https://dumps.wikimedia.org/other/unique_devices/per_domain/2021/2021-06/{infile}"
outfile = 'unique_devices_wikipedia_gt_100000.txt'

r = requests.get(url, allow_redirects=True)
open(infile, 'wb').write(r.content)

f = gzip.open(infile, 'rb')
file_content = f.read().decode("utf-8").split('\n')

connection = mysql.connector.connect(host='localhost',database='bbc',user=getuser(),password=getpass())
cursor = connection.cursor(prepared=True)

cursor.execute('DROP TABLE IF EXISTS bbc.devices_per_domain')
# Column order from spec sheet: <domain> <uniques_underestimate> <uniques_offset> <uniques_estimate>
cursor.execute(
    """CREATE TABLE IF NOT EXISTS bbc.devices_per_domain(
      domain varchar(50),
      uniques_underestimate bigint,
      uniques_offset bigint,
      uniques_estimate bigint)
    ;"""
)

# The exercise specifies a whitespace-delimited input file, but it appears to be tab-delimited.
for row in file_content:
    if row:
        cursor.execute("INSERT INTO bbc.devices_per_domain VALUES(%s, %s, %s, %s)", tuple(row.split('\t')))

# Commit the insertions so the table is populated in future (if desired)
connection.commit()

# No sorting specified for output file in data_engineer_technical_exercise.pdf, so none applied here
# Double %% to escape the % so python doesn't think it's a %-formatting
cursor.execute('SELECT * FROM devices_per_domain WHERE DOMAIN LIKE \'%%wikipedia.org\' AND uniques_estimate > 100000;')
result = cursor.fetchall()

# note the spec for the same format as input file, so no column headers
with open(outfile, 'w', newline='\n') as f:
    for row in result:
        csv.writer(f, delimiter='\t').writerow(row)

# Note: Running the above returns 64 records

