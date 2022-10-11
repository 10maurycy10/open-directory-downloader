"""
Detect blobs that dont have a db entry.
"""

import mariadb
import json
import os
import tqdm
import sys

config = json.loads(open("config.json").read())

db = mariadb.connect(
                user=config["db_username"],
                password=config["db_passwd"],
                host=config["db_host"],
                port=config["db_port"],
                database=config["db_dbname"]
)
for line in sys.stdin.readlines():
    line = line.rstrip()
    dbc = db.cursor()
    dbc.execute("select * from paths where blobid = ?", (line,))
    if len(list(dbc)) == 0:
        f = open("orphaned", "a")
        f.write(f"blobs/{line}\n");
        f.close()
        print(line)
