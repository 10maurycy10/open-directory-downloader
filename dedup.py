"""
A quick work arround for the duplication issue
"""

import mariadb
import json
import os
import tqdm

config = json.loads(open("config.json").read())

db = mariadb.connect(
                user=config["db_username"],
                password=config["db_passwd"],
                host=config["db_host"],
                port=config["db_port"],
                database=config["db_dbname"]
)

dbc = db.cursor()
dbc.execute("select full,count(*) from paths group by full having count(*) > 1;")
dups = list(dbc)

for (url, count) in tqdm.tqdm(dups):
    count_to_remove = count - 1
    dbc = db.cursor()
    dbc.execute("select blobid from paths where full=? limit ?;", (url, count_to_remove));
    blobs_to_remove = list(dbc)
    for (blob,) in blobs_to_remove:
        dbc.execute("delete from paths where blobid = ?", (blob,));
        os.remove(f"blobs/{blob}")
        db.commit()

dbc = db.cursor()
dbc.execute("select url,count(*) from queue group by url having count(*) > 1;")
dups = list(dbc)

for (url, count) in tqdm.tqdm(dups):
    count_to_remove = count - 1
    dbc.execute("delete from queue where url = ? limit ?", (url,count_to_remove));
db.commit()
