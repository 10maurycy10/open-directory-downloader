#!/bin/python3
# TODO: Threading can lead to duplicate downloads

import data
import json
import download
import urllib
import auditlog
from queue import Queue
import threading
import time

config = json.loads(open("config.json").read())
db = data.DB(config)

def job(url, starthost, retry_count, db):
    if not db.indb(url):
        auditlog.log(f"started\t{url}")
        newlinks = download.dispatch(url, starthost, db, config, retry_count, auditlog.log)
        db.commit()
        if newlinks:
            for link in newlinks:
                absolute = urllib.parse.urljoin(url, link)
                parsed = urllib.parse.urlparse(absolute)
                if parsed.hostname:
                    if parsed.hostname.startswith(starthost) and not db.indb(absolute) and not db.inqueue(absolute):
                        if parsed.path.startswith(urllib.parse.urlparse(url).path):
                            auditlog.log(f"inserting\t{absolute}")
                            db.insert_queue(absolute, starthost, 0)
                        else:
                            auditlog.log(f"dropping updir\t{absolute}")
                    else:
                        auditlog.log(f"dropping limited\t{absolute}")
                else:
                    auditlog.log(f"dropping unsuported\t{url}")
                    db.unsuported(url)
        else:
            auditlog.log(f"no links for\t{url}")
        db.commit()
    else:
        auditlog.log(f"dropping indb\t{url}")

queue = Queue(maxsize=1)

def worker(queue, config):
    db = data.DB(config)
    while True:
        (url, shost, retry_count) = queue.get()
        job(url, shost, retry_count,db)

for _ in range(10):
    t = threading.Thread(target=worker, args=(queue, config))
    t.start()

for items in db.queue_items():
    for item in items:
        queue.put(item)
