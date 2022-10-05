#!/bin/python3
import data
import json
import download
import urllib
import auditlog

config = json.loads(open("config.json").read())
db = data.DB(config)

def job(url, starthost, retry_count, db):
    if not db.indb(url):
        auditlog.log(f"started\t{url}")
        newlinks = download.dispatch(url, starthost, db, config, retry_count)
        if newlinks:
            for link in newlinks:
                absolute = urllib.parse.urljoin(url, link)
                parsed = urllib.parse.urlparse(absolute)
                if parsed.hostname:
                    if parsed.hostname.startswith(starthost) and not db.indb(absolute) and not db.inqueue(absolute):
                        auditlog.log(f"inserting\t{absolute}")
                        db.insert_queue(absolute, starthost, 0)
                    else:
                        auditlog.log(f"dropping limited\t{absolute}")
                else:
                    auditlog.log(f"dropping unsuported\t{url}")
                    db.unsuported(url)
        else:
            auditlog.log(f"no links for\t{url}")
    else:
        auditlog.log(f"dropping indb\t{url}")

import threading

for items in db.queue_items():
    # Sharing the db handle caused issues, even with the introduction of a lock
    # As a fix, open a new db connection for the thread
    threads = [threading.Thread(target=job, args=(url, sh, rc, data.DB(config))) for (url,sh,rc) in items]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
