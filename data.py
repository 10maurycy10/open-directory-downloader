import mariadb
import urllib
import uuid
import os
import threading
import time

class DB:
    def __init__(self, config):
            self.db = mariadb.connect(
                user=config["db_username"],
                password=config["db_passwd"],
                host=config["db_host"],
                port=config["db_port"],
                database=config["db_dbname"]
            )
            self.max_retry_count = config["max_retry_count"]

    def get_queue_hostnames(self):
        dbc = self.db.cursor()
        dbc.execute("select hostname from queue group by hostname;")
        return [x for (x,) in dbc]
    
    def get_items(self, hostname, maxcount):
        dbc = self.db.cursor()
        dbc.execute("select url,starthost,retry_count from queue where hostname=? limit ?;", (hostname,maxcount));
        return [full for full in dbc]
    
    def get_dled(self):
        dbc = self.db.cursor()
        dbc.execute("select hostname,count(*) from paths group by hostname;");
        return [full for full in dbc]


    def remove_queue(self, url):
        dbc = self.db.cursor()
        dbc.execute("delete from queue where url=?", (url,));

    def queue_items(self):
        """
        Generator that iterates over the batches of items queue
        """
        while True:
            batch = []
            hostnames = self.get_queue_hostnames()
            for hostname in hostnames:
                urls = self.get_items(hostname, 10)
                batch = batch + urls
            self.commit()
            yield batch
            for url in batch:
                self.remove_queue(url[0])
            if len(hostnames) == 0:
                time.sleep(5)

    def insert_queue(self, url, starthost, retry_count):
        if retry_count > self.max_retry_count:
            return
        parsed = urllib.parse.urlparse(url)
        hostname = parsed.hostname
        dbc = self.db.cursor()
        dbc.execute("insert into queue (starthost,hostname,url,retry_count) values (?,?,?,?);", (starthost,hostname,url,retry_count));

    def unsuported(self, url):
        dbc = self.db.cursor();
        dbc.execute("insert into unsupported (full) values (?);", (url,));
    
    def removefile(self, full):
        """
        Undoes the write file method
        """
        blobid = str(uuid.uuid4())
        dbc = self.db.cursor()
        parsed = urllib.parse.urlparse(full)
        
        dbc.execute("select blobid from paths where full=?", (full,))
        blobids = [l for (l) in dbc]
        dbc.execute("delete from paths paths where full=?", (full,))
        for blobid in blobids:
            os.remove(os.path.join("blobs/", blobid))

    def writefile(self, scheme, hostname, full):
        """
        Returns a open file for the content
        """
        blobid = str(uuid.uuid4())
        dbc = self.db.cursor()
        parsed = urllib.parse.urlparse(full)
        dbc.execute("insert into paths (proto, hostname, blobid, full, filepath) values (?,?,?,?,?);", (scheme, hostname, blobid, full, parsed.path))
        if not os.path.exists("blobs"):
            os.mkdir("blobs")
        return open(os.path.join("blobs/", blobid), "wb")

    
    def indb(self, url):
        """
        checks in a url has been downloaded.
        """
        dbc = self.db.cursor()
        parsed = urllib.parse.urlparse(url)
        dbc.execute("select blobid from paths where proto=? and hostname=? and filepath=?", (parsed.scheme, parsed.hostname, parsed.path))
        for (blobid,) in dbc:
            print(blobid)
            return True
        return False


    def inqueue(self, url):
        """
        checks if a url is in the queue, may have false negatives
        """
        dbc = self.db.cursor()
        dbc.execute("select url from queue where url=?", (url,))
        for (full,) in dbc:
            return True
        return False

    def get_queue_len(self, host):
        dbc = self.db.cursor()
        dbc.execute("select count(*) from queue where hostname=?", (host,))
        for (l,) in dbc:
            return l

    def get_downloads_for_site(self, host):
        dbc = self.db.cursor()
        dbc.execute("select full,blobid from paths where hostname=?", (host,))
        return [full for full in dbc]

    def delete(self, url):
        dbc = self.db.cursor()
        dbc.execute("select blobid from paths where full=?", (url,));
        blobids = list(dbc)
        for blobid in blobids:
            dbc.execute("delete from paths where full=?", (url,))
            os.remove(os.path.join("blobs/", blobid))

    def commit(self):
        self.db.commit()
