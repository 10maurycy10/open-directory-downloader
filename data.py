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
            self.lock = threading.Lock()

    def get_queue_hostnames(self):
        self.lock.acquire()
        dbc = self.db.cursor()
        dbc.execute("select hostname from queue group by hostname;")
        self.lock.release()
        return [x for (x,) in dbc]
    
    def get_items(self, hostname, maxcount):
        self.lock.acquire()
        dbc = self.db.cursor()
        dbc.execute("select url,starthost,retry_count from queue where hostname=? limit ?;", (hostname,maxcount));
        self.lock.release()
        return [full for full in dbc]

    def remove_queue(self, url):
        self.lock.acquire()
        dbc = self.db.cursor()
        dbc.execute("delete from queue where url=?", (url,));
        self.lock.release()

    def queue_items(self):
        """
        Generator that iterates over the batches of items queue
        """
        while True:
            batch = []
            hostnames = self.get_queue_hostnames()
            for hostname in hostnames:
                urls = self.get_items(hostname, 10)
                for url in urls:
                    self.remove_queue(url[0])
                batch = batch + urls
            yield batch
            if len(hostnames) == 0:
                time.sleep(5)
            self.commit()

    def insert_queue(self, url, starthost, retry_count):
        self.lock.acquire()
        if retry_count > self.max_retry_count:
            self.lock.release()
            return
        parsed = urllib.parse.urlparse(url)
        hostname = parsed.hostname
        dbc = self.db.cursor()
        dbc.execute("insert into queue (starthost,hostname,url,retry_count) values (?,?,?,?);", (starthost,hostname,url,retry_count));
        self.lock.release()

    def unsuported(self, url):
        self.lock.acquire()
        dbc = self.db.cursor();
        dbc.execute("insert into unsupported (full) values (?);", (url,));
        self.lock.release()
    
    def writefile(self, scheme, hostname, full):
        """
        Returns a open file for the content
        """
        self.lock.acquire()
        blobid = str(uuid.uuid4())
        dbc = self.db.cursor()
        parsed = urllib.parse.urlparse(full)
        dbc.execute("insert into paths (proto, hostname, blobid, full, filepath) values (?,?,?,?,?);", (scheme, hostname, blobid, full, parsed.path))
        if not os.path.exists("blobs"):
            os.mkdir("blobs")
        self.lock.release()
        return open(os.path.join("blobs/", blobid), "wb")

    def indb(self, url):
        self.lock.acquire()
        """
        checks in a url has been downloaded.
        """
        dbc = self.db.cursor()
        parsed = urllib.parse.urlparse(url)
        dbc.execute("select blobid from paths where proto=? and hostname=? and filepath=?", (parsed.scheme, parsed.hostname, parsed.path))
        for (blobid,) in dbc:
            self.lock.release()
            return True
        self.lock.release()
        return False


    def inqueue(self, url):
        """
        checks if a url is in the queue, may have false negatives
        """
        self.lock.acquire()
        dbc = self.db.cursor()
        dbc.execute("select url from queue where url=?", (url,))
        for (full,) in dbc:
            self.lock.release()
            return True
        self.lock.release()
        return False

    def get_queue_len(self, host):
        self.lock.acquire()
        dbc = self.db.cursor()
        dbc.execute("select count(*) from queue where hostname=?", (host,))
        for (l,) in dbc:
            self.lock.release()
            return l
        self.lock.release()

    def get_downloads_for_site(self, host):
        self.lock.acquire()
        dbc = self.db.cursor()
        dbc.execute("select full,blobid from paths where hostname=?", (host,))
        self.lock.release()
        return [full for full in dbc]

    def delete(self, url):
        self.lock.acquire()
        dbc = self.db.cursor()
        dbc.execute("select blobid from paths where full=?", (url,));
        blobid = list(dbc)[0][0]
        dbc.execute("delete from paths where full=?", (url,))
        os.remove(os.path.join("blobs/", blobid))
        self.lock.release()

    def commit(self):
        self.lock.acquire()
        self.db.commit()
        self.lock.release()
