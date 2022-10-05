import urllib
import downloaders.http
import auditlog

downloaders = {
    'http': downloaders.http.download,
    'https': downloaders.http.download
}

def dispatch(url,starthost,db, config, retry_count):
    parsed = urllib.parse.urlparse(url)
    scheme = parsed.scheme
    downloader = downloaders.get(scheme)
    if downloader:
        auditlog.log(f"dispaching\t{url}")
        links = downloader(url, starthost, db, config, retry_count or 0)
        db.commit()
        return links
    else:
        auditlog.log(f"unsupporing scheme\t{url}")
        db.unsuported(url)
        db.commit()
    
