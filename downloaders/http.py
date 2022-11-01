import requests
import urllib
from bs4 import BeautifulSoup
import auditlog

TAGS = {
    "a": ['href'],
    "frame": ["src"],
    "iframe": ["src"],
    "img": ["src"],
    "script": ["src"],
    "link": ['href']
}

def extract_urls(body):
    soup = BeautifulSoup(body, 'html.parser')
    links = []
    for tag in TAGS.keys():
        for link in soup.findAll(tag):
            for attrib in TAGS[tag]:
               if link.get(attrib):
                   links.append(link.get(attrib))
    return links

def get_tor_session():
    session = requests.session()
    # Tor uses the 9050 port as the default socks port
    proxy = "socks5h://127.0.0.1:1080"
    session.proxies = {'http':  proxy,'https': proxy}
    return session
                                   

def download(url, starthost, db, config, retry_count, log):
    """
    Returns the links found in the file
    """
    try:
        headers = {
            'useragent': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36"
        }
        log(f"Making request\t{url}")
        parsed = urllib.parse.urlparse(url)
        tor = get_tor_session()
        req = tor.get(url, timeout=60, headers=headers, stream=True, verify=False);

        print(req)
        print(req.status_code)

        if (req.status_code == 200):
            # If content type is passed, dont extract links if the document is not html.
            attempt_link_extraction = True
            if req.headers.get('content-type'):
                attempt_link_extraction = "html" in req.headers['content-type']
            link_extraction_raw = []

            # Create db entry and get file handle
            handle = db.writefile(parsed.scheme, parsed.hostname, url)

            # Steam download, saving part of the file to memeory for link_extraction
            log(f"Started download\t{url}")
            counter = 0
            for chunk in req.iter_content(chunk_size=1024):
                if config["max_size"] < counter:
                    log(f"Aborting {url} due to size constriants")
                    handle.close()
                    db.delete(url)
                    return None
                if attempt_link_extraction and len(link_extraction_raw) < 64:
                    link_extraction_raw.append(chunk)
                handle.write(chunk)
                counter = counter + 1
                print(f"{counter} KB {url}")
            handle.close()

            if attempt_link_extraction:
                body = b"".join(link_extraction_raw)
                return extract_urls(body)
            else:
                return None
        else:
            print("Attempting headers link extraction")
            header_links = [x["url"] for x in req.links.values()]
            print(header_links)
            return header_links
    except requests.exceptions.ConnectionError as e:
        log(f"Retrying {url}")
        db.delete(url)
        db.insert_queue(url, starthost, retry_count + 1)
    except requests.exceptions.ReadTimeout as e:
        log(f"Retrying {url}")
        db.delete(url)
        db.insert_queue(url, starthost, retry_count + 1)
    except requests.exceptions.TooManyRedirects as e:
        db.delete(url)
        print(e)
        # This error is unlikly to resolve itself on a retry,
        

