#!/bin/python3
import data
import json
import argparse
import urllib
import sys
import tqdm

def print_table(headers, data):
    """
    Utility function to print a nice table
    """
    # Compute column widths
    width = [len(header) for header in headers];
    for row in data:
        assert(len(headers) == len(row)) # All the rows in data should be the same width as the headers
        for (i, point) in enumerate(row):
            width[i] = max(width[i], len(point))
    # Row seperator
    seperator = '+' + "+".join(["-"*(colwidth+2) for colwidth in width]) + "+"
    # Format and print header
    line = []
    for (colidx, col) in enumerate(headers):
        line.append(col.ljust(width[colidx]))
    print(seperator)
    print('| ' + " | ".join(line) + " |")
    print(seperator)
    # Format and print data
    for row in data:
        line = []
        for (colidx, col) in enumerate(row):
            line.append(col.ljust(width[colidx]))
        print("| " + " | ".join(line) + " |");
        print(seperator)


config = json.loads(open("config.json").read())
db = data.DB(config)

# Setup argparse
parser = argparse.ArgumentParser()
parser.add_argument('-c', '--config', help="Config file to use", default='config.json')
subparsers = parser.add_subparsers(dest="subcommand")
subparsers.required = True

# Decorator for argparse, this would be nice to have in stdlib
def subcommand(args=[], parent=subparsers):
    def decorator(func):
        parser = parent.add_parser(func.__name__, description=func.__doc__)
        for arg in args:
            parser.add_argument(*arg[0], **arg[1])
        parser.set_defaults(func=func)
    return decorator
def argument(*name_or_flags, **kwargs):
    return ([*name_or_flags], kwargs)

@subcommand([
    argument("url", help="Url of open directorieys")
])
def add(args):
    url = args.url
    parsed = urllib.parse.urlparse(url)
    db.insert_queue(url, parsed.hostname, 0)
    db.commit()

@subcommand()
def addmul(args):
    import urllib
    import tqdm
    for url in tqdm.tqdm(sys.stdin.readlines()):
        url = url.rstrip()
        parsed = urllib.parse.urlparse(url)
        db.insert_queue(url, parsed.hostname, 0)
    db.commit()

@subcommand()
def statdl(args):
    """
    Shows information on *running* downloads.
    """
    hosts =  db.get_queue_hostnames();
    lens = [str(db.get_queue_len(host)) for host in hosts]
    print_table(["Hostname", "Queue len"],list(zip(hosts, lens)))

@subcommand()
def statstore(ars):
    """
    Shows listing of downloaded data
    """
    print_table(["Hostname", "Filecount"],[(str(h), str(n)) for (h,n) in db.get_dled()])

@subcommand([
    argument("hostname", help="Hostname to generate zip for")
])
def mklist(args):
    """
    Creates a listing of files.
    """
    urls = db.get_downloads_for_site(args.hostname)
    for (url,blob) in urls:
        print(f"{blob}\t{url}")

@subcommand([
    argument("hostname", help="Hostname to generate zip for"),
    argument("-o", "--output", help="Location of zip file", required=True),
])
def mkzip(args):
    """
    Creates a zip file after download is compleated
    """
    import zipfile
    import os
    print("Getting downloaded paths...")
    urls = db.get_downloads_for_site(args.hostname)
    
    print("Parsing urls...")
    paths = [(urllib.parse.urlparse(url).path, blobid).path for (url, blobid) in urls]
    paths = [(urllib.parse.unquote(url), blobid) for (url, blobid) in urls]

    
    print("Reconstructing directory structure...")
    # Find files with subfiles, that must be saved as directories
    dirs = []
    filenames = [path.removesuffix("/") for (path, blob) in paths]
    filenames = list(set(filenames))
    filenames.sort()
    for i in range(len(filenames) - 1):
        if filenames[i+1].startswith(filenames[i]):
            dirs.append(filenames[i])

    print(f"Packing {len(urls)} files")
    
    writen = set()
    
    with zipfile.ZipFile(args.output, 'w') as outzip: # compression=zipfile.ZIP_DEFLATED) as outzip:
        for (path, blobid) in tqdm.tqdm(paths):
            if not path in writen:
                writen.add(path)
                if path in dirs:
                    path = path + "/index"
                with outzip.open(path, "w", force_zip64=True) as inzip:
                    content = open(os.path.join(db.blobpath, blobid), "rb").read()
                    inzip.write(content)

@subcommand([
    argument("hostname", help="Hostname to generate zip for"),
])
def check(args):
    """
    Verifyes if all files downloaded from a site can be opened, if any are missing, it will print the url and the correct filename in the blobdir, allowing manual fixing.

    for some reason during a long dowload some, ~10, files went missing, and this was causing falures zipping, this allowed me to find and fix all of them quicly.
    """
    import os
    urls = db.get_downloads_for_site(args.hostname)
    for (url,blobid) in tqdm.tqdm(urls):
        try:
            with open(os.path.join(db.blobpath, blobid), "rb") as file:
                pass
        except FileNotFoundError:
            print(blobid, url)


@subcommand([
    argument("url", help="URL to delete"),
])
def delete(args):
    """
    Removes an URL from the db
    """
    db.delete(str(args.url))
    db.commit()

@subcommand([
    argument("hostname", help="Hostname to purge"),
])
def purge(args):
    """
    Deletes all downloads from a site
    """
    urls = db.get_downloads_for_site(args.hostname)
    for (url, bolbid) in tqdm.tqdm(urls):
        db.delete(url)
    db.commit()



if __name__ == "__main__":
    args = parser.parse_args()
    if args.subcommand is None:
        parser.print_help()
    else:
        args.func(args)

