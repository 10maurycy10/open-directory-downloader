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
def statdl(args):
    """
    Shows information on *running* downloads.
    """
    hosts =  db.get_queue_hostnames();
    lens = [str(db.get_queue_len(host)) for host in hosts]
    print_table(["Hostname", "Queue len"],list(zip(hosts, lens)))

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
    urls = db.get_downloads_for_site(args.hostname)
    print(f"Packing {len(urls)} files")
    with zipfile.ZipFile(args.output, 'w',  compression=zipfile.ZIP_DEFLATED) as outzip:
        for (url,blobid) in tqdm.tqdm(urls):
            path = urllib.parse.urlparse(url).path
            with outzip.open(path, "w") as inzip:
                content = open(os.path.join("blobs/", blobid), "rb").read()
                inzip.write(content)

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

