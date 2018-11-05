#! /usr/bin/python

import csv
import glob
import requests
import sys

already_reported_pages = set()

def process_one_batch(lang, result, writer):
    global already_reported_pages
    for page in result["pages"].values():
        title = page["title"]
        # The way the result continuation works, it is normal to get
        # pages with a pageid but no extlinks array.  These will be
        # dumped in a different continuation block.  Pages that
        # don't exist at all have a "missing" key and no pageid.
        if "missing" in page:
            if (lang, title) not in already_reported_pages:
                already_reported_pages.add((lang, title))
                sys.stderr.write("{}: no page {}\n".format(lang, title))
            continue
        if "pageid" not in page:
            if (lang, title) not in already_reported_pages:
                already_reported_pages.add((lang, title))
                sys.stderr.write("{}: bogus page {}: {!r}\n".format(lang, title, page))
        for link in page.get("extlinks", []):
            # For no documented reason, this isn't just a list of URLs,
            # it's a list of objects with the invariant format { "*" : url }.
            writer.writerow((title, link["*"]))

def process_one_language(session, lang, titles, writer):
    endpoint = "https://{}.wikipedia.org/w/api.php".format(lang)
    base_params = {
        "action":      "query",
        "prop":        "extlinks",
        "format":      "json",
        "ellimit":     "500",
        "elexpandurl": "",
        "redirects":   "",
    }

    # It is not documented anywhere, but there is an upper limit of 50 titles
    # at one time.

    for i in range(0, len(titles), 50):
        params = base_params.copy()
        params.update({ "titles": "|".join(titles[i:(i+50)]) })
        lastContinue = { "continue": "" }
        while True:
            sys.stderr.write("{}.{} @ {!r}...\n".format(lang, i, lastContinue))
            req = params.copy()
            req.update(lastContinue)
            result = session.get(endpoint, params=req).json()
            if 'error' in result:
                raise RuntimeError(result['error'])
            if 'warnings' in result:
                sys.stderr.write(str(result['warnings'])+"\n")
            if 'query' in result:
                process_one_batch(lang, result['query'], writer)
            if 'continue' not in result:
                break
            lastContinue = result['continue']

def main():
    session = requests.Session()
    session.headers.update({
        "User-Agent": "wp.controversial.outbounds.retriever/1.0"
    })

    if len(sys.argv) == 1:
        fnames = sorted(glob.glob("top100_*_wiki.txt"))
    else:
        fnames = sys.argv[1:]

    for fname in fnames:
        lang   = fname[7:-9]
        rfname = "outbounds_{}.csv".format(lang)
        with open(fname) as inf:
            titles = [t.strip() for t in inf.readlines()]
        with open(rfname, "w") as outf:
            writer = csv.writer(outf, "unix", quoting=csv.QUOTE_MINIMAL)
            writer.writerow(("page","link"))
            process_one_language(session, lang, titles, writer)

main()
