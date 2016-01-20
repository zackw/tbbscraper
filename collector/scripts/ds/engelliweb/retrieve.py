#! /usr/bin/python3

import requests
import time
import csv
import sys
import html.parser

# Expected contents of a row are something like:
# ["<a href=\"http:\/\/engelliweb.com\/url\/zzinsider-com\">zzinsider.com<\/a>",
#  "22.10.2014",
#  "Telekom\u00fcnikasyon \u0130leti\u015fim Ba\u015fkanl\u0131\u011f\u0131",
#  "490.05.01.2014.-184658"]
# We want to pull the domain name out of the first entry, and reformat the
# second entry as an ISO date (YYYY-MM-DD).

class LinkTextExtractor(html.parser.HTMLParser):
    def __init__(self):
        self.extracted = []
        html.parser.HTMLParser.__init__(self, strict=False, convert_charrefs=True)

    def reset(self):
        html.parser.HTMLParser.reset(self)
        self.extracted.clear()

    def __call__(self, text):
        self.reset()
        self.feed(text)
        self.close()
        return "".join(self.extracted)

    def handle_data(self, data):
        self.extracted.append(data)

extract_text = LinkTextExtractor()

def process_rows(rows, writer):
    for row in rows:
        cells = row['cell']
        cells[0] = extract_text(cells[0])
        cells[1] = "-".join(reversed(cells[1].split(".")))
        writer.writerow(cells)

def main():
    # engelliweb tries not to let you scrape it.
    # all of the following headers must be provided or it doesn't work.
    # (it also horks if you don't say "Accept-Encoding: gzip, deflate"
    # but requests does that for us.)

    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:40.0) Gecko/20100101 Firefox/40.0 Iceweasel/40.0',
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': 'http://engelliweb.com/',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
    }

    # List 0 has everything.
    params = {
        'liste': 0,
        'nd':    int(time.time()),
        'rows':  50,
        'page':  1
    }
    base_url = 'http://engelliweb.com/server.php'

    writer = csv.writer(sys.stdout, dialect='unix', quoting=csv.QUOTE_MINIMAL)
    writer.writerow(("domain", "date", "agency", "caseno"))

    with requests.Session() as s:
        # Retrieve the first page; this will tell us how many there are.
        page1 = s.get(base_url, params=params, headers=headers).json()
        process_rows(page1['rows'], writer)
        for i in range(2, page1['total'] + 1):
            time.sleep(1) # rate limit
            sys.stderr.write("{}\n".format(i))
            params['page'] = i
            page = s.get(base_url, params=params, headers=headers).json()
            process_rows(page['rows'], writer)

main()
