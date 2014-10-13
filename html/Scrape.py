#! /usr/bin/python

import re
import urllib3
from bs4 import BeautifulSoup


def _visible(element):
    if element.parent.name in ['style', 'script', '[document]', 'head', 'title']:
        return False
    elif re.match('<!--.*-->', str(element.encode('utf-8','replace'))):
        return False
    return True

def scrape(url):
#url = 'http://blog.cellbreaker.com/can-switch-carriers-old-phone/'
    http = urllib3.PoolManager()
    r = http.request('GET',url)
    html = r.data.decode('utf-8')

    soup = BeautifulSoup(html)
    texts = soup.findAll(text=True)

    visible_list = filter(_visible,texts)
    visible_texts = re.sub('[^0-9a-zA-Z]+', ' ',', '.join(visible_list))
    return visible_texts


if __name__ == '__main__':
    #sample = 'http://www.google.com'
    sample = 'http://blog.cellbreaker.com/can-switch-carriers-old-phone/'
    print(scrape(sample))
