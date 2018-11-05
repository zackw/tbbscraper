#! /usr/bin/python3

# Sample the Bluesmote data set.  We are only interested in URLs
# tagged "policy_denied".  A URL's probability of selection is
# *inversely* proportional to the number of times it appears in the
# data set, weakly penalized by its depth in the site; also, there is
# a hard upper limit of 25 URLs selected per domain name.

import collections
import random
import re
import sys
import urllib.parse

# Code for random sampling without replacement taken from
# http://stackoverflow.com/questions/2140787/select-k-random-elements-from-a-list-whose-elements-have-weights/2149533#2149533


class Node:
    # Each node in the heap has a weight, value, and total weight.
    # The total weight, self.tw, is self.w plus the weight of any children.
    __slots__ = ['w', 'v', 'tw']
    def __init__(self, w, v, tw):
        self.w, self.v, self.tw = w, v, tw

def rws_heap(items):
    # h is the heap. It's like a binary tree that lives in an array.
    # It has a Node for each pair in `items`. h[1] is the root. Each
    # other Node h[i] has a parent at h[i>>1]. Each node has up to 2
    # children, h[i<<1] and h[(i<<1)+1].  To get this nice simple
    # arithmetic, we have to leave h[0] vacant.
    h = [None]                          # leave h[0] vacant
    for w, v in items:
        h.append(Node(w, v, w))
    for i in range(len(h) - 1, 1, -1):  # total up the tws
        h[i>>1].tw += h[i].tw           # add h[i]'s total to its parent
    return h

def rws_heap_pop(h, rng):
    gas = h[1].tw * rng.random()     # start with a random amount of gas

    i = 1                     # start driving at the root
    while gas > h[i].w:       # while we have enough gas to get past node i:
        gas -= h[i].w         #   drive past node i
        i <<= 1               #   move to first child
        if gas > h[i].tw:     #   if we have enough gas:
            gas -= h[i].tw    #     drive past first child and descendants
            i += 1            #     move to second child
    w = h[i].w                # out of gas! h[i] is the selected node.
    v = h[i].v

    h[i].w = 0                # make sure this node isn't chosen again
    while i:                  # fix up total weights
        h[i].tw -= w
        i >>= 1
    return v

def random_weighted_sample_no_replacement(items, rng):
    heap = rws_heap(items)              # just make a heap...
    try:
        for i in range(len(heap) - 1):
            yield rws_heap_pop(heap, rng)   # and pop items off it until
                                            # caller gets bored or we run out.
    except IndexError:
        return

# End code for random sampling without replacement

def read_items(fp):

    # File extensions that will definitely not provide us with any
    # useful information if we try to scan them:
    resource_exts = re.compile(
        r"\.(?:jpe?g|png|gif|svg|ico|exe|js|css|flv|rm|gz|gzip|zip|swf|rar"
            r"|xpi|wwzip|mp3|mp4|wmv|apk|jar|deb|bmp|torrent|ogg|pdf)$",
        re.I
    )

    while True:
        try:
            line = fp.readline()
        except UnicodeDecodeError:
            continue
        if not line:
            break

        try:
            count, tag, url = line.split()
        except ValueError:
            continue
        if tag != "policy_denied" or not url.startswith("http"):
            continue
        if resource_exts.search(url):
            continue

        #count = int(count)
        #depth = url.count('/') - 3
        #weight = (1/count) * 0.707 ** depth

        yield (1, url)

def main():
    # we want this many pages
    npages = 100000
    nsel   = 0
    # no more than this many pages per host
    max_per_host = 25
    # fixed seed, originally pulled from /dev/urandom
    rng = random.Random(0xdd5901db8fd2ce5f)

    selected = collections.defaultdict(set)
    for url in random_weighted_sample_no_replacement(
            read_items(sys.stdin), rng):

        host = urllib.parse.urlsplit(url).hostname
        selected_thishost = selected[host]
        if len(selected_thishost) < max_per_host and \
           url not in selected_thishost:
            selected_thishost.add(url)
            nsel += 1

        if nsel >= npages:
            break

    urls = []
    for h in selected.values():
        for u in h:
            urls.append(u)
    urls.sort()
    for url in urls: print(url)

main()
