words="""
accorés,accorés
anbisyon,Ambitions
banbochè,Prodigal
barcelonais,barcelonais
barclays,Barclays
beaucoup,deal
belfleur,Belfleur
borussia,Borussia
casillas,CASILLAS
dorcelus,dorcelus
estinvil,estinvil
giovanny,giovanny
glassesman,glassesman
homologues,homologated
konprann,understand
lewandowski,Lewandowski
malonèt,dishonest
mourinho,mourinho
occelin,occelin
pefomans,performance
pierre-louis,pierre-louis
prestigieuse,prestigieuse
professionnel,professionally
retrouve,return
rossonerri,rossonerri
sanksyon,sanctions
shaarawy,Shaarawy
suspendu,Hanging
tabloïd,Tabloid
"""

ht = [s.partition(',')[0] for s in words.split()]
en = [s.partition(',')[2] for s in words.split()]

import sys
import os
import json
from urllib.request import urlopen, Request
from urllib.parse import urlencode
from urllib.error import HTTPError

with open(os.path.join(os.environ["HOME"], ".google-api-key"), "rt") as f:
    API_KEY = f.read().strip()

TRANSLATE_URL = \
    "https://www.googleapis.com/language/translate/v2"

def do_POST(url, postdata):
    req = Request(url, method='POST',
                  data=urlencode(postdata).encode('utf-8'),
                  headers={
                      'Content-Type':
                          'application/x-www-form-urlencoded;charset=utf-8',
                      'X-HTTP-Method-Override': 'GET'
                  })
    while True:
        try:
            return json.loads(urlopen(req).read().decode('utf-8'))
        except HTTPError as e:
            if (e.code == 403 and e.reason == "Forbidden" and
                json.loads(e.read().decode("utf-8"))["error"]["message"]
                == "User Rate Limit Exceeded"):
                sys.stdout.write('.')
                sys.stdout.flush()
                time.sleep(15)
                continue
        sys.stderr.write("{} {}\n".format(e.code, e.reason))
        sys.stderr.write(repr(e.read()))
        sys.stderr.write("\n")
        raise SystemExit(1)

def get_translations(source, target, words):
    params = [('key', API_KEY), ('source', source), ('target', target),
              ('q', '\n'.join(words))]
    blob = do_POST(TRANSLATE_URL, params)
    return list(zip(words,
                    blob["data"]["translations"][0]["translatedText"].split()))

def main():
    en_t = get_translations('ht', 'en', ht)
    ok = True
    for x, y in zip(en_t, en):
        if x != y:
            print(x, y)
            ok = False

    if not ok: sys.exit(1)

main()
