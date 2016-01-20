from collections import defaultdict
import regex as re
import unicodedata

__all__ = ('segment', 'presegment', 'is_nonword', 'is_url')

def get_url_re():
    # https://data.iana.org/TLD/tlds-alpha-by-domain.txt
    # Version 2015122700, Last Updated Sun Dec 27 07:07:01 2015 UTC
    tlds = "|".join(sorted(set("""
    aaa aarp abb abbott abogado ac academy accenture accountant
    accountants aco active actor ad ads adult ae aeg aero af afl ag agency
    ai aig airforce airtel al allfinanz alsace am amica amsterdam
    analytics android ao apartments app apple aq aquarelle ar aramco archi
    army arpa arte as asia associates at attorney au auction audi audio
    author auto autos aw ax axa az azure ba band bank bar barcelona
    barclaycard barclays bargains bauhaus bayern bb bbc bbva bcn bd be
    beats beer bentley berlin best bet bf bg bh bharti bi bible bid bike
    bing bingo bio biz bj black blackfriday bloomberg blue bm bms bmw bn
    bnl bnpparibas bo boats boehringer bom bond boo book boots bosch
    bostik bot boutique br bradesco bridgestone broadway broker brother
    brussels bs bt budapest bugatti build builders business buy buzz bv bw
    by bz bzh ca cab cafe cal call camera camp cancerresearch canon
    capetown capital car caravan cards care career careers cars cartier
    casa cash casino cat catering cba cbn cc cd ceb center ceo cern cf cfa
    cfd cg ch chanel channel chat cheap chloe christmas chrome church ci
    cipriani circle cisco citic city cityeats ck cl claims cleaning click
    clinic clothing cloud club clubmed cm cn co coach codes coffee college
    cologne com commbank community company computer comsec condos
    construction consulting contact contractors cooking cool coop corsica
    country coupons courses cr credit creditcard creditunion cricket crown
    crs cruises csc cu cuisinella cv cw cx cy cymru cyou cz dabur dad
    dance date dating datsun day dclk de dealer deals degree delivery dell
    delta democrat dental dentist desi design dev diamonds diet digital
    direct directory discount dj dk dm dnp do docs dog doha domains doosan
    download drive durban dvag dz earth eat ec edu education ee eg email
    emerck energy engineer engineering enterprises epson equipment er erni
    es esq estate et eu eurovision eus events everbank exchange expert
    exposed express fage fail fairwinds faith family fan fans farm fashion
    fast feedback ferrero fi film final finance financial firestone
    firmdale fish fishing fit fitness fj fk flights florist flowers
    flsmidth fly fm fo foo football ford forex forsale forum foundation
    fox fr frl frogans fund furniture futbol fyi ga gal gallery game
    garden gb gbiz gd gdn ge gea gent genting gf gg ggee gh gi gift gifts
    gives giving gl glass gle global globo gm gmail gmo gmx gn gold
    goldpoint golf goo goog google gop got gov gp gq gr grainger graphics
    gratis green gripe group gs gt gu gucci guge guide guitars guru gw gy
    hamburg hangout haus healthcare help here hermes hiphop hitachi hiv hk
    hm hn hockey holdings holiday homedepot homes honda horse host hosting
    hoteles hotmail house how hr hsbc ht hu hyundai ibm icbc ice icu id ie
    ifm iinet il im immo immobilien in industries infiniti info ing ink
    institute insurance insure int international investments io ipiranga
    iq ir irish is ist istanbul it itau iwc jaguar java jcb je jetzt
    jewelry jlc jll jm jmp jo jobs joburg jot joy jp jprs juegos kaufen
    kddi ke kfh kg kh ki kia kim kinder kitchen kiwi km kn koeln komatsu
    kp kpn kr krd kred kw ky kyoto kz la lacaixa lamborghini lamer
    lancaster land landrover lasalle lat latrobe law lawyer lb lc lds
    lease leclerc legal lexus lgbt li liaison lidl life lifestyle lighting
    like limited limo lincoln linde link live lixil lk loan loans lol
    london lotte lotto love lr ls lt ltd ltda lu lupin luxe luxury lv ly
    ma madrid maif maison man management mango market marketing markets
    marriott mba mc md me med media meet melbourne meme memorial men menu
    meo mg mh miami microsoft mil mini mk ml mm mma mn mo mobi mobily moda
    moe moi mom monash money montblanc mormon mortgage moscow motorcycles
    mov movie movistar mp mq mr ms mt mtn mtpc mtr mu museum mutuelle mv
    mw mx my mz na nadex nagoya name navy nc ne nec net netbank network
    neustar new news nexus nf ng ngo nhk ni nico ninja nissan nl no nokia
    norton nowruz np nr nra nrw ntt nu nyc nz obi office okinawa om omega
    one ong onl online ooo oracle orange org organic origins osaka otsuka
    ovh pa page panerai paris pars partners parts party pe pet pf pg ph
    pharmacy philips photo photography photos physio piaget pics pictet
    pictures pid pin ping pink pizza pk pl place play playstation plumbing
    plus pm pn pohl poker porn post pr praxi press pro prod productions
    prof properties property protection ps pt pub pw py qa qpon quebec
    racing re read realtor realty recipes red redstone redumbrella rehab
    reise reisen reit ren rent rentals repair report republican rest
    restaurant review reviews rexroth rich ricoh rio rip ro rocher rocks
    rodeo room rs rsvp ru ruhr run rw rwe ryukyu sa saarland safe safety
    sakura sale salon samsung sandvik sandvikcoromant sanofi sap sapo sarl
    sas saxo sb sbs sc sca scb schaeffler schmidt scholarships school
    schule schwarz science scor scot sd se seat security seek sener
    services seven sew sex sexy sfr sg sh sharp shell shia shiksha shoes
    show shriram si singles site sj sk ski sky skype sl sm smile sn sncf
    so soccer social software sohu solar solutions sony soy space spiegel
    spreadbetting sr srl st stada star starhub statefarm statoil stc
    stcgroup stockholm storage studio study style su sucks supplies supply
    support surf surgery suzuki sv swatch swiss sx sy sydney symantec
    systems sz tab taipei tatamotors tatar tattoo tax taxi tc tci td team
    tech technology tel telefonica temasek tennis tf tg th thd theater
    theatre tickets tienda tips tires tirol tj tk tl tm tn to today tokyo
    tools top toray toshiba tours town toyota toys tr trade trading
    training travel travelers travelersinsurance trust trv tt tui tushu tv
    tw tz ua ubs ug uk university uno uol us uy uz va vacations vana vc ve
    vegas ventures verisign versicherung vet vg vi viajes video villas vin
    vip virgin vision vista vistaprint viva vlaanderen vn vodka vote
    voting voto voyage vu wales walter wang wanggou watch watches webcam
    weber website wed wedding weir wf whoswho wien wiki williamhill win
    windows wine wme work works world ws wtc wtf xbox xerox xin
    xn--11b4c3d xn--1qqw23a xn--30rr7y xn--3bst00m xn--3ds443g
    xn--3e0b707e xn--3pxu8k xn--42c2d9a xn--45brj9c xn--45q11c xn--4gbrim
    xn--55qw42g xn--55qx5d xn--6frz82g xn--6qq986b3xl xn--80adxhks
    xn--80ao21a xn--80asehdb xn--80aswg xn--90a3ac xn--90ais xn--9dbq2a
    xn--9et52u xn--b4w605ferd xn--c1avg xn--c2br7g xn--cg4bki
    xn--clchc0ea0b2g2a9gcd xn--czr694b xn--czrs0t xn--czru2d xn--d1acj3b
    xn--d1alf xn--eckvdtc9d xn--efvy88h xn--estv75g xn--fhbei
    xn--fiq228c5hs xn--fiq64b xn--fiqs8s xn--fiqz9s xn--fjq720a
    xn--flw351e xn--fpcrj9c3d xn--fzc2c9e2c xn--gecrj9c xn--h2brj9c
    xn--hxt814e xn--i1b6b1a6a2e xn--imr513n xn--io0a7i xn--j1aef xn--j1amh
    xn--j6w193g xn--jlq61u9w7b xn--kcrx77d1x4a xn--kprw13d xn--kpry57d
    xn--kpu716f xn--kput3i xn--l1acc xn--lgbbat1ad8j xn--mgb9awbf
    xn--mgba3a3ejt xn--mgba3a4f16a xn--mgbaam7a8h xn--mgbab2bd
    xn--mgbayh7gpa xn--mgbb9fbpob xn--mgbbh1a71e xn--mgbc0a9azcg
    xn--mgberp4a5d4ar xn--mgbpl2fh xn--mgbt3dhd xn--mgbtx2b xn--mgbx4cd0ab
    xn--mk1bu44c xn--mxtq1m xn--ngbc5azd xn--ngbe9e0a xn--node xn--nqv7f
    xn--nqv7fs00ema xn--nyqy26a xn--o3cw4h xn--ogbpf8fl xn--p1acf xn--p1ai
    xn--pbt977c xn--pgbs0dh xn--pssy2u xn--q9jyb4c xn--qcka1pmc xn--qxam
    xn--rhqv96g xn--s9brj9c xn--ses554g xn--t60b56a xn--tckwe xn--unup4y
    xn--vermgensberater-ctb xn--vermgensberatung-pwb xn--vhquv xn--vuq861b
    xn--wgbh1c xn--wgbl6a xn--xhq521b xn--xkc2al3hye2a xn--xkc2dl3a5ee0h
    xn--y9a3aq xn--yfro4i67o xn--ygbi2ammx xn--zfr164b xperia xxx xyz
    yachts yamaxun yandex ye yodobashi yoga yokohama youtube yt za zara
    zero zip zm zone zuerich zw""".split())))

    # https://gist.github.com/gruber/8891611
    url_re = r"""
    (?xi)
    \b
    (                                   # Capture 1: entire matched URL
      (?:
        https?:                         # URL protocol and colon
        (?:
          /{1,3}                        # 1-3 slashes
          |                             #   or
          [a-z0-9%]                     # Single letter or digit or '%'
                                        # (Trying not to match e.g. "URI::Escape")
        )
        |                               #   or
                                        # looks like domain name followed by a slash:
        [a-z0-9.\-]+[.]
        (?:"""+tlds+r""")
        /
      )
      (?:                               # One or more:
        [^\s()<>{}\[\]]+                # Run of non-space, non-()<>{}[]
        |                               #   or
        \([^\s()]*?\([^\s()]+\)[^\s()]*?\)  # balanced parens, one level deep:
        |                                   #  (…(…)…)
        \([^\s]+?\)                     # balanced parens, non-recursive: (…)
      )+
      (?:                                   # End with:
        \([^\s()]*?\([^\s()]+\)[^\s()]*?\)  # balanced parens, one level deep:
        |                                   # (…(…)…)
        \([^\s]+?\)                     # balanced parens, non-recursive: (…)
        |                                                                       #   or
        [^\s`!()\[\]{};:'".,<>?«»“”‘’]  # not a space or one of these punct chars
      )
      |                         # OR, the following to match naked domains:
      (?:
            (?<!@)      # not preceded by a @, avoid matching foo@_gmail.com_
        [a-z0-9]+
        (?:[.\-][a-z0-9]+)*
        [.]
        (?:"""+tlds+r""")
        \b
        /?
        (?!@)   # not succeeded by a @, avoid matching "foo.na"
      )         # in "foo.na@example.com"
    )
    """
    return re.compile(url_re, re.VERBOSE|re.IGNORECASE)

class Segmenter:
    """Segmenter is a singleton object which does lazy initialization of
       the various external segmenters, some of which are quite
       expensive to start up.  It provides methods to "presegment" and
       "segment" text, and also has utility methods to identify URLs
       and nonwords (nonwords consist entirely of digits and punctuation).
    """

    def __init__(self):
        symbols_s = []
        symbols_t = []
        digits    = []
        white     = []
        for c in range(0x10FFFF):
            x = chr(c)
            cat = unicodedata.category(x)
            if cat[0] in ('P', 'S'): # Punctuation, Symbols
                # These symbol characters may appear inside a word without
                # breaking it in two.  FIXME: Any others?
                split = (x not in ('-', '‐', '\'', '’', '.'))

                # These characters need to be escaped inside a character class.
                if (x in '\\', '[', ']', '-'):
                    x = '\\' + x

                symbols_t.append(x)
                if split:
                    symbols_s.append(x)

            elif cat[0] == 'N':
                digits.append(x)

            # Treat all C0 and C1 controls the same as whitespace.
            # (\t\r\n\v\f are *not* in class Z.)
            elif cat[0] == 'Z' or cat in ('Cc', 'Cf'):
                white.append(x)

        symbols_s = "".join(symbols_s)
        symbols_t = "".join(symbols_t)
        digits    = "".join(digits)
        white     = "".join(white)

        self.white      = re.compile("["  +             white +          "]+")
        self.split      = re.compile("["  + symbols_s + white +          "]+")
        self.nonword    = re.compile("^[" + symbols_t + white + digits + "]+$")
        self.left_trim  = re.compile("^[" + symbols_t + white +          "]+")
        self.right_trim = re.compile("["  + symbols_t + white +          "]+$")

        self.url = None

        self.mecab      = None
        self.dongdu     = None
        self.pythai     = None
        self.s_chinese  = None
        self.s_arabic   = None
        self._lang_segmenters = defaultdict(
            lambda: self._lang_segment_default, {
                'zh':      self._lang_segment_zh,
                'zh-Hant': self._lang_segment_zh,
                'ja':      self._lang_segment_ja,
                'vi':      self._lang_segment_vi,
                'th':      self._lang_segment_th,

                # The Arabic segmenter is trained on the _language_,
                # not the _script_, but should still do acceptably
                # well (better than the generic, anyway) on the other
                # common languages written with that script.
                'ar':      self._lang_segment_ar,
                'fa':      self._lang_segment_ar,
                'ku':      self._lang_segment_ar,
                'ps':      self._lang_segment_ar,
                'ur':      self._lang_segment_ar
            })

    # Public entry points:
    def is_url(self, text):
        """If TEXT contains an URL, return that URL. Otherwise, return None."""
        if self.url is None:
            self.url = get_url_re()
        m = self.url.match(text)
        if m: return m.group(1)
        return None

    def is_nonword(self, text):
        """True if TEXT consists entirely of digits and punctuation."""
        return bool(self.nonword.match(text))

    def presegment(self, text):
        """Perform generic word segmentation on TEXT.  Returns an iterable."""
        return self._lang_segment_default(text)

    def segment(self, lang, text):
        """Perform language-aware word segmentation on TEXT.
           Returns an iterable."""
        return self._lang_segmenters[lang](text)

    # Internal:
    def _presegment_internal(self, text, language_seg):
        """Presegmentation is independent of language.  It first splits on
           (Unicode) whitespace, then detects embedded URLs which are
           passed through unmodified, and then splits again on punctuation
           and trims a slightly larger set of leading and trailing
           punctuation.  Anything that survives that process is fed to the
           language-specific segmenter.
        """
        for word in self.white.split(text):
            u = self.is_url(word)
            if u:
                yield u
            else:
                for w in self.split.split(word):
                    w = self.left_trim.sub("", w)
                    if w:
                        w = self.right_trim.sub("", w)
                        yield from language_seg(
                            unicodedata.normalize('NFKC', w).casefold())

    def _lang_segment_default(self, text):
        """The default behavior is just to do presegmentation."""
        return self._presegment_internal(text, lambda word: (word,))

    # Thai: libthai/pythai
    def _lang_segment_th(self, text):
        if self.pythai is None:
            from . import pythai
            self.pythai = pythai

        return self._presegment_internal(text, self.pythai.split)

    # Japanese: MeCab
    def _lang_segment_ja(self, text):
        if self.mecab is None:
            # '-O wakati' means "put spaces between the words"
            import MeCab
            self.mecab = MeCab.Tagger('-O wakati')

        return self._presegment_internal(
            text, lambda word: self.mecab.parse(word).split())

    # Chinese: SNLP
    def _lang_segment_zh(self, text):
        if self.s_chinese is None:
            from . import stanford
            self.s_chinese = stanford.ChineseSegmenter()
        return self._presegment_internal(
            text, lambda word: self.s_chinese.segment(word))

    # Arabic and related languages: SNLP + heuristics
    def _lang_segment_ar(self, text):
        if self.s_arabic is None:
            from . import stanford
            self.s_arabic = stanford.ArabicSegmenter()
        return self._presegment_internal(
            text, lambda word: self.s_arabic.segment(word))

    # Vietnamese: dongdu
    # In Vietnamese, spaces appear _within_ every multisyllabic word.
    # Also, the segmenter cares about word capitalization.
    # To handle this correctly we must reimplement the presegmentation
    # loop ourselves.
    def _lang_segment_vi(self, text):
        if self.dongdu is None:
            from . import dongdu
            self.dongdu = dongdu.Segmenter()

        run = []
        def flush():
            nonlocal run
            if run:
                # The output of dongdu.segment may need a second
                # round of trimming.
                for w in self.dongdu.segment(" ".join(run)):
                    w = self.left_trim.sub("", w)
                    if w:
                        yield self.right_trim.sub("", w).casefold()
                run = []

        for word in self.white.split(text):
            u = self.is_url(word)
            if u:
                yield from flush()
                yield u
            else:
                for w in self.split.split(word):
                    w = self.left_trim.sub("", w)
                    if w:
                        w = self.right_trim.sub("", w)
                        run.append(unicodedata.normalize('NFKC', w))

        yield from flush()

_segmenter = None
def presegment(text):
    global _segmenter
    if _segmenter is None: _segmenter = Segmenter()
    return _segmenter.presegment(text)

def segment(lang, text):
    global _segmenter
    if _segmenter is None: _segmenter = Segmenter()
    return _segmenter.segment(lang, text)

def presegment_iter(text):
    global _segmenter
    if _segmenter is None: _segmenter = Segmenter()
    return _segmenter.presegment_iter(text)

def is_url(text):
    global _segmenter
    if _segmenter is None: _segmenter = Segmenter()
    return _segmenter.is_url(text)

def is_nonword(text):
    global _segmenter
    if _segmenter is None: _segmenter = Segmenter()
    return _segmenter.is_nonword(text)
