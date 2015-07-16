import sys
import cld2

def dumplang(lang):
    return "{}={} ({:.3f}, {}%)".format(lang.code, lang.name,
                                        lang.score, lang.percent)

def main():
    for fn in sys.argv[1:]:
        with open(fn) as f:
            guesses = cld2.detect(f.read())
            sys.stdout.write("{}: {}\n"
                             .format(fn, " / ".join(dumplang(g)
                                                    for g in guesses)))

main()
