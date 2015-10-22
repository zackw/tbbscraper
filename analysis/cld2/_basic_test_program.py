import sys
import cld2
import textwrap

def dumplang(lang):
    return "{}={} ({:.3f}, {}%)".format(lang.code, lang.name,
                                        lang.score, lang.percent)

def main():
    wrapper = textwrap.TextWrapper(initial_indent="    ",
                                   subsequent_indent="    ")
    for fn in sys.argv[1:]:
        with open(fn) as f:
            result = cld2.detect(f.read(), want_chunks=True)
            sys.stdout.write("{}: {}\n"
                             .format(fn, " / ".join(dumplang(g)
                                                    for g in result.scores)))
            for i, chunk in enumerate(result.chunks):
                sys.stdout.write("  Chunk {}: {}={}\n"
                                 .format(i+1, chunk[0].code, chunk[0].name))
                sys.stdout.write(wrapper.fill(chunk[1][:2000]))
                sys.stdout.write("\n\n")

main()
