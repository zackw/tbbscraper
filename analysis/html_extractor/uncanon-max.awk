BEGIN {
    maxl=0;
    minl=9999;
}
/^E\(/ {
    ll = length($2) - 3; # minus 3 for the quotation marks and comma
    maxl = (maxl > ll) ? maxl : ll;
    minl = (minl > ll) ? ll   : minl;
}
END {
    print "#define UNCANON_LABEL_MIN " minl;
    print "#define UNCANON_LABEL_MAX " maxl;
}
