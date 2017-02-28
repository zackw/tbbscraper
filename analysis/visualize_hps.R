#! /usr/bin/Rscript

suppressPackageStartupMessages({
    library(ggplot2)
    library(plyr)
})

d <- read.csv(commandArgs(trailingOnly=TRUE)[1], header=TRUE)

# prune the levels of "what" down to the most interesting 10 and force
# an ordering on them
d$change <- ordered("other failure",
                    levels=c(
                        "original",
                        "topic change",
                        "host not found",
                        "page not found",
                        "domain parked",
                        "domain unparked",
                        "timeout",
                        "forbidden",
                        "TLS handshake failure",
                        "other failure"))

d$change[d$what=="original"]                 <- "original"
d$change[d$what=="topic change"]             <- "topic change"
d$change[d$what=="host not found"]           <- "host not found"
d$change[d$what=="page not found (404/410)"] <- "page not found"
d$change[d$what=="domain parked"]            <- "domain parked"
d$change[d$what=="domain unparked"]          <- "domain unparked"
d$change[d$what=="timeout"]                  <- "timeout"
d$change[d$what=="forbidden (403)"]          <- "forbidden"
d$change[d$what=="TLS handshake failed"]     <- "TLS handshake failure"

dd <- ddply(d, .(day, change), function (b) {
    data.frame(n=sum(b$n))
})

maxday  = max(dd$day)
ybreaks = seq(from=0, to=maxday, by=365.2425)
ylabels = as.character(seq_along(ybreaks)-1)

cairo_pdf("hps_viz.pdf", width=10, height=7.5)
print(
    ggplot(dd, aes(x=day, y=n, group=change, fill=change)) + geom_area() +
    scale_fill_brewer(type="qual", palette="Set3") +
    ylab("Number of pages") +
    scale_x_continuous(name="Years since entered onto a watchlist",
                       breaks=ybreaks, labels=ylabels)
)
invisible(dev.off())
