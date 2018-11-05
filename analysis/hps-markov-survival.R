#! /usr/bin/Rscript

suppressPackageStartupMessages({
    library(ggplot2)
    library(msm)
})

rd <- read.csv("hps-msm-20160201.csv", header=TRUE)

# arbitrarily prune simultaneous observations
rd <- rd[c(TRUE, diff(rd$years)!=0), ]

# prune URLs with only one observation
rl <- rle(rd$url)
rl$values <- rl$lengths > 1
rd <- rd[inverse.rle(rl), ]

# proper ordering of status
rd$status <- ordered(rd$status,
                     levels=c("live", "parked", "http error", "dns error"))
# gobsmackingly perverse restriction on the state variable
rd$n.status <- as.numeric(rd$status)

# all transitions are possible; there is no absorbing state
iq <- 1 - diag(length(levels(rd$status)))

# basic MSM model
m1 <- msm(n.status ~ years, subject=url, data=rd, qmatrix=iq, gen.inits=TRUE)


          covariates=~group,
          control=list(trace=1,REPORT=1)
          )
