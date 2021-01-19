# maybe can use the foreach function to
# loop from 1 -> x depending upon the # of groups in the data.class(




library(foreach)

foreach(i = 0:1) %do%
    sum(1 + i)