################################################################
##                    Multiple City-Rscript                    #
################################################################

library(lubridate)
library(tidyverse)
library(ggplot2)
library(gridExtra)
library(incidence)
library(magrittr)
library(readr) # for read_csv
library(knitr) # for kable

# TODO --> cmd line arg Rscript name.R --args file_name 
df <- read.csv(file = '/usr/data/citystats.csv')
# we need only 5 columns for the calculation -
df2 <- df %>% dplyr::select(date, district, total.confirmed, total.deceased, total.recovered)
# remove rows with NA to allow for calculation.
df2 <- df2[complete.cases(df2), ]
# delta_case
df2$delta_case <- df2$total.confirmed - df2$total.deceased - df2$total.recovered
# ensure date format
date <- as_date(df2$date)
# tibble; Dates and delta case
confirm <- df2$delta_case
df3 <- tibble(date, confirm )

# ================
# Doubling time
# doubling time function does not depend upon any package.
# so that can be used it is.
# paste the doubling time function here first...

# TODO --> consult Salil on this implementation compared to 
# implementation in doub_time.r script.

compute_doubling_time <- function(total_cases, case_dates, time.gap, alpha = 0.05) {

  data_tab <- data.frame(date = case_dates, tot_cases = total_cases)

  delta_case <- data_tab[-1, 2] - data_tab[-dim(data_tab)[1], 2]

  dat <- data_tab

  t.gap <- time.gap

  end.time <- dat$date + t.gap
  end.time <- end.time[which(end.time %in% dat$date)]
  t.end <- dat$tot_cases[which(dat$date %in% end.time)]

  start.time <- dat$date[seq(1, length(t.end))]
  t.start <- dat$tot_cases[seq(1, length(t.end))]

  if (length(t.start) != length(t.end)) {
    message("check the date")
    break
  }

  r <- ((t.end - t.start) / t.start)
  dt <- time.gap * (log(2) / log(1 + (r)))

  r_d <- (dat$tot_cases[-1] - dat$tot_cases[-length(dat$tot_cases)]) / dat$tot_cases[-length(dat$tot_cases)]
  dt_d <- (log(2) / log(1 + (r_d)))

  sd_r <- c()
  sd_dt <- c()
  for (t in 1:(length(t.start) - 1)) {
    sd_r <- c(sd_r, sd(r_d[which(dat$date %in% seq(start.time[t], end.time[t], 1))]))
    sd_dt <- c(sd_dt, sd(dt_d[which(dat$date %in% seq(start.time[t], end.time[t], 1))]))
  }
  sd_r <- c(sd_r, sd_r[(length(t.start) - 1)])
  sd_dt <- c(sd_dt, sd_dt[(length(t.start) - 1)])


  return(data.frame(date = as.Date(end.time, origin = "1970-01-01"), 
                    r = r, 
                    r_ci_low = r + qnorm(alpha / 2) * sd_r, 
                    r_ci_up = r + qnorm(1 - alpha / 2) * sd_r,
                    dt = dt, 
                    dt_ci_low = dt + qnorm(alpha / 2) * sd_dt, 
                    dt_ci_up = dt + qnorm(1 - alpha / 2) * sd_dt))
  }

total_cases <- df3$confirm
cases_dates <- df3$date

# compute db time
db <- compute_doubling_time(total_cases, cases_dates, time.gap = 7, alpha = 0.95)
# TODO Default data for Mumbai, will replace with all districts
db["city"] = "Mumbai"

write.csv(db, "/usr/data/doubling_time.csv", row.names=FALSE)
