################################################################
##                    Multiple City-Rscript                    #
################################################################
# first load all the libraries needed.
# options(warn = -1)
# options(message = -1)

library(lubridate)
library(tidyverse)
library(EpiNow2)
library(rstan)
library(EpiEstim)
library(ggplot2)
library(gridExtra)
library(incidence)
library(magrittr)
library(readr) # for read_csv
library(knitr) # for kable
library(readxl)
library(googlesheets4)

# to make the code acceptable for multiple
# cites, we need to keep the name of the df constant
# and then keep a placeholder for that specific city.

# then run the code where x will be replaced by the name of the city.
# column names need to be used for the code, but need to be kept the same always.
# have used the col names from the google sheet.

# to check the code I have created a toy dataset where the two cities are mumbai and pune.
# I have randomly assigned the two cities into another column - city.
# this code can be used to calculate rt and dt for each city separately

# need to provide value for x and then run the code without any changes.
# we can provide a path to the folder with the city name to make sure that the
# results are saved in that folder.

# import data from the google sheet...
# no authorisation needed...

gs4_deauth()
sheets_url <- "https://docs.google.com/spreadsheets/d/1HeTZKEXtSYFDNKmVEcRmF573k2ZraDb6DzgCOSXI0f0/edit#gid=0"

df <- read_sheet(sheets_url, sheet = "city_stats")

print(df)

city <- "Mumbai"
df2 <- df %>% filter(district == city)
# todo v2 --> city <- args[1]
# todo v2 --> command line args or simply read from results of city criteria filters for RT and all districts for DT

# ensure that there is no missing data for the columns here.
# we need only 5 columns for the calculation -
df2 <- df2 %>% dplyr::select(date, district, total.confirmed, total.deceased, total.recovered) # keep only col needed.

df2 <- df2[complete.cases(df2), ] # remove rows with NA to allow for calculation.

df2$delta_case <- df2$total.confirmed - df2$total.deceased - df2$total.recovered

date <- as_date(df2$date)

# column names have changed. also active column is not present in the present table. so, using the columns present...
delta_case <- df2$total.confirmed - df2$total.deceased - df2$total.recovered

date <- as_date(df2$date)

confirm <- df2$delta_case

# the colnames need to be exactly this for the epinow function to work.
df3 <- tibble(date, confirm)

# now the 2 columns needed for the Rt calculations are decided.
# to make the Rt calculation for that cityname
# need to get the generation_time and then the incubation_period.

# get the generation and incubation time from the new EpiNow2 package.
generation_time <- get_generation_time(disease = "SARS-CoV-2", source = "ganyani")

incubation_period <- get_incubation_period(disease = "SARS-CoV-2", source = "lauer")

# model parameters as default
# note that parameters about generation_time,
# incubation_period, and reporting_delay are set as default in the package.
reporting_delay <- EpiNow2::bootstrapped_dist_fit(rlnorm(100, log(6), 1))

## Set max allowed delay to 30 days to truncate computation
reporting_delay$max <- 30

# values for generation time and incubation period have been defined now.
# the code below is for v 1.3.0 package.
# set credible interval as 0.95
rt <-
  EpiNow2::epinow(
    reported_cases = df3,
    generation_time = generation_time,
    delays = delay_opts(incubation_period, reporting_delay),
    rt = rt_opts(prior = list(mean = 2, sd = 0.2)),
    stan = stan_opts(cores = 4, samples = 100),
    verbose = TRUE,
    CrIs = 0.95
  )

# get the summary estimates with the credible intervals.
rt <- summary(rt, type = "parameters", params = "R")

# dummy RT df
# date <- c('2021-01-15', '2021-01-16','2021-01-17','2021-01-18','2021-01-19')
# variable <- c('R','R','R','R','R')
# strat <- c(NA,NA,NA,NA,NA)
# type <- c('forecast','forecast','forecast','forecast','forecast')
# median <- c(.959,.959,.959,.959,.959)
# mean <- c(.968,.968,.968,.968,.968)
# sd <- c(.131,.131,.131,.131,.131 )
# lower_95 <- c(.76,.76,.76,.76,.76)
# upper_95 <- c(1.14,1.14,1.14,1.14,1.14)
# rt <- data.frame(date, variable, strat, type, median, mean, sd, lower_95, upper_95)

# ============================
# Doubling time

# doubling time function does not depend upon any package.
# so that can be used it is.
# paste the doubling time function here first...
compute_doubling_time <- function(total_cases, case_dates, time.gap, alpha = 0.05) {
  suppressMessages(require(dplyr))

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


  return(data.frame(
    date = as.Date(end.time, origin = "1970-01-01"), r = r, r_ci_low = r + qnorm(alpha / 2) * sd_r, r_ci_up = r + qnorm(1 - alpha / 2) * sd_r,
    dt = dt, dt_ci_low = dt + qnorm(alpha / 2) * sd_dt, dt_ci_up = dt + qnorm(1 - alpha / 2) * sd_dt
  ))
}


total_cases <- df3$confirm
cases_dates <- df3$date

db <- compute_doubling_time(total_cases, cases_dates, time.gap = 7, alpha = 0.95)

# right now using the same code provided by @krishna for saving both rt and doubling_time results.
print("write to local FS")
write.csv(rt, "/usr/data/epinow2_out.csv")
write.csv(db, "/usr/data/doubling_time.csv")