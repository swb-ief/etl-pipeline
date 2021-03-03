############################
##  Doubling time script  ##
############################

# script to calculate the doubling time only.
# 01/18/2021
# script does not depend upon the city/district name.
# script contains a dummy var == group.

# first load all the libraries needed.

library("lubridate"))
library("tidyverse"))
library("EpiNow2"))
library("rstan"))
library(EpiEstim))
library("gridExtra")
library(incidence)
library(magrittr)
library(readr) # for read_csv
library(knitr) # for kable

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

# TODO --> cmd line arg Rscript name.R --args file_name 
df <- read.csv(file = '/usr/data/citystats.csv')

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

df3$group <- sample(0:1, length(df3$confirm), replace = T)

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

# this works, now to run the doubling time for each group separately.

df3 <- df3 %>%
    rename(total_cases = confirm, cases_dates = date)

a <- df3 %>% group_by(group)

a2 <- group_split(a)

str(a2)

d1 <- a2[1]

total_cases <- d1[[1]]$total_cases

cases_dates <- d1[[1]]$cases_dates

db1 <- compute_doubling_time(total_cases,
    cases_dates,
    time.gap = 7, alpha = 0.95
)

db1$group <- d1[[1]]$group[1] # insert the value for group placeholder.

d2 <- a2[2]

total_cases <- d2[[1]]$total_cases

cases_dates <- d2[[1]]$cases_dates

db2 <- compute_doubling_time(total_cases, cases_dates,
    time.gap = 7, alpha = 0.95
)

db2$group <- d2[[1]]$group[1] # insert the value for the group placeholder.

comp_db_time <- rbind(db1, db2)

comp_db_time # now this dataframe contains the doubling time for both groups 0/1.

# going to test this code with group as character rather than number.
# change that after using sample code in df3.
# pick up after inserting that col in df3.
# run after line 83.


df3$group <- factor(df3$group,
    levels = c(0, 1),
    labels = c("Pune", "Nagpur")
)


df3 %>% count(group)

# ran the rest of the code and it works.
# creates a final dataaset comp_db_time that contains the grouping variable
# as a column in the results dataset.
# this can then be exported for graphing as needed.


# maybe a better way would be to prepare a function
# that can run the doubling time within it.

calc_db <- function(df) {
    a <- df %>% group_by(group)

    a2 <- group_split(a)

    str(a2)




    d1 <- a2[1]

    total_cases <- d1[[1]]$total_cases

    cases_dates <- d1[[1]]$cases_dates

    db1 <- compute_doubling_time(total_cases,
        cases_dates,
        time.gap = 7, alpha = 0.95
    )

    db1$group <- d1[[1]]$group[1] # insert the value for group placeholder.

    d2 <- a2[2]

    total_cases <- d2[[1]]$total_cases

    cases_dates <- d2[[1]]$cases_dates

    db2 <- compute_doubling_time(total_cases, cases_dates,
        time.gap = 7, alpha = 0.95
    )

    db2$group <- d2[[1]]$group[1] # insert the value for the group placeholder.

    comp_db_time <- rbind(db1, db2)

    return(data.frame(comp_db_time))
    # now this dataframe contains the doubling time for both groups 0/1.
}

db_res <- calc_db(df3)

db_res <- data.frame(db_res)