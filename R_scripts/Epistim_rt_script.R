########################################
##  Rt Calculations:EpiEstim package  ##
########################################

# rt calculations using EpiEstim package.
library(lubridate)
library(tidyverse)
library(EpiEstim)
library(gridExtra)
library(incidence)
library(magrittr)
library(readr)
library(knitr)

# # TODO --> cmd line arg Rscript name.R --args file_name 
df <- read.csv(file = '/usr/data/citystats.csv')
# filter
df2 <- df2 %>% dplyr::select(date, district, total.confirmed, total.deceased, total.recovered)
# remove rows with NA to allow for calculation.
df2 <- df2[complete.cases(df2), ]
# delta_case
df2$delta_case <- df2$total.confirmed - df2$total.deceased - df2$total.recovered
# ensure date format
df2$date <- as_date(df2$date)
# tibble; Dates and delta case
df3 = tibble(dates = df2$date, I = df2$delta_case)
# default RT arguments --> non-parametric 
config = make_config(list(mean_si = 3.96, std_mean_si = 0.215,
                          min_mean_si = 3.53, max_mean_si = 4.39,
                          std_si = 4.75, std_std_si = 0.145,
                          min_std_si = 4.46, max_std_si = 5.07,
                          n1 = 468, n2 = 468,mean_prior=2.6,
                          std_prior=2))
# calc RT
rt_nonparametric = estimate_R(df3,
                   method = "uncertain_si",
                   config = config)

res <- rt_nonparametric$R
dates <- rt_nonparametric$dates
n <- length(df3$dates)

dates_list <- dates[8:n]

res_df <- tibble(mean = rt_nonparametric$R$`Mean(R)`,
                 upper = rt_nonparametric$R$`Mean(R)` - 1.96*rt_nonparametric$R$`Std(R)`,
                 lower = rt_nonparametric$R$`Mean(R)` + 1.96*rt_nonparametric$R$`Std(R)`,
                 date = dates_list,
                 city = city, 
                 median = rt_nonparametric$R$`Median(R)`)

# now res_df contains the rt, low and high ci and dates.
# TODO --> add this path as cmd line arg
write.csv(res_df, "/usr/data/epiestim_out.csv")

