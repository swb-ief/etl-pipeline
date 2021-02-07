########################################
##  Rt Calculations:EpiEstim package  ##
########################################

# rt calculations using EpiEstim package.

# library(easypackages)
library(lubridate)
library(tidyverse)
library(EpiEstim)
library(ggplot2)
library(gridExtra)
library(incidence)
library(magrittr)
library(readr)
library(knitr)
library(readxl)
library(googlesheets4)

# libraries("lubridate","tidyverse","EpiEstim","ggplot2",
#         "gridExtra","incidence","magrittr",
#         "readr","knitr","readxl","googlesheets4")


# get the data --
gs4_deauth()
sheets_url <- "https://docs.google.com/spreadsheets/d/1HeTZKEXtSYFDNKmVEcRmF573k2ZraDb6DzgCOSXI0f0/edit#gid=0"

df <- read_sheet(sheets_url, sheet = "city_stats")

city <- "Mumbai"

df2 <- df %>% filter(district == city)

# glimpse(df2)

# todo v2 --> city <- args[1]
# todo v2 --> command line args or simply read from results of city criteria filters for RT and all districts for DT

# ensure that there is no missing data for the columns here.
# we need only 5 columns for the calculation -
df2 <- df2 %>% dplyr::select(date, district, total.confirmed, total.deceased, total.recovered) # keep only col needed.

df2 <- df2[complete.cases(df2), ] # remove rows with NA to allow for calculation.

df2$delta_case <- df2$total.confirmed - df2$total.deceased - df2$total.recovered

df2$date <- as_date(df2$date)

print(df2)

df3 = df2 %>% rename(I = delta_case, dates = date) %>%
  dplyr::select(dates,I)


# now to calculate the Rt using the parametric method 
# with mean and sd of the serial interval.
# as per the publication mean = 3.96
# sd = 4.75


# rt_parametric <- estimate_R(df3, 
#                   method="parametric_si",
#                   config = make_config(list(
#                   mean_si = 2.6, 
#                   std_si = 1.5)))
# 
# rt_parametric
# 
# rt1 <- tibble(rt = rt_parametric$R$`Mean(R)`,
# low = rt_parametric$R$`Mean(R)` - 1.96*rt_parametric$R$`Std(R)`,
# high = rt_parametric$R$`Mean(R)` + 1.96*rt_parametric$R$`Std(R)`,
# id = 1:nrow(rt1))
# 
# plot(rt_parametric)



config = make_config(list(mean_si = 3.96, std_mean_si = 0.215,
                          min_mean_si = 3.53, max_mean_si = 4.39,
                          std_si = 4.75, std_std_si = 0.145,
                          min_std_si = 4.46, max_std_si = 5.07,
                          n1 = 468, n2 = 468,mean_prior=2.6,
                          std_prior=2))
                     

rt_nonparametric = estimate_R(df3,
                   method = "uncertain_si",
                   config = config)


# plot(rt_nonparametric)
# 
# 
# res <- rt_nonparametric$R
# 
# dates <- rt_nonparametric$dates
# 
# glimpse(res)
# 
# length(dates)


n <- length(df3$dates)

dates_list <- dates[8:n]

res_df <- tibble(rt = rt_nonparametric$R$`Mean(R)`,
                 low = rt_nonparametric$R$`Mean(R)` - 1.96*rt_nonparametric$R$`Std(R)`,
                 high = rt_nonparametric$R$`Mean(R)` + 1.96*rt_nonparametric$R$`Std(R)`,
                 dates = dates_list,
                 city = city)


#str(res_df)


# plot(res_df$dates, res_df$rt, type = "l", col = "red")
# lines(res_df$dates, res_df$low, lty = 2, col = "blue")
# lines(res_df$dates, res_df$high, lty = 2, col = "blue")


# now res_df contains the rt, low and high ci and dates.

write.csv(res_df, "/usr/data/epiestim_out.csv")

