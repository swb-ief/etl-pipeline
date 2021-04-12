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
library(dplyr)

# # TODO --> cmd line arg Rscript name.R --args file_name 
df <- read.csv(file = '/usr/data/wardstats.csv')
print("--- df")
print(head(df))

df2 <- df %>% dplyr::select(date, ward, total.confirmed, total.deceased, total.recovered)
# for each city: 
ward_list = unique(df$ward)
print("ward list")
print(ward_list)
ward_num = length(ward_list)
ward_output = vector("list", ward_num)

index = 1
for (ward1 in ward_list) {

    # filter for city
    df_ward <- df2 %>% filter(ward == ward1)
    print("--- df ward")
    print(ward1)
    print(head(df_ward))
    # remove rows with NA to allow for calculation.
    df_ward <- df_ward[complete.cases(df_ward), ]
    # delta_case
    df_ward$delta_case <- df_ward$total.confirmed - df_ward$total.deceased - df_ward$total.recovered
    # ensure date format
    df_ward$date <- as_date(df_ward$date)
    # tibble; Dates and delta case
    df_ward <- tibble(dates = df_ward$date, I = df_ward$delta_case)
    print(ward1)
    print(head(df_ward))
    # default RT arguments --> non-parametric 
    config <- make_config(list(mean_si = 3.96, std_mean_si = 0.215,
                            min_mean_si = 3.53, max_mean_si = 4.39,
                            std_si = 4.75, std_std_si = 0.145,
                            min_std_si = 4.46, max_std_si = 5.07,
                            n1 = 468, n2 = 468,mean_prior=2.6,
                            std_prior=2))
    # calc RT
    rt_nonparametric <- estimate_R(df_ward,
                                  method = "uncertain_si",
                                  config = config)

    res <- rt_nonparametric$R
    dates <- rt_nonparametric$dates
    n <- length(df_ward$dates)

    dates_list <- dates[8:n]

    res_df <- tibble(mean = rt_nonparametric$R$`Mean(R)`,
                    lower = rt_nonparametric$R$`Mean(R)` - 1.96*rt_nonparametric$R$`Std(R)`,
                    upper = rt_nonparametric$R$`Mean(R)` + 1.96*rt_nonparametric$R$`Std(R)`,
                    date = dates_list,
                    ward = ward1, 
                    median = rt_nonparametric$R$`Median(R)`)
        
    # append df
    ward_output[[index]] <- res_df
    print("res_df")
    print(res_df)
    index <- index + 1

        }

# concatenate result list of dataframes
out_df <- bind_rows(ward_output, .id = "column_label")
print("out_df")
print(out_df)

# now res_df contains the rt, low and high ci and dates.
write.csv(out_df, "/usr/data/epiestim_out_wards.csv", row.names=FALSE)
