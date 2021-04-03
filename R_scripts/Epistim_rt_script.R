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
df <- read.csv(file = '/usr/data/citystats.csv')
# TODO --> temporary, mumbai identifier --> replaced by critical cities 
#city <- "Mumbai"

df2 <- df %>% dplyr::select(date, district, total.confirmed, total.deceased, total.recovered)
# for each city: 
city_list = unique(df[["some_col"]])
city_num = length(city_list)
city_output = vector("list", city_num)

index = 1
for (city in city_list) {

    # filter for city
    df_city = df2 %>% filter(district == city)
    # remove rows with NA to allow for calculation.
    df_city <- df_city[complete.cases(df_city), ]
    # delta_case
    df_city$delta_case <- df_city$total.confirmed - df_city$total.deceased - df_city$total.recovered
    # ensure date format
    df_city$date <- as_date(df_city$date)
    # tibble; Dates and delta case
    df_city <- tibble(dates = df_city$date, I = df_city$delta_case)
    # default RT arguments --> non-parametric 
    config <- make_config(list(mean_si = 3.96, std_mean_si = 0.215,
                            min_mean_si = 3.53, max_mean_si = 4.39,
                            std_si = 4.75, std_std_si = 0.145,
                            min_std_si = 4.46, max_std_si = 5.07,
                            n1 = 468, n2 = 468,mean_prior=2.6,
                            std_prior=2))
    # calc RT
    rt_nonparametric <- estimate_R(df_city,
                                  method = "uncertain_si",
                                  config = config)

    res <- rt_nonparametric$R
    dates <- rt_nonparametric$dates
    n <- length(df_city$dates)

    dates_list <- dates[8:n]

    res_df <- tibble(mean = rt_nonparametric$R$`Mean(R)`,
                    upper = rt_nonparametric$R$`Mean(R)` - 1.96*rt_nonparametric$R$`Std(R)`,
                    lower = rt_nonparametric$R$`Mean(R)` + 1.96*rt_nonparametric$R$`Std(R)`,
                    date = dates_list,
                    city = city, 
                    median = rt_nonparametric$R$`Median(R)`)
        
    # append df
    city_output[[index]] <- res_df
    print("res_df")
    print(res_df)
    index <- index + 1

        }

# concatenate result list of dataframes
out_df <- bind_rows(city_output, .id = "column_label")
print("out_df")
print(out_df)

# now res_df contains the rt, low and high ci and dates.
write.csv(out_df, "/usr/data/epiestim_out.csv", row.names=FALSE)

