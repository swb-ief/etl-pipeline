# implement EpiEstim code for our data.
# implement this on our data.


library(easypackages)
libraries('tidyverse','EpiEstim','googlesheets4','lubridate')


gs4_deauth()
sheets_url <- "https://docs.google.com/spreadsheets/d/1HeTZKEXtSYFDNKmVEcRmF573k2ZraDb6DzgCOSXI0f0/edit#gid=0"

df_u <- read_sheet(sheets_url, sheet = "city_stats")

# keep only without NA to calculate the I.

city <- "Mumbai"
df_u2 <- df_u %>% filter(district == city)


df2 <- df_u2

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

df = df3 %>% rename(active = confirm)


epid.count = as.numeric(unlist(c(df['active'])))

window = 7

res.R = estimate_R(epid.count,method = "uncertain_si",
      config = make_config(list(t_start=2:(length(epid.count)-window),t_end=(2+window):length(epid.count),
                          mean_si = 3.96, std_mean_si = 0.215,
                          min_mean_si = 3.53, max_mean_si = 4.39,
                          std_si = 4.75, std_std_si = 0.145,
                          min_std_si = 4.46, max_std_si = 5.07,
                          n1 = 468, n2 = 468,mean_prior=2.6,
                          std_prior=2)))


# returns res.R which contains all the data for graphing the R(t).

