################################################################
##                    Multiple City-Rscript                    #
################################################################

# COVID 19 multiple city pipeline code
# Aim:
# 1. Convert code for V 1.3
# 2. Change code to allow for multiple cities.
# 3. import data from google sheet, calculate rt and doubling time, then upload the result as sheets
# back into the google drive.

# first load all the libraries needed.

options(warn = -1)
options(message = -1)s

suppressMessages(library("lubridate"))
suppressMessages(library("tidyverse"))
suppressMessages(library("EpiNow2"))
suppressMessages(library("rstan"))
suppressMessages(library(EpiEstim))
suppressMessages(library(ggplot2))
suppressMessages(library("gridExtra"))
suppressMessages(library(incidence))
suppressMessages(library(magrittr))
suppressMessages(library(readr)) # for read_csv
suppressMessages(library(knitr)) # for kable
suppressMessages(library(readxl))
suppressMessages(library(googlesheets4))



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

df<- read_sheet(sheets_url,sheet="city_stats")

# decide city or district to calculate for.
# enter the city or district name as x.
x <- 'Mumbai'

# filter to keep data from only city of interest.
# here in the toy dataset have created a col - city with two values - mumbai , pune.
# we can change this small code according to the data-set colname that we are actually using
df2 <- df %>% filter(district == x)

# ensure that there is no missing data for the columns here.
# we need only 5 columns for the calculation - 
df2 = df2 %>% dplyr::select(date, district, total.confirmed, total.deceased, total.recovered) # keep only col needed.

df2 = df2[complete.cases(df2), ] # remove rows with NA to allow for calculation.

df2$delta_case = df2$total.confirmed - df2$total.deceased - df2$total.recovered

date <- as_date(df2$date)

# column names have changed. also active column is not present in the present table. so, using the columns present...
delta_case = df2$total.confirmed - df2$total.deceased - df2$total.recovered

date <- as_date(df2$date)

confirm <- df2$delta_case

# the colnames need to be exactly this for the epinow function to work.
df3 <- tibble(date, confirm) 

# now the 2 columns needed for the Rt calculations are decided.
# to make the Rt calculation for that cityname
# need to get the generation_time and then the incubation_period.

# get the generation and incubation time from the new EpiNow2 package.
generation_time <- 
get_generation_time(disease = "SARS-CoV-2", source = "ganyani")

incubation_period <- 
get_incubation_period(disease = "SARS-CoV-2", source = "lauer")

## model parameters as default##
## note that parameters about generation_time,
# incubation_period, and reporting_delay are set as default in the package.
reporting_delay <- EpiNow2::bootstrapped_dist_fit(rlnorm(100, log(6), 1))

## Set max allowed delay to 30 days to truncate computation
reporting_delay$max <- 30

# values for generation time and incubation period have been defined now.
# the code below is for v 1.3.0 package.
# set credible interval as 0.95 
rt <- 
  EpiNow2::epinow(reported_cases = df3, 
  generation_time = generation_time,
  delays = delay_opts(incubation_period, reporting_delay),
  rt = rt_opts(prior = list(mean = 2, sd = 0.2)), 
  stan = stan_opts(cores = 4, samples = 100),
  verbose = TRUE,
  CrIs = 0.95)

# get the summary estimates with the credible intervals.
rt <- summary(rt, type = "parameters", params = "R")  

# this is the summary estimate for the city specified in the beginning.
# here I have pasted the results back to google-sheets and created a new sheet named 'rt'.
# sheet_write(rt, sheet = string_c(x, "rt", sep = "_"))

# ============================
# Doubling time

# doubling time function does not depend upon any package.
# so that can be used it is. 
# paste the doubling time function here first...
compute_doubling_time <- function(total_cases, case_dates, time.gap, alpha=0.05){
  suppressMessages(require(dplyr))
  
  data_tab = data.frame(date = case_dates, tot_cases = total_cases )
  
  delta_case = data_tab[-1,2] - data_tab[-dim(data_tab)[1],2] 
  
  dat = data_tab
  
  t.gap = time.gap
  

  # is this code block below needed?
  # ======================================
  #	dbl_timr <- function(dat,  t.gap = time.gap) {
  
  #if (is.null(end_date)) {
  #    end_date <- max(dat$date)
  #  }
  
  #t.start <-  dat %>% filter(date == as.Date(as.Date(end_date, origin="1970-01-01") - t.gap)) %>% pull(tot_cases)
  #n = length(data$date)
  
  #t.start = as.Date(data$date[-seq(n-time + 1, n)], origin="1970-01-01")
  #  if (length(t.start) == 0) {
  #    NA
  # } else if (t.start == 0) {
  #    NA
  #  } else {
  #    t.end   <- data %>% filter(date == as.Date(end_date, origin="1970-01-01")) %>% pull(tot_cases)
  #t.end <- as.Date(data$date[-seq(1, time)], origin="1970-01-01")
  # }
  # ======================================
  
  
  end.time   <- dat$date + t.gap
  
  end.time   <- end.time[which(end.time %in% dat$date)]
  
  t.end   <- dat$tot_cases[which(dat$date %in%  end.time)]
  
  start.time <- dat$date[seq(1, length(t.end))]
  
  t.start <- dat$tot_cases[seq(1, length(t.end))]
  
  if(length(t.start) != length(t.end)){
    message("check the date")
    break
  }
  
  
  r <- ((t.end - t.start) / t.start) 
  dt <- time.gap * (log(2) / log(1 + (r)))
  
  r_d <- (dat$tot_cases[-1] - dat$tot_cases[-length(dat$tot_cases)] )/dat$tot_cases[-length(dat$tot_cases)]
  dt_d <-  (log(2) / log(1 + (r_d)))
  
  sd_r <-c()
  sd_dt <-c()
  for(t in 1:(length(t.start)-1)){
    
    sd_r <- c(sd_r, sd(r_d[which(dat$date %in% seq(start.time[t], end.time[t], 1))]))
    sd_dt <- c(sd_dt, sd(dt_d[which(dat$date %in% seq(start.time[t], end.time[t], 1))]))
    
  }
  sd_r <- c(sd_r, sd_r[(length(t.start)-1)])
  sd_dt <- c(sd_dt, sd_dt[(length(t.start)-1)])
  
  
  return(data.frame(date=as.Date(end.time, origin="1970-01-01"),r=r, r_ci_low = r + qnorm(alpha/2)*sd_r, r_ci_up = r + qnorm(1-alpha/2)*sd_r,
                    dt=dt, dt_ci_low = dt + qnorm(alpha/2)*sd_dt, dt_ci_up = dt + qnorm(1-alpha/2)*sd_dt))
  
}


total_cases <- df3$confirm
cases_dates <- df3$date

db <- compute_doubling_time(total_cases, cases_dates, time.gap = 7, alpha = 0.95)

# right now using the same code provided by @krishna for saving both rt and doubling_time results.
write.csv(rt,'/usr/data/epinow2_out.csv')
write.csv(db,'/usr/data/doubling_time.csv')



# ==================================
# ======== OLD VERSION =============
# ==================================

# ##COVID19 mumbai for pipline ##


# options(warn=-1)
# options(message=-1)
# #install.packages("drat")
# #drat:::add("epiforecasts")
# #install.packages("rstan")



# library("EpiNow2")
# library("rstan")
# # library(EpiEstim)
# library(ggplot2)
# library("gridExtra")
# #library(incidence)
# library(magrittr)
# library(readr)  # for read_csv
# library(knitr)  # for kable
# library(dplyr)
# library(googlesheets4)


# #myfile <- "/usr/data/city_stats.csv"
# #suppressMessages(mumbai_new<-read_csv(myfile))
# #kable(head(mumbai))

# #to skip auth 
# gs4_deauth()
# sheets_url <- "https://docs.google.com/spreadsheets/d/1HeTZKEXtSYFDNKmVEcRmF573k2ZraDb6DzgCOSXI0f0/edit#gid=0"
# mumbai_new <- read_sheet(sheets_url,sheet="city_stats")

# mumbai_filtered = mumbai_new[mumbai_new$district == 'Mumbai',]
# #mumbai_filtered <- na.omit(mumbai_filtered) 
# case_series_mumbai<-as.numeric(unlist(mumbai_filtered[,"delta.confirmed"])) ## take out delta case##
# tot_cases_mumbai<-as.numeric(unlist(mumbai_filtered[,"total.confirmed"])) ## take out delta case##
# case_dates_mumbai <- unlist(mumbai_filtered[,"date"])


# #length(case_series_mumbai)

# mumbai_tab <- data.frame(date= as.Date(case_dates_mumbai,  origin = "1970-01-01"), confirm=case_series_mumbai)
# mumbai_tab <- na.omit(mumbai_tab)
# mumbai_tab["confirm"] <- replace(mumbai_tab["confirm"], mumbai_tab["confirm"] < 0, 0)

# #mumbai_tab2 <- mumbai_tab[-1,]
# mumbai_tab3 <- data.frame(date= as.Date(case_dates_mumbai,  origin = "1970-01-01"), tot_cases=tot_cases_mumbai)
# mumbai_tab3 <- na.omit(mumbai_tab3)

# # ##this part is from {incidence}##
# # mumbai_tab2$dates.x <- (case_dates_mumbai[-1] -  case_dates_mumbai[-length(case_dates_mumbai)])/2
# # lm1 <- stats::lm(log(confirm) ~ dates.x, data = mumbai_tab2)

# # r <- stats::coef(lm1)["dates.x"]
# # r.conf <- stats::confint(lm1, "dates.x", 0.95)
# # new.data <- data.frame(dates.x = sort(unique(lm1$model$dates.x)))
# # pred     <- exp(stats::predict(lm1, newdata = new.data, interval = "confidence", level = 0.95))
# # pred <- cbind.data.frame(new.data, pred)
# # info_list <- list(
# #   tab = round(c(r = r,
# #                 r.conf = r.conf,
# #                 doubling = log(2) / r,
# #                 doubling.conf = log(2) / r.conf),4),
# #   pred = pred
# # )
# #info_list


# ##this part is from 
# dbl_timr <- function(data, end_date = NULL, time = 7) {
  
#   if (is.null(end_date)) {
#     end_date <- max(data$date)
#   }
  
#   start <-  data %>% filter(date == as.Date(as.Date(end_date, origin="1970-01-01") - time)) %>% pull(tot_cases)
  
#   if (length(start) == 0) {
#     NA
#   } else if (start == 0) {
#     NA
#   } else {
#     end   <- data %>% filter(date == as.Date(end_date, origin="1970-01-01")) %>% pull(tot_cases)
    
#     r <- ((end - start) / start) * 100
    
#     dt <- time * (log(2) / log(1 + (r / 100)))
#     return(c(r=r, dt=dt))
#   }
# }

# dbl_times <- NA


# tmp_v     <- matrix(NA, ncol=2, nrow=length(case_dates_mumbai))
# for(j in seq_along(case_dates_mumbai)) {
#   task <- dbl_timr(data = mumbai_tab3, end_date = case_dates_mumbai[j], time = 7)
#   if(is.na(task)==T) {
#     tmp_v[j, ] <-c(NA, NA)
#   } else {
#     tmp_v[j, ]  <- task
#   }
  
# }

# colnames(tmp_v) <- c("r", "doubling time")
# dt_mumbai <-data.frame(date=as.Date(case_dates_mumbai, origin="1970-01-01"), tmp_v)
# dt_mumbai <-dt_mumbai[is.na(dt_mumbai[,2])==F, ]

# tab_dt_mumbai <- c(r = mean(dt_mumbai[,2]/100), r_CI = c(mean(dt_mumbai[,2]/100) + qnorm(0.025)*sd(dt_mumbai[,2]/100), mean(dt_mumbai[,2]/100) + qnorm(1-0.025)*sd(dt_mumbai[,2]/100)),
#                    doubling_time = mean(dt_mumbai[,3]), dt_CI = c(mean(dt_mumbai[,3]) + qnorm(0.025)*sd(dt_mumbai[,3]), mean(dt_mumbai[,3]) + qnorm(1-0.025)*sd(dt_mumbai[,3])))

# write.csv(dt_mumbai,'/usr/data/dt_mumbai.csv')

# ##old Rt: EpiEstim##
# t_start <- seq(6, 87 - 6)
# t_end   <- t_start + 6

# #Rt_covid_mumbai <- EpiEstim::estimate_R(incid = case_series_mumbai, method = "parametric_si",
# #                                        config = make_config(list(mean_si = 3.96, std_si = 4.75, si_parametric_distr = "G",
# #                                                                  t_start = t_start, t_end = t_end, seed = 123)))

# #plot(Rt_covid_mumbai) #see the result##

# ##R_sim_CI <- sample_posterior_R(Rt_covid19_mumbai, n = 10000, window=77:81) ##need to fit model moving window##

# ##note that parameters about generation_time, incubation_period, reporting_delay are 
# reporting_delay <- EpiNow2::bootstrapped_dist_fit(rlnorm(100, log(6), 1))
# ## Set max allowed delay to 30 days to truncate computation
# reporting_delay$max <- 30
# generation_time <- list(mean = EpiNow2::covid_generation_times[1, ]$mean,
#                         mean_sd = EpiNow2::covid_generation_times[1, ]$mean_sd,
#                         sd = EpiNow2::covid_generation_times[1, ]$sd,
#                         sd_sd = EpiNow2::covid_generation_times[1, ]$sd_sd,
#                         max = 30)

# incubation_period <- list(mean = EpiNow2::covid_incubation_period[1, ]$mean,
#                           mean_sd = EpiNow2::covid_incubation_period[1, ]$mean_sd,
#                           sd = EpiNow2::covid_incubation_period[1, ]$sd,
#                           sd_sd = EpiNow2::covid_incubation_period[1, ]$sd_sd,
#                           max = 30)
# estimates_mumbai <- EpiNow2::epinow(reported_cases = mumbai_tab, generation_time = generation_time,
#                                     delays = list(incubation_period, reporting_delay), horizon = 7, samples = 1000, 
#                                     warmup = 200, cores = 4, chains = 4, verbose = TRUE, adapt_delta = 0.95)

# ##to see the result##
# ##estimates_mumbai$summary
# ###estimates_mumbai$plot

# #compare result##
# #Rt_Epiestim <- cbind(mumbai_tab[unlist(Rt_covid_mumbai$R[ 2]),1],Rt_covid_mumbai$R[,c (8, 5, 11)])

# Rt_EpiNow2 <- estimates_mumbai$estimates$summarised
# Rt_mean_sd <- Rt_EpiNow2[which(Rt_EpiNow2[,"variable"]=="R") ,c(10,11)]
# Rt_tab = cbind(Rt_EpiNow2[which(Rt_EpiNow2[,"variable"]=="R") ,c(1,4, 9, 7,8)], mean=Rt_mean_sd[,1],
#                CI_lower = Rt_mean_sd[,1]-1.96* Rt_mean_sd[,2],  CI_upper =Rt_mean_sd[,1]+ 1.96* Rt_mean_sd[,2]) 

# Rt_tab
# write.csv(Rt_tab,'/usr/data/epinow2_out.csv')
 
# # Rt_EpiNow2 <- estimates_mumbai$estimates$summarised[which(estimates_mumbai$estimates$summarised[,"variable"]=="R" &
# #                                                            ( (estimates_mumbai$estimates$summarised[,"type"]=="estimate") |
# #                                                              (estimates_mumbai$estimates$summarised[,"type"]=="estimate based on partial data") |
# #                                                              (estimates_mumbai$estimates$summarised[,"type"]=="forecast")))]





# # write.csv(Rt_EpiNow2,'/usr/data/epinow2_out.csv')
