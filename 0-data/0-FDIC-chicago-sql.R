#######
# FDIC Chicago Call Reports Bulk Data 
# https://www.chicagofed.org/banking/
#  financial-institution-reports/commercial-bank-data
# http://bit.ly/2rVkzjS

library("DBI")
library("haven")
library("RSQLite")
library("tidyverse")

local_dir   <- "0-data/FDIC/calls_chicago"
data_source <- paste0(local_dir, "/raw")
if (!file.exists(local_dir)) dir.create(local_dir, recursive = T)
if (!file.exists(data_source)) dir.create(data_source)

fdic_db <- dbConnect(SQLite(), "0-data/FDIC/fdic.sqlite")

fdic_files <- dir(data_source, full.names = T)

# ---- read ---------------------------------------------------------------

# Assets - total assets RCFD2170
# Total Loans and Leases: 1976-83.12 RCFD2122 + RCFD2165
#    84.3 RCFD1400
# Loans secured real estate - RCFD1410
# Ag loans - RCFD1590
# Total deposits RCFD2200
# Interest income 84.3 RIAD4107
# Interest and fees - 76 RIAD4020

demo <- c(DATE = "DATE", RSSD9999 = "DATE", # problem of this missing in 1997
          CALL8786 = "reporting_level",
          RSSD9050 = "cert", RSSD9001 = "fed_rssd" , RSSD9002 = "top_rssd",
          RSSD9063 = "rssd_predecessor", RSSD9064 = "rssd_successor",
          RSSD9068 = "rssd_parent", RSSD9348 = "rssd_id_reg_1",
          RSSD9351 = "rssd_id_reg_2", RSSD9355 = "rssd_id_reg_3",
          RSSD9358 = "rssd_id_reg_4", RSSD9375 = "rssd_head",
          RSSD9379 = "rssd_first", RSSD9382 = "rssd_second",
          RSSD9386 = "rssd_third", RSSD9389 = "rssd_fourth",
          RSSD9010 = "name",
          RSSD9130 = "city", RSSD9200 = "stalp", RSSD9220 = "zip",
          RSSD9150 = "cty_fips", RSSD9210 = "st_fips",
          RSSD9220 = "fips", RSSD4087 = "inst.webaddr",
          RSSD9170 = "fed_district_code", RSSD9950 = "estymd")
bank <- c(RCFD2170 = "assets", RCFD2948 = "liabilities", RCFD3210 = "equity",
          RIAD4000 = "op_income", RIAD4130 = "op_expense",
          RIAD4135 = "salaries", RIAD4340 = "net_income",
          RCFD2122 = "loans_pre841",
          RCFD2165 = "loans_pre842", RCFD1400 = "loans",
          RCFD1410 = "loans_real_estate", RCFD2200 = "total_deposits",
          RIAD4107 = "interest_income", RIAD4010 = "interest_fees_income",
          RIAD4008 = "interest_fees_real_estate1",
          RIAD4011 = "interest_fees_real_estate2",
          RIAD4246 = "interest_fees_income_real_estate")

# Data Dictionary: https://www.federalreserve.gov/apps/mdrm/data-dictionary
# Search for "agricult" or "farm"

farm <- c(RCON1230 = "ag_d_30", RCON1231 = "ag_d_90",
          RCON1232 = "ag_d_nonaccrual", RCON1233 = "ag_d_renegotiated",
          RCON1583 = "farm_d_nonaccrual", RCFD1583 = "farm_d_nonaccrual_alt",
          RCON1584 = "farm_d_renegotiated", RCFD1584 = "farm_d_renegotiated_alt",
          RCON1590 = "farm_loans", RCFD1590 = "farm_loans_alt",
          RCON1594 = "farm_d_30", RCFD1594 = "farm_d_30_alt",
          RCON1597 = "farm_d_90", RCFD1597 = "farm_d_90_alt",
          RCON1613 = "farm_restructured", RCFD1613 = "farm_restructured_alt",
          RCON3379 = "ag_loans_avg",
          RCON3386 = "farm_loans_quarterly_avg",
          RCON4024 = "ag_interest_fee_income", RIAD4268 = "ag_charge_offs",
          RIAD4269 = "ag_recoveries", RIAD4665 = "farm_recoveries",
          RIAD4655 = "farm_charge_offs",
          RCON5577 = "farm_loans_number", RCON6860 = "small_farms_loans",
          RCON1420 = "farmland_loans",
          RCON3493 = "farmland_d_30_alt",
          RCON3494 = "farmland_d_90_alt",
          RCON3495 = "farmland_d_nonaccrual_alt",
          RIAD3584 = "farmland_charge_offs_alt",
          RIAD3585 = "farmland_recoveries_alt",
          RCON5427 = "farmland_d_30",
          RCON5428 = "farmland_d_90", RCON5429 = "farmland_d_nonaccrual",
          RIAD5447 = "farmland_charge_offs", RIAD5448 = "farmland_recoveries",
          RCON5509 = "farmland_owned", RCON5576 = "farmland_loans_number")


# Really need to extract the relevant variables before saving them in
#  the map command...
vars <- c(demo, bank, farm)

j5 <- map(fdic_files, function(x){
  print(x)
  # td <- tempdir()
  # unzip(x, exdir = td)
  # 
  # x_file <- dir(td, pattern = ".xpt", ignore.case = T, full.names = T)
  # 
  # dat <- read_xpt(x_file)
  x_file <- unzip(x, list = T)
  x_file_name <- x_file$Name[grep(".xpt", x_file$Name, ignore.case = T)]
  
  dat        <- read_xpt(unz(description = x,
                             filename = x_file_name))
  names(dat) <- vars[names(dat)]
  dat        <- dat[, !is.na(names(dat))]
  
  # unlink(td, recursive = T)
  return(dat)
})

# 01   Boston
# 02   New York
# 03   Philadelphia
# 04   Cleveland
# 05   Richmond
# 06   Atlanta
# 07   Chicago
# 08   St. Louis
# 09   Minneapolis
# 10   Kansas City
# 11   Dallas
# 12   San Francisco
# 13   Washington, D.C.
fed_cross <- c("01" = "Boston", "02" = "New York", "03" = "Philadelphia",
               "04" = "Cleveland", "05" = "Richmond", "06" = "Atlanta",
               "07" = "Chicago", "08" = "St. Louis", "09" = "Minneapolis",
               "10" = "Kansas City", "11" = "Dallas", "12" = "San Francisco",
               "13" = "Washington, D.C.")

# CALL8786 reporting level
# 0 = NA
# 1 = Fully consolidated entity including foreign office(s)
# 2 = Domestic office consolidation
# 3 = Foreign office consolidation
# 4 = Head office only
# 5 = Branch or combination of branches or offices
call_cross <- c("0" = "NA",
                "1" = "Fully consolidated entity including foreign",
                "2" = "Domestic office consolidation",
                "3" = "Foreign office consolidation",
                "4" = "Head office only",
                "5" = "Branch or combination of branches or offices")

j6 <- j5 %>% 
  bind_rows() %>% 
  mutate(date = as.Date(as.character(DATE), "%Y%m%d"),
         established = as.Date(as.character(estymd), "%Y%m%d"),
         fips = 1000*st_fips + cty_fips,
         fed_district_code = str_pad(fed_district_code, 2, "left", "0"),
         fed_district_name = fed_cross[fed_district_code],
         call_name = call_cross[as.character(reporting_level)])

# Go in and adjust for the call report data being based in $1,000s
loan_vars <- c("loans", "loans_real_estate", "loans_pre841", "loans_pre842",
               "assets", "total_deposits", "liabilities", "equity", "ag_d_30",
               "ag_d_90", "ag_d_nonaccrual", "farmland_loans",
               "farm_d_nonaccrual", "farm_loans", "farm_d_30", "farm_d_90",
               "farm_restructured", "ag_loans_avg", "farm_loans_quarterly_avg",
               "farmland_d_30", "farmland_d_90",
               "farmland_d_nonaccrual", "farmland_owned",
               "farmland_d_30_alt", "farmland_d_90_alt",
               "farmland_d_nonaccrual_alt",
               "farm_d_nonaccrual_alt", "farm_loans_alt", "farm_d_30_alt",
               "farm_d_90_alt", "farm_restructured_alt",
               "farmland_charge_offs_alt", "farm_recoveries",
               "ag_recoveries", "farmland_recoveries_alt",
               # "farmland_loans_number", "farm_loans_number",
               # "small_farms_loans",
               "op_income", "interest_fees_income",
               "interest_fees_real_estate2", "interest_income", "op_expense",
               "salaries", "interest_fees_income_real_estate", "ag_charge_offs",
               "net_income", "farm_charge_offs", "farmland_charge_offs",
               "farmland_recoveries", "ag_d_renegotiated",
               "farm_d_renegotiated", "interest_fees_real_estate1")

j6 <- mutate_at(j6, vars(loan_vars), function(x) 1000*x)

# Going to make my correction to the farmland loan value problem in 1979-03-31
hey <- j6 %>% 
  filter(date > "1978-12-30", date < "1979-07-01") %>% 
  arrange(fed_rssd, date) %>% 
  group_by(fed_rssd) %>% 
  select(date, fed_rssd, farmland_loans) %>% 
  mutate(n = n(),
         temp = if_else(is.na(farmland_loans), 0, farmland_loans)) %>% 
  filter(n > 1)

hey <- hey %>% 
  group_by(fed_rssd) %>% 
  mutate(farmland_loans_alt = case_when(is.na(lag(temp)) ~ temp,
                                        temp > 20*lag(temp) ~ lag(temp),
                                        T ~ temp)) %>% 
  filter(date == "1979-03-31") %>% 
  select(date, fed_rssd, farmland_loans_alt)

j6 <- j6 %>% 
  left_join(hey) %>% 
  mutate(farmland_loans_alt = if_else(is.na(farmland_loans_alt),
                                      farmland_loans, farmland_loans_alt))

# Correct for the negative values of deposits, which are not possible:
j6 <- mutate(j6, total_deposits = if_else(total_deposits < 0,
                                          0, total_deposits))

j7 <- mutate(j6, farm_d_30 = if_else(date == "1983-09-30", 0, farm_d_30),
             farm_d_30_alt = if_else(date == "1983-09-30", 0, farm_d_30_alt),
             ag_d_90 = if_else(date == "1983-09-30",
                               ag_d_90 / 10000, ag_d_90),
             farm_d_90 = if_else(date == "1983-09-30",
                                 farm_d_90 / 10000, farm_d_90),
             farm_d_90_alt = if_else(date == "1983-09-30",
                                     farm_d_90_alt / 10000, farm_d_90_alt),
             ag_d_nonaccrual = if_else(date == "1983-09-30",
                                       ag_d_nonaccrual / 10000, ag_d_nonaccrual),
             farm_d_nonaccrual = if_else(date == "1983-09-30",
                                         farm_d_nonaccrual / 10000, farm_d_nonaccrual),
             farm_d_nonaccrual_alt = if_else(date == "1983-09-30",
                                             farm_d_nonaccrual_alt / 10000,
                                             farm_d_nonaccrual_alt)) %>% 
  select(-DATE)


dbWriteTable(fdic_db, "CALLS_chicago_fed", j7, overwrite = T)
