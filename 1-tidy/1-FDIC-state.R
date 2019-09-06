#######
# State level loan delinquencies
#
library("DBI")
library("lubridate")
library("RSQLite")
library("stringr")
library("tidyverse")

# # Row-wise summation of variables, with the NA removed
# sum_r <- function(...) rowSums(cbind(...), na.rm = T)
# sumn  <- function(x) sum(x, na.rm = T)
# meann <- function(x) mean(x, na.rm = T)

local_dir   <- "1-tidy/state"
if (!file.exists(local_dir)) dir.create(local_dir, recursive = T)

fdic <- dbConnect(SQLite(), "0-data/FDIC/fdic.sqlite")

# First join the institutional data with the branches, then gather up the call
#  report data needed to merge with the institution.

# Keep all the SQL executable commands before the collect() to speed up
#  the process

# ---- sod ----------------------------------------------------------------

# Variable of "year" will match up with the June 30th survey date
#  also implies that the quarter and month are going to be the same throughout

districts_main <- tbl(fdic, "district_counties") %>% 
  rename_all(list(~paste0(., "_main")))

institution <- tbl(fdic, "SOD_institution") %>% 
  select(year, cert, fips_main = stcnty, fed_rssd = rssdid, specdesc,
         # fed_rssd = rssdid, rssdhcr, name = namefull, city, stalp, zip,
         asset, depsum, depdom) %>% 
  mutate(ag_bank = if_else(specdesc == "AGRICULTURAL", 1, 0)) %>% 
  left_join(districts_main)


# ---- branch -------------------------------------------------------------

# Join the institutional info with branch location and deposits data

districts_br <- tbl(fdic, "district_counties") %>% 
  rename_all(list(~paste0(., "_br")))

branch      <- tbl(fdic, "SOD_branch") %>% 
  select(year, rssdid, cert, depsumbr, brnum, uninumbr, addresbr, namebr,
         stalpbr, citybr, fips_br = stcntybr, zip_br = zipbr,
         lat, long) %>% 
  left_join(institution) %>% 
  left_join(districts_br)

# --- call-reports --------------------------------------------------------

# Select the tables and variables wanted
demos <- tbl(fdic, "BULK_demographics") %>% 
  select(repdte, fed_rssd, rssdhcr, cert, docket,
         name, namehcr, city, stalp, zip, call_quarter, call_year)

netloans <- tbl(fdic, "BULK_netloansandleases") %>% 
  select(repdte, fed_rssd, cert,
         lnlsgr, lnre, lnag, lnreag)

chargeoffs <- tbl(fdic, "BULK_loanchargeoffsandrecoveries") %>% 
  select(repdte, fed_rssd, cert,
         drlnls, crlnls, ntlnls,
         drre, crre, ntre,
         drag, dragsm, crag, cragsm, ntag, ntagsm,
         drreag, crreag, ntreag)

nonaccrual <- tbl(fdic, "BULK_pastdueandnonaccrualassets") %>% 
  select(repdte, fed_rssd, cert,
         contains("asset"), contains("ag"),
         -nagtypar,
         p3re, p9re, nare) %>%
  mutate(loans_d = coalesce(p3asset, 0) + coalesce(p9asset, 0) +
           coalesce(naasset, 0),
         loans_d_alt = coalesce(p9asset, 0) + coalesce(naasset, 0),
         re_loans_d = coalesce(p3re, 0) + coalesce(p9re, 0) +
           coalesce(nare, 0),
         re_loans_d_alt = coalesce(p9re, 0) + coalesce(nare, 0),
         agloans_d = coalesce(p3ag, 0) + coalesce(p3agsm, 0) +
           coalesce(p9ag, 0) + coalesce(p9agsm, 0) +
           coalesce(naag, 0) + coalesce(naagsm, 0),
         agloans_d_alt = coalesce(p9ag, 0) + coalesce(p9agsm, 0) +
           coalesce(naag, 0) + coalesce(naagsm, 0),
         agloans_re_d = coalesce(p3reag, 0) + coalesce(p9reag, 0) +
           coalesce(nareag, 0),
         agloans_re_d_alt = coalesce(p9reag, 0) + coalesce(nareag, 0))

# Combine the tables and collect()
calls <- demos %>% 
  # Only select the June call reports for consistency
  filter(call_quarter == 2) %>% 
  rename(year = call_year) %>% 
  left_join(netloans) %>% 
  left_join(chargeoffs) %>% 
  left_join(nonaccrual)

# Calendar year average
calls_year <- demos %>% 
  left_join(netloans) %>% 
  left_join(chargeoffs) %>% 
  left_join(nonaccrual) %>% 
  rename(year = call_year) %>% 
  group_by(year, fed_rssd) %>% 
  summarise_at(vars(lnlsgr:agloans_re_d_alt), ~sum(., na.rm = T)/n()) %>% 
  ungroup() %>% 
  rename_at(vars(lnlsgr:agloans_re_d_alt), list(~paste0(., "_year")))

# Most recent!
calls_recent <- demos %>% 
  left_join(netloans) %>% 
  left_join(chargeoffs) %>% 
  left_join(nonaccrual) %>% 
  mutate(call_date = call_year + call_quarter/10) %>% 
  filter(call_date > max(call_date, na.rm = T) - 1) %>% 
  mutate(year = max(call_date)) %>% 
  group_by(year, fed_rssd) %>% 
  summarise_at(vars(lnlsgr:agloans_re_d_alt), ~sum(., na.rm = T)/n()) %>% 
  ungroup()


# ---- state --------------------------------------------------------------

# State level financials

j6_state <- branch %>% 
  left_join(calls) %>% 
  left_join(calls_year) %>%
  group_by(fed_rssd, year) %>% 
  mutate(deposits = pmax(depsumbr, depsum),
         adj = if_else(depsumbr == deposits, 1, depsumbr / deposits)) %>% 
  mutate_at(vars(asset, lnlsgr:agloans_re_d_alt,
                 lnlsgr_year:agloans_re_d_alt_year), list(~.*adj)) %>% 
  ungroup() %>% 
  group_by(year, stalpbr) %>% 
  # Caution: this is where the $1,000s are adjusted
  summarise_at(vars(asset, deposits, lnlsgr:agloans_re_d_alt,
                    lnlsgr_year:agloans_re_d_alt_year),
               list(~sum(.*1000, na.rm = T))) %>% 
  collect()

# Also add in the number of branches
state_numbs <- branch %>% 
  left_join(calls) %>% 
  left_join(calls_year) %>%
  group_by(fed_rssd, year) %>%
  mutate(deposits = pmax(depsumbr, depsum),
         adj = if_else(depsumbr == deposits, 1, depsumbr / deposits)) %>%
  ungroup() %>%
  group_by(year, stalpbr) %>% 
  # Caution: this is where the $1,000s are adjusted
  summarise(total_branches = n(),
            total_ag_branches = sum(ag_bank, na.rm = T),
            total_ag_branch_deposits = sum(ag_bank*deposits*1000,
                                           na.rm = T)) %>% 
  collect()

j7 <- left_join(j6_state, state_numbs)


write_rds(j7, paste0(local_dir, "/state_branches.rds"))
j7 %>% 
  select(year, stalpbr, loans = lnlsgr, re_loans = lnre, agloans = lnag,
         agloans_re = lnreag,
         loans_year = lnlsgr_year, re_loans_year = lnre_year,
         agloans_year = lnag_year, agloans_re_year = lnreag_year,
         contains("loan"), contains("branch")) %>% 
  write_csv(paste0(local_dir, "/state_branches.csv"))


# Recent Collect

recent_state <- branch %>% 
  filter(year == max(year, na.rm = T)) %>% 
  select(-year) %>% 
  left_join(calls_recent) %>% 
  mutate(deposits = pmax(depsumbr, depsum),
         adj = if_else(depsumbr == deposits, 1, depsumbr / deposits)) %>% 
  mutate_at(vars(asset, lnlsgr:agloans_re_d_alt),
            list(~.*adj)) %>% 
  ungroup() %>% 
  group_by(year, stalpbr) %>% 
  # Caution: this is where the $1,000s are adjusted
  summarise_at(vars(asset, deposits, lnlsgr:agloans_re_d_alt),
               list(~sum(.*1000, na.rm = T))) %>% 
  collect()

recent_num <- branch %>% 
  filter(year == max(year, na.rm = T)) %>% 
  select(-year) %>% 
  left_join(calls_recent) %>% 
  mutate(deposits = pmax(depsumbr, depsum),
         adj = if_else(depsumbr == deposits, 1, depsumbr / deposits)) %>%
  ungroup() %>%
  group_by(year, stalpbr) %>% 
  # Caution: this is where the $1,000s are adjusted
  summarise(total_branches = n(),
            total_ag_branches = sum(ag_bank, na.rm = T),
            total_ag_branch_deposits = sum(ag_bank*deposits*1000,
                                           na.rm = T)) %>% 
  collect()

recent <- recent_state %>% 
  left_join(recent_num) %>% 
  ungroup() %>% 
  filter(!is.na(year))

write_rds(recent, paste0(local_dir, "/state_branches_recent.rds"))
recent %>% 
  select(year, stalpbr,
         loans = lnlsgr, re_loans = lnre, agloans = lnag,
         agloans_re = lnreag, contains("loan"), contains("branch")) %>% 
  write_csv(paste0(local_dir, "/state_branches_recent.csv"))

