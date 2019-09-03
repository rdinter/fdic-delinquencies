#######
# Branch level deposit volume, 

# Probably too much data for this to be useful

#
library("DBI")
library("lubridate")
library("RSQLite")
library("stringr")
library("tidyverse")

local_dir   <- "1-tidy/branch"
if (!file.exists(local_dir)) dir.create(local_dir, recursive = T)

fdic <- dbConnect(SQLite(), "0-data/FDIC/fdic.sqlite")

# First join the institutional data with the branches, then gather up the call
#  report data needed to merge with the institution.

# Keep all the SQL executable commands before the collect() to speed up
#  the process

# ---- sod ----------------------------------------------------------------

# Variable of "year" will match up with the June 30th survey date
#  also implies that the quarter and month are going to be the same throughout

# districts_main <- tbl(fdic, "district_counties") %>% 
#   rename_all(list(~paste0(., "_main")))

institution <- tbl(fdic, "SOD_institution") %>% 
  select(year, cert, fips_main = stcnty, fed_rssd = rssdid, specdesc,
         # fed_rssd = rssdid, rssdhcr, name = namefull, city, stalp, zip,
         asset, depsum, depdom) %>% 
  mutate(ag_bank = if_else(specdesc == "AGRICULTURAL", 1, 0))# %>% 
  # left_join(districts_main)


# ---- branch -------------------------------------------------------------

# Join the institutional info with branch location and deposits data

# districts_br <- tbl(fdic, "district_counties") %>% 
#   rename_all(list(~paste0(., "_br")))

branch      <- tbl(fdic, "SOD_branch") %>% 
  select(year, rssdid, cert, depsumbr, brnum, uninumbr, #addresbr, namebr,
         stalpbr, #citybr, zip_br = zipbr,
         fips_br = stcntybr, lat, long) %>% 
  left_join(institution) #%>% 
  # left_join(districts_br) %>% 
  # collect()

# --- call-reports --------------------------------------------------------

# Select the tables and variables wanted
demos <- tbl(fdic, "BULK_demographics") %>% 
  select(repdte, fed_rssd, rssdhcr, cert, docket,
         name, namehcr, city, stalp, zip, call_quarter, call_year)

netloans <- tbl(fdic, "BULK_netloansandleases") %>% 
  select(repdte, fed_rssd, cert,
         lnlsgr, lnre, lnag, lnreag)

# chargeoffs <- tbl(fdic, "BULK_loanchargeoffsandrecoveries") %>% 
#   select(repdte, fed_rssd, cert,
#          drlnls, crlnls, ntlnls,
#          drre, crre, ntre,
#          drag, dragsm, crag, cragsm, ntag, ntagsm,
#          drreag, crreag, ntreag)

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
  # left_join(chargeoffs) %>% 
  left_join(nonaccrual)

# Calendar year average
calls_year <- demos %>% 
  left_join(netloans) %>% 
  # left_join(chargeoffs) %>% 
  left_join(nonaccrual) %>% 
  rename(year = call_year) %>% 
  group_by(year, fed_rssd) %>% 
  summarise_at(vars(lnlsgr:agloans_re_d_alt), ~sum(., na.rm = T)/n()) %>% 
  ungroup() %>% 
  rename_at(vars(lnlsgr:agloans_re_d_alt), list(~paste0(., "_year")))

# ---- aggregating --------------------------------------------------------

# Calculate the adjustment factor based on share of deposits then aggregate
#  the adjusted financial data by each county

j6_branch <- branch %>% 
  left_join(calls) %>% 
  left_join(calls_year) %>%
  group_by(fed_rssd, brnum, uninumbr, year) %>% 
  mutate(deposits = pmax(depsumbr, depsum),
         adj = if_else(depsumbr == deposits, 1, depsumbr / deposits)) %>% 
  # Caution: this is where the $1,000s are adjusted
  mutate_at(vars(asset, lnlsgr:agloans_re_d_alt,
                 lnlsgr_year:agloans_re_d_alt_year), list(~.*adj*1000)) %>% 
  # ungroup() %>% 
  # group_by(year, fips_br) %>% 
  # summarise_at(vars(asset, deposits, lnlsgr:agloans_re_d_alt,
  #                   lnlsgr_year:agloans_re_d_alt_year),
  #              list(~sum(., na.rm = T))) %>% 
  collect()

# All of the branch data, which is excessive
# j6_branch %>% 
#   select(year, fed_rssd, brnum, uninumbr, lat, long,
#          depsumbr,
#          loans = lnlsgr, re_loans = lnre, agloans = lnag,
#          agloans_re = lnreag) %>% 
#   write_csv(paste0(local_dir, "/branches.csv"))
j6_branch %>%
  select(year, fed_rssd, brnum, uninumbr, lat, long, stalpbr,
         depsumbr,
         loans = lnlsgr, re_loans = lnre, agloans = lnag,
         agloans_re = lnreag) %>%
#  filter(!(stalpbr %in% c("AS", "FM", "GU", "MH", "MP", "VI", "PR", "PW"))) %>% 
  write_rds(paste0(local_dir, "/branches.rds"))

# Important one: the most recent locations
j6_branch %>% 
  select(year, fed_rssd, brnum, uninumbr, lat, long, stalpbr,
         depsumbr,
         loans = lnlsgr, re_loans = lnre, agloans = lnag,
         agloans_re = lnreag) %>% 
  ungroup() %>% 
  filter(#!(stalpbr %in% c("AS", "FM", "GU", "MH", "MP", "VI", "PR", "PW")),
         year == max(year)) %>% 
  write_csv(paste0(local_dir, "/branches_recent.csv"))

# What do these look like
# j6_branch %>%
#   filter(!(stalpbr %in% c("AS", "FM", "GU", "MH", "MP", "VI", "PR", "PW")),
#          year == 2018) %>%
#   ggplot(aes(long, lat)) + geom_point()
