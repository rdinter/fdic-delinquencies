# Summary of Deposits from FDIC
# https://www5.fdic.gov/sod/dynaDownload.asp?barItem=6

# ---- start --------------------------------------------------------------


library("httr")
library("tidyverse")

local_dir   <- "0-data/FDIC/SOD"
data_source <- paste0(local_dir, "/raw")
if (!file.exists(local_dir)) dir.create(local_dir, recursive = T)
if (!file.exists(data_source)) dir.create(data_source, recursive = T)

# Download the variable definitions for later

sod_defs     <- paste0(local_dir, "/sod_variables_definitions.xls")
sod_defs_url <- paste0("https://www5.fdic.gov/sod/pdf/",
                       "sod_variables_definitions.xls")
if (!file.exists(sod_defs)) download.file(sod_defs_url, sod_defs, mode = "wb")

# Download the files into raw if they do not currently exist:

sod_base <- paste0("https://www5.fdic.gov/sod/",
                   "ShowFileWithStats1.asp?strFileName=")

sod_urls  <- paste0(sod_base, "ALL_",
                    format(Sys.Date(), "%Y"):1994, ".zip")
sod_files <- paste0(data_source, "/ALL_",
                    format(Sys.Date(), "%Y"):1994, ".zip")

map2(sod_urls, sod_files, function(urls, files){
  if (!file.exists(files) & !http_error(urls)) {
    Sys.sleep(runif(1, 20, 30))
    download.file(urls, files, mode = "wb")
    }
  })


# ---- read ---------------------------------------------------------------

# Files have many rows, so there's a few csv files in the zip to correct 
#  for older versions of excel that cannot handle that many rows. Avoid those
#  files by unzipping then selecting only the main file with the ALL_YEAR.csv
#  and avoid the ALL_YEAR1.csv and ALL_YEAR2.csv or other files.

# Get zip files for SOD, but not the historical
sod_files <- dir(data_source, full.names = T, pattern = ".zip")
sod_files <- sod_files[!grepl("SoD8193data.zip", sod_files)]

temp_dir <- tempdir()

j5 <- map(sod_files, function(x){
  unzip(x, exdir = temp_dir)
  fil <- paste0(temp_dir, "/", tools::file_path_sans_ext(basename(x)), ".csv")
  mydata <- read_csv(fil, col_types = cols(.default = "c"))
  unlink(temp_dir)
  return(mydata)
})

# Select only a few variables: I want branch name, institution name, unique IDs,
#  address, zip, county, lat-long (and projection), and established date.

branches <- j5 %>% 
  bind_rows() %>% 
  select(YEAR, CERT, DOCKET, BRNUM, UNINUMBR, RSSDHCR, RSSDID,
         NAMEBR, NAMEFULL, NAMEHCR, ADDRESBR, CITYBR, CNTYNAMB,
         STALPBR, ZIPBR, DEPSUMBR, DEPSUM, ASSET, SIMS_ACQUIRED_DATE,
         SIMS_ESTABLISHED_DATE, SIMS_LATITUDE, SIMS_LONGITUDE,
         SIMS_PROJECTION) %>% 
  mutate_at(vars(YEAR, CERT, DOCKET, BRNUM, UNINUMBR, RSSDHCR, RSSDID, ZIPBR,
                 DEPSUMBR, DEPSUM, ASSET, SIMS_LATITUDE, SIMS_LONGITUDE),
            parse_number)

names(branches) <- tolower(names(branches))


# write_csv(branches, paste0(local_dir, "/generic_branches.csv"))
# write_rds(branches, paste0(local_dir, "/generic_branches.rds"))

# ---- pre1994 ------------------------------------------------------------

# From Christa Bouwman at TAMU
# https://sites.google.com/a/tamu.edu/bouwman/data
# https://drive.google.com/file/d/0B8rxlwlMcXW1aXRPMVVzZ19vWlU/view?usp=sharing

library("googledrive")

bouwman <- as_id("0B8rxlwlMcXW1aXRPMVVzZ19vWlU")
drive_download(bouwman, path = paste0(data_source, "/SoD8193data.zip"),
               overwrite = T)

bouwman_zip <- paste0(data_source, "/SoD8193data.zip")

insagnt1_cross <- c("0" = "NONE", "1" = "BIF",
                    "2" = "SAIF", "3" = "NCUSIF",
                    "4" = "State", "5" = "Other",
                    "6" = "FIRREA", "7" = "DIF")

# State fips code to abbreviation
stalpbr_cross <- c("0" = "", "1" = "AL", "2" = "AL", "4" = "AZ", "5" = "AR",
                   "6" = "CA", "8" = "CO", "9" = "CT",
                   
                   "10" = "DE", "11" = "DC", "12" = "FL", "13" = "GA",
                   "15" = "HI", "16" = "ID", "17" = "IL", "18" = "IN",
                   "19" = "IA",
                   
                   "20" = "KS", "21" = "KY", "22" = "LA", "23" = "ME",
                   "24" = "MD", "25" = "MA", "26" = "MI", "27" = "MN",
                   "28" = "MS", "29" = "MO",
                   
                   "30" = "MT", "31" = "NE", "32" = "NV", "33" = "NH",
                   "34" = "NJ", "35" = "NM", "36" = "NY", "37" = "NC",
                   "38" = "ND", "39" = "OH",
                   
                   "40" = "OK", "41" = "OR", "42" = "PA", "44" = "RI",
                   "45" = "SC", "46" = "SD", "47" = "TN", "48" = "TX",
                   "49" = "UT",
                   
                   "50" = "VT", "51" = "VA", "53" = "WA", "54" = "WV",
                   "55" = "WI", "56" = "WY",
                   
                   "60" = "AS", "64" = "FM", "66" = "GU", "68" = "MH",
                   "69" = "MP", "70" = "PW", "72" = "PR",
                   "75" = "", "78" = "VI", "90" = "")


bouwman <- read_csv(bouwman_zip, col_types = cols(.default = "c")) %>% 
  rename(rssdid = RSSD9001, charter_type = RSSD9048, rssdhcr = RSSD9348,
         insagnt1 = RSSD9424, bank_type_analysis_code = RSSD9425,
         depsumbr = SUMD2700, namefull = SUMD9011, brnum = SUMD9021,
         cert = SUMD9050, addresbr = SUMD9110, citybr = SUMD9135,
         cntynumb = SUMD9150, pmsa = SUMD9180, stnumbr = SUMD9210,
         bank_system_code = SUMD9310, num_offices = SUMD9380) %>% 
  mutate(stcntybr = paste0(str_pad(stnumbr, 2, "left", pad = "0"),
                           str_pad(cntynumb, 3, "left", pad = "0")),
         insagnt1 = insagnt1_cross[insagnt1],
         stalpbr = stalpbr_cross[stnumbr])


bank_type_analysis_code_cross <- c("0" = "Not applicable",
                                   "1" = "A bankers bank that is subject to reserve requirements",
                                   "2" = "A bankers bank that is not subject to reserve requirements",
                                   "3" = "Grandfathered nonbank bank",
                                   "4" = "Entity is primarily conducting credit card activities",
                                   "5" = "Wholesale bank (with commercial bank charter)",
                                   "6" = "Standalone Internet Bank (SAIB): Refers to a domestic head office depository entity only (no branches) with no physical location to which customers can go to obtain banking services. All transactions occur via the Internet.",
                                   "7" = "Workout entity (problem-loan, spin-off, or collecting bank)",
                                   "8" = "Depository Institution National Bank (DINB)",
                                   "9" = "Depository trust company",
                                   "10" = "Bridge entity",
                                   "11" = "Banking Edge or agreement corporation",
                                   "12" = "Investment Edge or agreement corporation",
                                   "13" = "Data processing services",
                                   "14" = "Trust preferred securities subsidiary",
                                   "15" = "Cash management banks",
                                   "16" = "Farm Credit System Institution",
                                   "17" = "10L Election - An election made by a state savings bank to be deemed a savings association under section 10(l) of the Home Owners' Loan Act.",
                                   "18" = "Grandfathered SLHC")


# When brnum == -1, this is a summary observation and should not be included.
branch_bouwman <- bouwman %>% 
  filter(brnum != "-1") %>% 
  mutate_at(vars(depsumbr:num_offices, rssdid, rssdhcr, stcntybr),
            parse_number) %>% 
  mutate(fips = 1000*stnumbr + cntynumb,
         year = parse_number(str_sub(date, 1, 4)))

write_csv(branch_bouwman, paste0(local_dir, "/branch_bouwman.csv"))
write_rds(branch_bouwman, paste0(local_dir, "/branch_bouwman.rds"))

institution_bouwman <- bouwman %>% 
  filter(brnum == "-1") %>% 
  select(-brnum) %>% 
  mutate_at(vars(depsumbr:num_offices, rssdid, rssdhcr, stcntybr),
            parse_number) %>% 
  mutate(fips = 1000*stnumbr + cntynumb,
         year = parse_number(str_sub(date, 1, 4))) %>% 
  rename(address = addresbr, depsum = depsumbr, city = citybr,
         stcnty = stcntybr, stalp = stalpbr)

write_csv(institution_bouwman, paste0(local_dir, "/institution_bouwman.csv"))
write_rds(institution_bouwman, paste0(local_dir, "/institution_bouwman.rds"))
