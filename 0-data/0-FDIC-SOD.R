# Summary of Deposits from FDIC
# https://www5.fdic.gov/sod/dynaDownload.asp?barItem=6

library("tidyverse")

local_dir   <- "0-data/FDIC/SOD"
data_source <- paste0(local_dir, "/raw")
if (!file.exists(local_dir)) dir.create(local_dir, recursive = T)
if (!file.exists(data_source)) dir.create(data_source, recursive = T)

# Download the variable definitions for later

sod_defs     <- paste0(local_dir, "/sod_variables_definitions.xls")
sod_defs_url <- paste0("https://www5.fdic.gov/sod/pdf/",
                       "sod_variables_definitions.xls")
if (!file.exists(sod_defs)) download.file(sod_defs_url, sod_defs)

# Download the files into raw if they do not currently exist:

sod_base <- paste0("https://www5.fdic.gov/sod/",
                   "ShowFileWithStats1.asp?strFileName=")

sod_urls  <- paste0(sod_base, "ALL_",
                    format(Sys.Date(), "%Y"):1994, ".zip")
sod_files <- paste0(data_source, "/ALL_",
                    format(Sys.Date(), "%Y"):1994, ".zip")

map2(sod_urls, sod_files, function(urls, files){
  if (!file.exists(files)) {
    Sys.sleep(runif(1, 2, 3))
    download.file(urls, files)
    }
  })


# ---- read ---------------------------------------------------------------

# Files have many rows, so there's a few csv files in the zip to correct 
#  for older versions of excel that cannot handle that many rows. Avoid those
#  files by unzipping then selecting only the main file with the ALL_YEAR.csv
#  and avoid the ALL_YEAR1.csv and ALL_YEAR2.csv or other files.

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
            funs(parse_number))

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

bouwman <- read_csv(bouwman_zip, col_types = cols(.default = "c")) %>% 
  rename(rssdid = RSSD9001, charter_type = RSSD9048, rssdhcr = RSSD9348,
         primary_insurer = RSSD9424, bank_type_analysis_code = RSSD9425,
         depsumbr = SUMD2700, namefull = SUMD9011, brnum = SUMD9021,
         cert = SUMD9050, addresbr = SUMD9110, citybr = SUMD9135,
         county_code = SUMD9150, pmsa = SUMD9180, state_code = SUMD9210,
         bank_system_code = SUMD9310, num_offices = SUMD9380)

# When brnum == -1, this is a summary observation and should not be included.
non_summary <- bouwman %>% 
  filter(brnum != "-1") %>% 
  mutate_at(vars(depsumbr:num_offices, rssdid, rssdhcr), parse_number) %>% 
  mutate(fips = 1000*state_code + county_code,
         year = parse_number(str_sub(date, 1, 4)))

write_csv(non_summary, paste0(local_dir, "/bouwman_branches.csv"))
write_rds(non_summary, paste0(local_dir, "/bouwman_branches.rds"))
