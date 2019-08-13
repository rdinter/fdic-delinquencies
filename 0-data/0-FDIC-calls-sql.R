#######
# FDIC Call Reports Bulk Data 1992 to 2016+
# https://www5.fdic.gov/sdi/download_large_list_outside.asp

library("DBI")
library("RSQLite")
library("tidyverse")

local_dir   <- "0-data/FDIC/calls"
data_source <- paste0(local_dir, "/raw")
if (!file.exists(local_dir)) dir.create(local_dir, recursive = T)
if (!file.exists(data_source)) dir.create(data_source)

# https://cran.r-project.org/web/packages/RSQLite/vignettes/RSQLite.html

fdic_db <- dbConnect(SQLite(), "0-data/FDIC/fdic.sqlite")
# unlink(paste0(local_dir, "fdic_calls.sqlite"))

# Demo Vars
demos <- c("repdte", "rundate", "fed_rssd", "rssdhcr", "cert", "docket",
           "name", "namehcr", "address", "city", "stalp", "zip",
           "citystatezip", "msa", "cbsa_metro", "cbsa_metro_name", "county",
           "inst.webaddr", "status", "offdom", "offfor", "offoa", "stmult",
           "sod_desc", "listofoffices", "specgrp", "subchaps", "estymd",
           "insdate", "procdate", "effdate", "mutual", "parcert", "bkclass",
           "cb", "trust", "regagnt", "insagnt1", "qbprcoml", "fdicdbs",
           "fdicsupv", "fldoff", "fed", "occdist", "otsregnm", "callform",
           "webaddr", "te01n529", "te02n529", "te03n529", "te04n529",
           "te05n529", "te06n529", "te01n528", "te02n528", "te03n528",
           "te04n528", "te05n528", "te06n528", "te07n528", "te08n528",
           "te09n528", "te10n528")

# Do not grab the "calls", which need to be parsed through from the Chicago
#  Fed. Correction is to only grab the .zip files instead of the folder.
fdic_files <- dir(data_source, full.names = T, pattern = ".zip")

files <- map(fdic_files, function(x){
  temp_name <- paste0(str_sub(basename(x), 1, -5), "_")
  txts <- unzip(x, list = T)[, 1]
  txts <- txts[str_detect(txts,".csv")]
  tbls <- str_remove(txts, temp_name)
  tbls <- str_remove(tbls, ".csv")
  
  j5 <- tibble(file_zip = x, file_csv = txts, tables = tbls)
  return(j5)
})

files <- bind_rows(files)

# Change table names
tab_name <- c("Covered by FDIC Loss-Share Agreements" =
                paste0("Carrying Amount of Assets Covered by ",
                       "FDIC Loss-Share Agreements"),
              "- PD & NA Loans Wholly or Partially US Gvmt Guaranteed" =
                paste0("- Past Due and Nonaccrual Loans Wholly or ",
                       "Partially US Gvmt Guaranteed"),
              "Unused Commitments" = "Unused Commitments Securitization")

files$tbls <- files$tables
files$tbls[files$tbls %in% tab_name] <-
  names(tab_name)[na.omit(match(files$tbls, tab_name))]
files$tbls <- gsub('[[:punct:] ]+', ' ', files$tbls)
files$tbls <- str_remove_all(tolower(files$tbls), " ")
files$tbls <- paste0("BULK_", files$tbls)

# Let's create a demographics table ... which table?
demo_table <- files %>% 
  filter(tbls == "BULK_14familyresidentialnetloansandleases") %>% 
  pmap(function(file_zip, file_csv, tables, tbls){
    j5 <- read_csv(unz(file_zip, file_csv), col_types = cols(.default = "c"))
    names(j5) <- tolower(names(j5))
    print(file_csv)
    return(j5)
})

demo_table <- bind_rows(demo_table)

demos_temp  <- demos[demos %in% names(demo_table)]
j8    <- demo_table %>% 
  select(demos_temp) %>% 
  mutate_at(vars(fed_rssd, rssdhcr, cert, docket), parse_number) %>% 
  mutate(fed_rssd = case_when(cert == 17798 ~ 782306,
                              cert == 34966 ~ 2849463,
                              T ~ fed_rssd),
         rssdhcr = if_else(rssdhcr == 0, NA_real_, rssdhcr)) # %>% 
  # mutate_at(vars(repdte, rundate, estymd, insdate, effdate),
  #           function(x) as.Date(x, "%m/%d/%Y"))
  

dbWriteTable(fdic_db, "BULK_demographics", j8, overwrite = TRUE)

# ---- combine tables -----------------------------------------------------

demo <- by(files, files$tbls, function(x){
  j7 <- pmap(x, function(file_zip, file_csv, tables, tbls){
    j5 <- read_csv(unz(file_zip, file_csv), col_types = cols(.default = "c"))
    names(j5) <- tolower(names(j5))
    print(file_csv)
    return(j5)
    })
  
  j8 <- bind_rows(j7)
  
  # Grab only the demographic variables that exist, then put them to the start.
  demos_temp  <- demos[demos %in% names(j8)]
  var_numeric <- names(j8)[!(names(j8) %in% demos)]
  j8    <- select(j8, demos_temp, everything())
  
  j8 <- mutate_at(j8, var_numeric, parse_number)
  
  # Take out the demographic variables that are redundant
  j8 <- j8 %>% 
    select(-one_of(demos_temp[!(demos_temp %in%
                                  c("repdte","fed_rssd", "cert"))])) %>% 
    mutate(cert = parse_number(cert),
           fed_rssd = parse_number(fed_rssd),
           fed_rssd = case_when(cert == 17798 ~ 782306,
                                cert == 34966 ~ 2849463,
                                T ~ fed_rssd))
  
  dbWriteTable(fdic_db, x$tbls[1], j8, overwrite = TRUE)
  return("Done")
})

dbDisconnect(fdic_db)
