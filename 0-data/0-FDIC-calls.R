#######
# FDIC Call Reports Bulk Data 1992 to 2016+
# https://www5.fdic.gov/sdi/download_large_list_outside.asp

library("httr")
library("stringr")
library("rvest")
library("tidyverse")

local_dir   <- "0-data/FDIC/calls"
data_source <- paste0(local_dir, "/raw")
if (!file.exists(local_dir)) dir.create(local_dir, recursive = T)
if (!file.exists(data_source)) dir.create(data_source)

failed_banks <- "https://www.fdic.gov/bank/individual/failed/banklist.csv"
download.file(failed_banks, paste0(local_dir, "/failed_banks.csv"))

fdic_url <- "https://www5.fdic.gov/sdi/download_large_list_outside.asp"

fdic_links <- fdic_url %>% 
  read_html() %>% 
  html_nodes("table+ table a") %>% 
  html_attr("href") %>% 
  paste0("https://www5.fdic.gov/sdi/", .)

# HACK, apparently you cannot scrape the FDIC links anymore, this should bypass
# that error...
year_file <- expand.grid(1992:format(Sys.Date(), "%Y"),
                         c("1231", "0331", "0630", "0930"),
                         KEEP.OUT.ATTRS = F, stringsAsFactors = F)

fdic_links <- paste0("https://www5.fdic.gov/sdi/Resource/AllReps/All_Reports_",
                     paste0(year_file$Var1, year_file$Var2), ".zip")

map(fdic_links, function(x){
  Sys.sleep(runif(1, 2, 3))
  file_x <- paste0(data_source, "/", basename(x))
  if (!file.exists(file_x) & !http_error(x)) {
    download.file(x, file_x, method = "libcurl")
  }
})

# Do not grab the "calls", which I still need to parse through from the Chicago
#  Fed. Correction is to only grab the .zip files instead of the folder.
fdic_files <- dir(data_source, full.names = T, pattern = ".zip")

# Tables needed:
# Loan Charge-Offs and Recoveries
# Net Loans and Leases
# Past Due and Nonaccrual Assets
# Small Business Loans

temp_dir <- tempdir()

tables_map <- map(fdic_files, function(x){
  unzip(x, exdir = temp_dir)
  files_x <- dir(temp_dir, full.names = T, pattern = "*.csv")
  
  results1 <- grepl(tolower("Small Business Loans"), tolower(files_x))
  results1 <- read_csv(files_x[results1], col_types = cols(.default = "c"))
  names(results1) <- tolower(names(results1))
  # results1$table <- "Small Business Loans"
  # results1$file  <- basename(x)
  
  results2 <- grepl(tolower("Past Due and Nonaccrual Assets"),
                    tolower(files_x))
  results2 <- read_csv(files_x[results2], col_types = cols(.default = "c"))
  names(results2) <- tolower(names(results2))
  # results2$table <- "Past Due and Nonaccrual Assets"
  # results2$file  <- basename(x)
  
  results3 <- grepl(tolower("Loan Charge-Offs and Recoveries"),
                    tolower(files_x))
  results3 <- read_csv(files_x[results3], col_types = cols(.default = "c"))
  names(results3) <- tolower(names(results3))
  # results3$table <- "Loan Charge-Offs and Recoveries"
  # results3$file  <- basename(x)
  
  results4 <- grepl(tolower("Net Loans and Leases"), tolower(files_x)) &
    !(grepl(tolower("Family"), tolower(files_x)))
  results4 <- read_csv(files_x[results4], col_types = cols(.default = "c"))
  names(results4) <- tolower(names(results4))
  # results4$table <- "Net Loans and Leases"
  # results4$file  <- basename(x)
  
  results5 <- grepl(tolower("Assets and Liabilities"), tolower(files_x)) &
    !(grepl(tolower("Foreign"), tolower(files_x)))
  results5 <- read_csv(files_x[results5], col_types = cols(.default = "c"))
  names(results5) <- tolower(names(results5))
  # results5$table <- "Assets and Liabilities"
  # results5$file  <- basename(x)
  
  results6 <- grepl(tolower("Assets and Liabilities"), tolower(files_x)) &
    (grepl(tolower("Foreign"), tolower(files_x)))
  results6 <- read_csv(files_x[results6], col_types = cols(.default = "c"))
  names(results6) <- tolower(names(results6))
  # results6$table <- "Assets and Liabilities in Foreign Offices"
  # results6$file  <- basename(x)
  
  results7 <- grepl(tolower("Performance and Condition Ratios"),
                    tolower(files_x))
  results7 <- read_csv(files_x[results7], col_types = cols(.default = "c"))
  names(results7) <- tolower(names(results7))
  # results7$table <- "Performance and Condition Ratios"
  # results7$file  <- basename(x)
  
  results8 <- grepl(tolower("Other Real Estate Owned"),
                    tolower(files_x))
  results8 <- read_csv(files_x[results8], col_types = cols(.default = "c"))
  names(results8) <- tolower(names(results8))
  # results8$table <- "Other Real Estate Owned"
  # results8$file  <- basename(x)
  
  results <- results1 %>% 
    full_join(results2) %>% 
    full_join(results3) %>% 
    full_join(results4) %>% 
    full_join(results5) %>% 
    full_join(results6) %>% 
    full_join(results7) %>% 
    full_join(results8)
  
  results$file <- basename(x)
  
  results <- mutate_at(results, vars(lnrenr4:oreothf), as.numeric)
  results <- mutate_at(results, vars(lnrenr4:oreothf),
                       funs(ifelse(is.na(.), 0, .*1000)))
  
  unlink(temp_dir, recursive = T)
  return(results)
})

tables_data <- bind_rows(tables_map)

write_rds(tables_data, paste0(local_dir, "/fdic.rds"))
write_csv(tables_data, paste0(local_dir, "/fdic.csv"))