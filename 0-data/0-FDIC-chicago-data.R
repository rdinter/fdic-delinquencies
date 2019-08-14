#######
# Chicago Fed Call Reports Data (in SAS format, zipped)
# https://www.chicagofed.org/banking/
#  financial-institution-reports/commercial-bank-data
# http://bit.ly/2rVkzjS

# ---- start --------------------------------------------------------------

library("haven")
library("httr")
library("rvest")
library("tidyverse")

local_dir   <- "0-data/FDIC/calls_chicago"
data_source <- paste0(local_dir, "/raw")
if (!file.exists(local_dir)) dir.create(local_dir, recursive = T)
if (!file.exists(data_source)) dir.create(data_source)

fed_base <- paste0("https://www.chicagofed.org/banking/",
                   "financial-institution-reports/")

fed_links <- paste0(fed_base, c("commercial-bank-data-complete-1976-2000",
                                "commercial-bank-data-complete-2001-2010",
                                "commercial-bank-structure-data"))

fed_map <- map(fed_links, function(x) {
  x %>% 
    read_html() %>% 
    html_nodes(".vfe a") %>% 
    html_attr("href") %>% 
    paste0("https://www.chicagofed.org", .)
})

fed_links <- unlist(fed_map)

map(fed_links, function(x){
  x1     <- str_replace(basename(x), "\\?la=en", "")
  x1     <- str_replace(x1, "zipx", "zip")
  file_x <- paste0(data_source, "/", x1)
  if (!file.exists(file_x)) {
    Sys.sleep(runif(1, 2, 3))
    download.file(x, file_x, mode = "wb")
  } 
})

fdic_files <- dir(data_source, full.names = T)

