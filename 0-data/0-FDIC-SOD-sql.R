# Summary of Deposits from FDIC
# https://www5.fdic.gov/sod/dynaDownload.asp?barItem=6

# ---- start --------------------------------------------------------------

library("DBI")
library("ggmap")
library("RSQLite")
library("tidyverse")
library("zipcode")
data("zipcode")

zipcode <- zipcode %>% 
  mutate(zipbr = parse_number(zip)) %>% 
  select(zipbr, city, state, latitude, longitude)

local_dir   <- "0-data/FDIC/SOD"
data_source <- paste0(local_dir, "/raw")
if (!file.exists(local_dir)) dir.create(local_dir, recursive = T)
if (!file.exists(data_source)) dir.create(data_source)

# ---- read-sql -----------------------------------------------------------

# https://cran.r-project.org/web/packages/RSQLite/vignettes/RSQLite.html

fdic_db <- dbConnect(SQLite(), "0-data/FDIC/fdic.sqlite")
# unlink(paste0(local_dir, "fdic_sod.sqlite"))

# Add in the county districts
dist_hacks <- data.frame(fips = c(2010, 46102),
                         county = c("", "OGLALA LAKOTA"),
                         state = c("ALASKA", "SOUTH DAKOTA"),
                         district = c("ALASKA", "SOUTH DAKOTA"),
                         circuit = c("NINTH CIRCUIT", "EIGHTH CIRCUIT"))
districts <- read_csv("0-data/offline/district_counties.csv") %>% 
  rename_all(tolower) %>% 
  mutate(fips = parse_number(fips)) %>% 
  bind_rows(dist_hacks)

dbWriteTable(fdic_db, "district_counties", districts, overwrite = T)

# Make sure this is not selecting the Bouwman historical data
fdic_files <- dir(data_source, full.names = T, pattern = ".zip")
fdic_files <- fdic_files[grepl("ALL", fdic_files)]

files <- map(fdic_files, function(x){
  temp_name <- paste0(str_sub(basename(x), 1, -5), "_")
  txts <- unzip(x, list = T)[, 1]
  txts <- txts[str_detect(txts,".csv")]
  tbls <- str_remove(txts, temp_name)
  tbls <- str_remove(tbls, ".csv")
  
  j5 <- tibble(file_zip = x, file_csv = txts, tables = tbls)
  return(j5)
})

files     <- bind_rows(files)
# Do not select the _1 and _2 tables because they duplicate ALL
files_all <- filter(files, !(tables %in% c(1,2)))

# Add in the tables
sod_map <- pmap(files_all, function(file_zip, file_csv, tables){
  j5 <- read_csv(unz(file_zip, file_csv), col_types = cols(.default = "c"))
  names(j5) <- tolower(names(j5))
  print(file_csv)
  return(j5)
})

sod <- bind_rows(sod_map)

# ---- bouwman ------------------------------------------------------------

# Bouwman historical data
branch_bouwman      <- read_rds("0-data/FDIC/SOD/branch_bouwman.rds")
institution_bouwman <- read_rds("0-data/FDIC/SOD/institution_bouwman.rds")

# Add on lat and longs
state_match <- toupper(state.name)
names(state_match) <- state.abb
city_lats <- zipcode %>% 
  filter(state %in% state.abb) %>% 
  mutate(citybr = paste0(toupper(city), ", ",
                         state_match[state])) %>% 
  group_by(citybr) %>% 
  summarise(lat = mean(latitude, na.rm = T),
            long = mean(longitude, na.rm = T))

lat_bouwman <- branch_bouwman %>%
  left_join(city_lats)

# County lat/longs?


dbWriteTable(fdic_db, "SOD_branch_bouwman", lat_bouwman, overwrite = T)
dbWriteTable(fdic_db, "SOD_institution_bouwman", institution_bouwman,
             overwrite = T)

# ---- branch -------------------------------------------------------------

# TO DO: the uninumbr is missing for a lot of branches pre-2011, this needs to
#  be clarified so there is a consistent identifier for a branch.

branch <- sod %>% 
  select(addresbr, bkmo, brcenm, brnum, brsertyp, cbsa_div_namb, city2br,
         citybr, cntrynab, cntynamb, cntynumb, consold, csabr, csanambr,
         depsumbr, divisionb, metrobr, microbr, msabr, msanamb, namebr,
         necnamb, nectabr, placenum, sims_acquired_date, sims_description,
         sims_established_date, sims_latitude, sims_longitude, sims_projection,
         stalpbr, stcntybr, stnamebr, stnumbr, uninumbr, usa, zipbr, year,
         rssdid, cert, namefull) %>%
  mutate(bkmo = factor(bkmo, levels = c("0", "1"),
                       labels = c("branch", "main office")),
         brcenm = factor(brcenm, levels = c("C", "E", "N", "M"),
                         labels = c("combined", "estimated",
                                    "non-deposit", "main office")),
         brsertyp = factor(brsertyp,
                           levels = c("11", "12", "13", "14", "15", "16", "21",
                                      "22", "23", "24", "25", "26", "27", "28",
                                      "29", "30"),
                           labels = c("full service brick and mortar office",
                                      "full service retail office",
                                      "full service cyber office",
                                      "full service mobile office",
                                      "full service home/phone banking",
                                      "full service seasonal office",
                                      "limited service administrative office",
                                      "limited service military facility",
                                      "limited service facility office",
                                      "limited service loan production office",
                                      "limited service consumer credit office",
                                      "limited service contractual office",
                                      "limited service messenger office",
                                      "limited service retail office",
                                      "limited service mobile office",
                                      "limited service trust office")),
         metrobr = factor(metrobr, levels = c("0", "1"),
                          labels = c("not in core urban area",
                                     "contains core urban area of >50,000") ),
         microbr = factor(metrobr, levels = c("0", "1"),
                          labels = c("not in core urban area",
                                     "contains core urban area of >10,000")),
         sims_acquired_date = as.Date(sims_acquired_date, "%d/%m/%Y"),
         sims_established_date = as.Date(sims_established_date, "%d/%m/%Y"),
         usa = factor(usa, levels = c("0", "1"),
                      labels = c("not headquartered in US",
                                 "headquartered in US"))) %>% 
  mutate_at(vars(brnum, cntynumb, consold, csabr, depsumbr, divisionb, msabr,
                 nectabr, placenum, sims_latitude, sims_longitude, stcntybr,
                 stnumbr, uninumbr, zipbr, year, cert, rssdid),
            parse_number) %>% 
  distinct()

# apply(branch, 2, function(x) sum(is.na(x)))
# addresbr                  bkmo                brcenm                 brnum 
# 59                     0               1888746                     0 
# brsertyp         cbsa_div_namb               city2br                citybr 
# 0               1727398                     0                     0 
# cntrynab              cntynamb              cntynumb               consold 
# 0                     0                     0               2198253 
# csabr              csanambr              depsumbr             divisionb 
# 0                644618                     0                     0 
# metrobr               microbr                 msabr               msanamb 
# 0               2260030                     0                495218 
# namebr               necnamb               nectabr              placenum 
# 3               2172709                     0                    72 
# sims_acquired_date      sims_description sims_established_date         sims_latitude 
# 1792904                130999               1221609                226970 
# sims_longitude       sims_projection               stalpbr              stcntybr 
# 226976                131516                     0                     0 
# stnamebr               stnumbr              uninumbr                   usa 
# 0                     0                225118                     0 
# zipbr                  year                  cert              namefull 
# 0                     0                     0                     0 


# Need to adjust the latitude and longitudes that are missing, but cannot do
#  it via brnum/uninumbr because there are missing IDs
branch <- branch %>% 
  arrange(rssdid, cert, brnum, uninumbr, year) %>% 
  mutate(lat = if_else(sims_latitude < 10, NA_real_, sims_latitude),
         long = if_else(sims_longitude > -50, NA_real_, sims_longitude)) %>% 
  mutate(lat = if_else(is.na(lat) | is.na(long), NA_real_, lat),
         long = if_else(is.na(lat) | is.na(long), NA_real_, long))

branch <- branch %>% 
  left_join(zipcode) %>% 
  mutate(lat = if_else(is.na(lat), latitude, lat),
         long = if_else(is.na(long), longitude, long)) %>% 
  select(-city, -state, -latitude, -longitude)

#####
# Grab the remaining missing ones and put them through the Google maps API

na_lons_google <- branch %>% 
  filter(is.na(lat)) %>% 
  select(addresbr, citybr, stalpbr, zipbr) %>% 
  distinct()

if (file.exists("0-data/FDIC/SOD/na_locations.rds")) {
  na_locations <- read_rds(paste0(local_dir,"/na_locations.rds")) %>% 
    right_join(na_lons_google)
  
  na_lons_google <- na_locations %>% 
    filter(is.na(lat)) %>% 
    mutate(address = paste(if_else(is.na(addresbr) , "", addresbr),
                           citybr, stalpbr, zipbr))
}

# If there are any locations which are still missing, find them.
if (nrow(na_lons_google) > 0) {
  branch_locations_dsk <- geocode(na_lons_google$address,
                                  output = c("latlon"),
                                  source = c("dsk"))
  
  na_lons_google$lon <- branch_locations_dsk$lon
  na_lons_google$lat <- branch_locations_dsk$lat
  
  na_lons_google <- na_lons_google %>% 
    mutate(lat = if_else(lat < 10, NA_real_, lat),
           lon = if_else(lon > -50, NA_real_, lon)) %>% 
    mutate(lat = if_else(is.na(lat) | is.na(lon), NA_real_, lat),
           lon = if_else(is.na(lat) | is.na(lon), NA_real_, lon))
  
  na_lons_google2 <- na_lons_google %>% 
    filter(is.na(lat)) %>% 
    mutate(address = paste0(citybr, ", ", stalpbr))
  
  branch_locations_dsk2 <- geocode(na_lons_google2$address,
                                   output = "latlon",
                                   source = "dsk")
  
  na_lons_google2$lon     <- branch_locations_dsk2$lon
  na_lons_google2$lat     <- branch_locations_dsk2$lat
  na_lons_google2$address <- NULL
  
  if (exists("na_locations")) {
    na_locations <- na_lons_google %>% 
      filter(!is.na(lat)) %>% 
      select(-address) %>% 
      bind_rows(na_lons_google2) %>% 
      bind_rows(na_locations)
  } else {
    na_locations <- na_lons_google %>% 
      filter(!is.na(lat)) %>% 
      select(-address) %>% 
      bind_rows(na_lons_google2)
  }
  
  write_rds(na_locations, paste0(local_dir,"/na_locations.rds"))
}
#####

# Now split the branch data by lat/lon that exists
na_branch <- branch %>% 
  filter(!is.na(lat))
branch <- branch %>% 
  filter(is.na(lat)) %>% 
  select(-lat, -long) %>% 
  left_join(na_locations) %>% 
  rename(long = lon) %>% 
  bind_rows(na_branch) %>% 
  arrange(year, cert, uninumbr)

# Write the tables
dbWriteTable(fdic_db, "SOD_branch", branch, overwrite = T)


# ---- institution --------------------------------------------------------

institution <- sod %>% 
  select(address, asset, bkclass, call, cert, charter, chrtagnn, chrtagnt,
         city, clcode, cntryna, denovo, depdom, depsum, docket, escrow,
         fdicdbs, fdicname, fed, fedname, insagnt1, insbrdd, insbrts, insured,
         namefull, occdist, occname, regagnt, rssdhcr, rssdid, specdesc,
         specgrp, stalp, stcnty, stname, unit, year, zip) %>% 
  mutate(bkclass = factor(bkclass, levels = c("N", "NM", "OI", "SA",
                                              "SB", "SL", "SM"),
                          labels = c("national member banks",
                                     "state nonmember banks",
                                     "other insured institutions",
                                     "savings associations",
                                     "savings banks and savings and loans",
                                     "state stock savings and loans",
                                     "state member banks")),
         call = factor(call, levels = c("CALL", "TFR"),
                       labels = c("call report", "thrift financial report")),
         # clcode = factor(clcode,
         #                 levels = c("3", "13", "15", "21", "23", "33", "34",
         #                            "35", "36", "37", "38", "41", "42", "43",
         #                            "44", "50", "52", "99"),
         #                 labels = c("national member bank",
         #                            "state member bank",
         #                            "state member industrial bank",
         #                            "state nonmember bank",
         #                            "state nonmember industrial bank",
         #                            "federal stock savings bank",
         #                            "federal mutual savings bank",
         #                            "state stock s&l association",
         #                            "state mutual s&l association",
         #                            "federal stock s&l association",
         #                            "federal mutual s&l association",
         #                            "state stock savings bank",
         #                            "state mutual savings bank",
         #                            # "federal stock savings bank",
         #                            # "federal mutual savings bank",
         #                            "national member trust company",
         #                            "international banking act", "other")),
         insagnt1 = factor(insagnt1, levels = c("BIF", "DIF", "SAIF"),
                           labels = c("bank insurance fund",
                                      "deposit insurance fund",
                                      "savings insurance fund")),
         insured = factor(insured, levels = c("CB", "IB", "SA"),
                          labels = c("commercial banks",
                                     "insured branches of foreign banks",
                                     "savings institutions")),
         occdist = factor(occdist, levels = c("1", "2", "3", "4", "5", "6"),
                          labels = c("northeast", "southeast", "central",
                                     "midwest", "southwest", "west")),
         specgrp = factor(as.numeric(specgrp),
                          levels = c("0" ,"1" ,"2" ,"3", "4",
                                     "5", "6", "7", "8", "9"),
                          labels = c("no specialization",
                                     "international specialization",
                                     "agricultural specialization",
                                     "credit card specialization",
                                     "commercial lending specialization",
                                     "mortgage lending specialization",
                                     "consumer lending specialization",
                                     "other specialized under a billion",
                                     "all other under 1 billion",
                                     "all other over 1 billion")),
         unit = factor(unit, levels = c("0", "1"),
                       labels = c("branching bank", "unit bank"))) %>% 
  mutate_at(vars(asset, cert, depdom, depsum, docket, escrow, fdicdbs, fed,
                 insbrdd, insbrts, rssdhcr, rssdid, stcnty, year, zip),
            parse_number) %>% 
  distinct()

# Correct for the mismatch addresses and how depsum will vary. Need to have
#  the unique cert-year codes -- everything else needs to be consistent:
#  cert  | rssdid
#  34966 | 2849463
#  35154 | 2916534
#  35329 | 498184
#  35602 | 449935
#  35518 | 2806877
#  35527 | 3001437

demo_inst <- institution %>% 
  select(-asset, -depsum, -depdom, -clcode, -cntryna) %>%
  mutate(address = if_else(address == "14 Danbury Road",
                           "145 Bank Street, Webster Plaza",
                           address),
         rssdid = case_when(cert == 34966 ~ 2849463,
                            cert == 35154 ~ 2916534,
                            cert == 35329 ~ 498184,
                            cert == 35602 ~ 449935,
                            cert == 35518 ~ 2806877,
                            cert == 35527 ~ 3001437,
                            T ~ rssdid),
         rssdhcr = if_else(rssdhcr == 0, NA_real_, rssdhcr)) %>%
  distinct()

dep_inst <- institution %>% 
  select(cert, year, asset, depsum, depdom) %>% 
  group_by(cert, year) %>% 
  summarise_at(vars(asset, depsum, depdom), max) %>% 
  distinct()

institution <- demo_inst %>% 
  left_join(dep_inst)

dbWriteTable(fdic_db, "SOD_institution", institution, overwrite = T)

# ---- bhc ----------------------------------------------------------------

# GILMAN 1119682 to WINCHESTER
# ASHLEY 1125517 to WISHEK
# 1131611 problem with hctmult, needs to be "ONE"
# 1242562 problem with hctmult, needs to be "ONE"
# 1244221 problem with hctmult, needs to be "ONE"
# 2961897 state needs to be NA
# 3035227 problem with hctmult, needs to be "MULT"
# 3072660 in 2016 hctmult needs to be "MULT"
# 3613504 problem with hctmult, needs to be "MULT"

bhc <- sod %>% 
  select(cityhcr, hctmult, namehcr, rssdhcr, stalphcr, year) %>%
  mutate(rssdhcr = parse_number(rssdhcr),
         year = parse_number(year),
         stalphcr = if_else(rssdhcr == 2961897, "NA", stalphcr),
         cityhcr = case_when(rssdhcr == 1119682 &
                               cityhcr == "GILMAN" ~ "WINCHESTER",
                             rssdhcr == 1125517 &
                               cityhcr == "ASHLEY" ~ "WISHEK",
                             T ~ cityhcr),
         hctmult = case_when(rssdhcr == 1131611 ~ "ONE",
                             rssdhcr == 1242562 ~ "ONE",
                             rssdhcr == 1244221 ~ "ONE",
                             rssdhcr == 3035227 ~ "MULT",
                             rssdhcr == 3613504 ~ "MULT",
                             rssdhcr == 3072660 & year == 2016 ~ "MULT",
                             T ~ hctmult)) %>% 
  filter(rssdhcr != 0) %>% 
  mutate(hctmult = factor(hctmult, levels = c("NONE", "ONE", "MULT"),
                          labels = c("not a member of a bank holding company",
                                     "one bank holding company",
                                     "multi-bank holding company"))) %>% 
  distinct()


dbWriteTable(fdic_db, "SOD_bankholdingcompany", bhc, overwrite = T)


dbDisconnect(fdic_db)
