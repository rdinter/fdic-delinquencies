# Order of Operations

# ---- download -----------------------------------------------------------

# Raw data downloads
source("0-data/0-FDIC-calls-data.R") # Download call reports directly
source("0-data/0-FDIC-SOD-data.R") # SOD downloads
# Optional:
# source("0-data/0-FDIC-chicago-data.R") # Chicago Fed call reports


# ---- SQL-setup ----------------------------------------------------------

# SQLite setup, possibly need to ensure this is on your system? Unsure of how Windows works
source("0-data/0-FDIC-calls-sql.R")
source("0-data/0-FDIC-SOD-sql.R")
# Optional:
# source("0-data/0-FDIC-chicago-sql.R")


# ---- tidy ---------------------------------------------------------------

# Tidy up the data for usable formats, mostly county level
source("1-tidy/1-FDIC-branch.R")
source("1-tidy/1-FDIC-county.R")
source("1-tidy/1-FDIC-state.R")

