# FDIC Data

This repository is meant as a way for organizing FDIC Call Reports and Summary of Deposits data through R and SQL. The main goal is to calculate agricultural financial statistics from this data, although the data can be used for much more than just agricultural focused issues.

## Repository Organization

For most users, the raw data are of the most importance and they are referenced above. However, this project is open-source and meant to be allow users to replicate the results to cross-check the validity of the data. If any errors are found, please submit a pull request.

- Raw data can be found in the [0-data](0-data)
- Tidy data can be found in [1-tidy](1-tidy)
- Some generic figures can be found in [2-eda](2-eda)

## Packages Needed

A few packages needs to be installed to maintain this repository. Most of these are on CRAN and can be installed with the `install.packages()` command but one requires the devtools to be installed to install a package on GitHub:

```R
install.packages("devtools", "gdata", "haven", "httr", "lubridate", "readxl", "rvest", "stringr", "tabulizer", "tidyverse", "zipcode", "zoo")
devtools::install_github("rdinter/usdarnass")
```

A quick reasoning for each package:

- gdata - for some old excel files which cannot be read with up to date packages
- haven - to read SAS files for the FJC data
- httr - web scraping the USCourts.gov
- lubridate - formatting for time series data
- readxl - reading newer excel files
- rvest - web scraping the USCourts.gov
- stringr - useful for string parsing
- tabulizer - useful for parsing the PDF tables
- tidyverse - useful for data munging
- usdarnass - downloading data from QuickStats
- zipcode - determining latitude and longitudes for locations in the FJC data
- zoo - useful for time series data