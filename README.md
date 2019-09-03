# FDIC Data

This repository is meant as a way for organizing FDIC Call Reports and Summary of Deposits data through R and SQL. The main goal is to calculate agricultural financial statistics from this data, although the data can be used for much more than just agricultural focused issues.

## Repository Organization

For most users, the raw data are of the most importance and they are referenced above. However, this project is open-source and meant to be allow users to replicate the results to cross-check the validity of the data. If any errors are found, please submit a pull request.

- Raw data can be found in the [0-data](0-data)
    - Download the data: [`0-FDIC-calls-data.R`](0-FDIC-calls-data.R), [`0-FDIC-chicago-data.R`](0-FDIC-chicago-data.R), and [`0-FDIC-SOD-data.R`](0-FDIC-SOD-data.R)
    - Insert data into SQL: [`0-FDIC-calls-sql.R`](0-FDIC-calls-sql.R), [`0-FDIC-chicago-sql.R`](0-FDIC-chicago-sql.R), and [`0-FDIC-SOD-sql.R`](0-FDIC-SOD-sql.R)
- Tidy data can be found in [1-tidy](1-tidy)
- Some generic figures can be found in [2-eda](2-eda)

## Packages Needed

A few packages needs to be installed to maintain this repository that are on CRAN and can be installed with the `install.packages()` command:

```R
install.packages(c("DBI", "lubridate", "ggmap", "haven", "httr", "RSQLite", "rvest", "tidyverse", "zipcode"))
```

A quick reasoning for each package:

- [DBI](https://db.rstudio.com/dbi/) - database interface with R
- lubridate - helpful for time series
- ggmap - mapping of locations
- haven - read in SAS
- httr - web scraping of the FDIC and Chicago Fed
- RSQLite - able to create SQLite databases
- rvest - web scraping
- tidyverse - useful for data munging
- zipcode - coordinates of zip codes
