## 2023-06-12

### CRNG ESD DIMA

- error with project table ingest: data ingests normally but not the ProjectTable
Traceback suggests has a nonetype value on projectname column

- the ingester displayed nonetype value not on project table update, but on
parsing a table that is allowed by `arcno.actual_list` but is not available
for `batch_utilities.table_operations` because it had not been implemented
("tblTreeDenHeader", "tblTreeDenDetail"). Added entries for these 2 tables in `batch_utilities`, `tablefields`, `utility_functions`, `tables\__init__` and `tables\`. added exemption to `pk_appender`.

### DuRP DIMA

- six entries for tblPlots but once ingested only three make it to postgres.
empty tblPlot for these missing primarykeys suggests that an intermediary step
expects table data to create joined dataframes, but joins fail because the column
it joins on has missing values.
