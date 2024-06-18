  ## About the ingester

Command line application to ingest the tabular contents of DIMA files (access databases with an _.mdb_ or _.accdb_ file extension).

## Built with

* Postgresql 11 database
* Docker engine 20.10
* Python 3.8

## Set up

### pre run
Clone this repo into a local directory. Deposit DIMA files to be ingested into the 'dimas' directory. Include the project metadata _.xlsx_ file within this 'dimas' directory. A _database.ini_ file inside the utils directory is required (and not included) with the following format:
```ini
[dima]
dbname= ***
host= ***
port= ***
user= ***
password= ***

[dimadev]
dbname= ***
host= ***
port= ***
user= ***
password= ***

```
### to run the application

1. Build the docker image using the docker-compose engine
```
docker-compose build
```
2. Run the interactive python container
```
docker-compose run --rm ingester
```

## Usage
The application has the following commands: pgtables, ingest, dimatables, report, exit, help.
1. pgtables: displays all the tables currently in the postgres database; required arguments implemented are 'dima' to see a printout of the tables in the production database, or 'dimadev' for a printout of the tables in the development database instead.
```sh
> pgtables dima # print all the tables currently in the production database
```
2. ingest: runs an ingestion cycle through all the dimafiles contained inside the 'dimas' directory. Requires 3 arguments: ProjectKey, custom date range to divide the formdate into, and a boolean that expresses if the cycle should ingest into the development database (True) or the production database (False)
```sh
> ingest testprojectkey 3 True # ingest all the tables, append testprojeckey as a projectkey with a 3 day custom daterange into the development database.
```
3. dimatables: displays all the tables contained across all the dimafiles deposited inside the 'dimas' directory. this function requires no arguments.
```sh
> dimatables
```
4. report: create a csv printout of the primarykeys created for a single table and a custom daterange. requires two arguments: tablename and custom daterange:
```sh
> report tblLines 3 # primary keys for the tblLines table with a custom daterange of 3 days.
```

5. help: printout docstrings with usage notes for each of the implemented functions. function name is only one required argument.
```sh
> help ingest # prints docstrings on the ingest function
```

#test
