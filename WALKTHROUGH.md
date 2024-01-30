1. running `batch looper()` with a path to the dima, desired projectkey, destination db and primarykey moving-window range

2. `batch_looper()`:
  - creates a list of tables contained inside the dima 
  - a loop initiatesover the list of tables, with each individual table fed into the `looper()` function.
  - uses the `looper()` function to aggregate the *same table* across multiple dima files.
  - with every dataframe created by the `looper()` function, 
    - we check if the table exists on the db using `tablecheck()`
    - ingest using the `main_ingest()` method of the Ingester class. 

<!-- LOOPER -->
1. `looper()`:
  - uses the `table_operations` as a switch-case to determine which function will parse each read dima table. 
  - creates a dictionary with all the parsed dataframes from each dima
  - 