import os, os.path, sys
from cmd import Cmd
import logging
from src.project.project import all_dimas, update_project
from src.utils.batch_utilities import batch_looper, table_collector
from src.utils.report import csv_report
from src.tables.tbllines import Lines
from src.utils.batch_utilities import table_operations, looper

logging.basicConfig(format='%(asctime)s | %(levelname)s: %(message)s', level=logging.NOTSET)


class main(Cmd):

    def __init__(self):
        super(main, self).__init__()
        self.prompt = "> "
        self.batch_path = os.path.normpath(os.path.join(os.getcwd(),"dimas"))
        self.dimafiles = [os.path.normpath(f"{self.batch_path}/{i}") for i in os.listdir(self.batch_path)]

    def do_pgtables(self, args):
        """tables currently in dima database or development database
        """
        if args == "dima":
            dimalst = all_dimas("dima")
            print("listing dimas in production db:")
            print(dimalst)

        elif args == "dimadev":
            devlst = all_dimas("dimadev")
            print("listing dimas in development db:")
            print(devlst)

        else:
            print(f"database '{args}' not yet implemented.")

    def do_dimatables(self, args):
        """ returns all tables contained across all the dimas inside the dimas
        directory.
        """
        print(table_collector(self.batch_path))

    def do_report(self,args):
        """
        Create a csv with primary keys and dbkeys for each table.
        If 'all' is used as the table argument, will print out a csv report per table
        inside the dima directory.
        """
        table, pk_formdate_range = args.split(" ", 3)
        tablelist = table_collector(self.batch_path)
        if table in tablelist:
            df = looper(self.batch_path, table,"report_table",pk_formdate_range )
            if "PrimaryKey" in df['dataframe'].columns:
                pk_list = df['dataframe'].PrimaryKey.unique()
                print("******************")
                print(f"number of primarykeys created with Formdate classes every '{pk_formdate_range}' days ")
                print("******************")
                print(f"list of PrimaryKey values using '{pk_formdate_range}' for table '{table}':")
                print(pk_list)
                print("******************")
                csv_report(df, table, self.batch_path)
                print("******************")
            else:
                print(f"No PrimaryKey field created for '{table}'")
        elif 'all' in table:
            revised_tablelist = [i for i in tablelist if i not in ['tblSites','tblSpecies','tblSpeciesGeneric', 'tblPlotNotes', 'tblPlotHistory']]
            print("creating a report for each table...")
            print("******************")
            for i in revised_tablelist:
                print(f"creating csv report for {}..")
                df = looper(self.batch_path, i,"report_table",pk_formdate_range )
                pk_list = df['dataframe'].PrimaryKey.unique()
                csv_report(df, table, self.batch_path)
                print("******************")
        else:
            print("Not implemented.")


    def do_ingest(self,args):
        """ ingest all tables contained across all the dimas inside the dima
        directory. submit arguments to function separated by spaces
        argument order:

            ProjectKey( string ): project key string

            PrimaryKey Date Range (integer): amount of days to divide the FormDate
            field into daterange classes for PrimaryKey creation.

            DevelopmentDB (boolean): True if dataset should be ingested into
            the development database, False if main database.

        example usage:
        "ingest test_project 3 True" - For this example ingestion cycle, use test_project as project key, create primarykey
        with form date divided into 3 day classes, and ingest to development database.

        """
        projectkey, pk_formdate_range, dev_or_not = args.split(" ", 3)
        dev_obj = {
            True:"dimadev",
            False:"dima"
            }
        if any([True for i in os.listdir(dir) if '.xlsx' in os.path.splitext(i)[1]]):
            batch_looper(self.batch_path, projectkey, dev_or_not, pk_formdate_range)
            update_project(self.batch_path, projectkey, dev_obj)
        else:
            print("No project file found within dima directory; unable to ingest.")

    def do_exit(self, args):
        raise SystemExit()

if __name__=="__main__":
    app = main()
    app.cmdloop("Currently available commands: pgtables, ingest, dimatables, report, exit, help.")
