import os, os.path, sys
from cmd import Cmd
import logging
from src.project.project import all_dimas, update_project
from src.utils.batch_utilities import batch_looper
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
        """tables currently in dima database or dev database
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

    def do_ingest(self,args):
        """ ingest all tables contained across all the dimas inside the dima
        directory. submit arguments to function separated by spaces
        argument order: ProjectKey( string ) PrimaryKey Date Range (integer) DevelopmentDB (boolean)
        example usage:
        "ingest test_project 3 True"
        For this ingestion cycle, use test_project as project key, create primarykey
        with form date divided into 3 day classes, and ingest to development database.

        """
        projectkey, pk_formdate_range, dev_or_not = args.split(" ", 3)

        batch_looper(self.batch_path, projectkey, dev_or_not, pk_formdate_range)

    def do_exit(self, args):
        raise SystemExit()

if __name__=="__main__":
    app = main()
    app.cmdloop("Currently available commands: pgtables, ingest, exit, help.")
