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
        """tables in dima database or dev database
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
            print("unknoun command")

        # name, cost = args.rsplit(" ", 1)

    def do_ingest(self,args):
        """ ingest projectkey pk_formdate_range development
        """
        projectkey, pk_formdate_range, dev_or_not = args.split(" ", 3)
        print(f"with project key: {projectkey}.")
        print(f"with primary key formdate range of {pk_formdate_range} days.")
        print(f"ingest to development db? (True/False): {dev_or_not}")
        print(self.batch_path)
        # print(Lines(self.dimafiles[1],3).final_df.iloc[:5,:])
        # print(table_operations("tblLines", self.dimafiles[0], 3)['db_name'] )
        # print(looper(self.batch_path, "tblLines", "test", 3))
        # print(table_operations("tblLines", self.dimafiles[0], 3)['operation']() )
        # print(projectkey, pk_formdate_range, dev_or_not)
        batch_looper(self.batch_path, projectkey, dev_or_not, pk_formdate_range)

    def do_exit(self, args):
        raise SystemExit()

if __name__=="__main__":
    app = main()
    app.cmdloop("Enter something")
