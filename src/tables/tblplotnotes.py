from src.utils.database_functions import arcno
from src.primarykeys.primary_key_functions import pk_appender
import os
import pandas as pd
import logging


class PlotNotes:
    _table_name = "tblPlotNotes"
    _join_key = "PlotKey"

    def __init__(self, dimapath):
        self._dimapath = dimapath
        logging.info(f"extracting the {self._table_name} from the {os.path.basename(self._dimapath).replace(' ','')} dimafile..")
        self.raw_table = arcno.MakeTableView(self._table_name, dimapath)
        # logging.info("PrimaryKey added.")
        self.final_df = self.tbl_fixes(self.raw_table).drop_duplicates()


    def tbl_fixes(self, df):
        df = df.loc[:,~df.columns.duplicated()]
        return df
