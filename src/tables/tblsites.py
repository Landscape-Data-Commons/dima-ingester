from src.utils.database_functions import arcno
# from src.primarykeys.primary_key_functions import pk_appender
import os
import pandas as pd
import logging


class Sites:
    _table_name = "tblSites"
    _join_key = "PlotKey"

    def __init__(self, dimapath):
        self._dimapath = dimapath
        logging.info(f"extracting the {self._table_name} from the {os.path.basename(self._dimapath).replace(' ','')} dimafile..")
        self.raw_table = arcno.MakeTableView(self._table_name, dimapath)
        logging.info(f"Appending primary key to the {self._table_name}..")
        self.table_pk = self.removables(self.raw_table)
        self.final_df = self.tbl_fixes(self.table_pk).drop_duplicates()

    def removables(self, df):
        if "SiteKey" in self.raw_table.columns:
            if ("888888888" in df.SiteKey.unique()) or ("999999999" in df.SiteKey.unique()):
                return df[(df.SiteKey != "888888888") & (df.SiteKey != "999999999")]
            else:
                return df

    def tbl_fixes(self, df):
        df = df.loc[:,~df.columns.duplicated()]
        return df
