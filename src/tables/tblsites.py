from src.utils.database_functions import arcno
# from src.primarykeys.primary_key_functions import pk_appender
import os
import pandas as pd


class Sites:

    def __init__(self, dimapath):
        self._dimapath = dimapath
        self._table_name = "tblSites"
        self._join_key = "PlotKey"
        self.raw_table = arcno.MakeTableView(self._table_name, dimapath)
        self.table_pk = self.removables(self.raw_table)

    def removables(self, df):
        if "SiteKey" in self.raw_table.columns:
            if ("888888888" in df.SiteKey.unique()) or ("999999999" in df.SiteKey.unique()):
                return df[(df.SiteKey != "888888888") & (df.SiteKey != "999999999")]
            else:
                return df


class MultipleSites:

    def __init__(self, dimadir):
        self.tables_dictionary = {f"list_{i}":PlotNotes(os.path.join(dimadir,dimalist[i])).raw_table for i in range(0,len(dimalist))}
        self.raw_tables = pd.concat([j for i,j in self.tables_dictionary.items()],ignore_index=True)

        # self.tables_dictionary_pk = {f"list_{i}":PlotNotes(os.path.join(dimadir,dimalist[i])).table_pk for i in range(0,len(dimalist))}
        # self.raw_table_pk = pd.concat([j for i,j in self.tables_dictionary_pk.items()],ignore_index=True)
