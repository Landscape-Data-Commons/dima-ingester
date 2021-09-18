from src.utils.database_functions import arcno
from src.primarykeys.primary_key_functions import pk_appender
import os
import pandas as pd


class Plots:

    def __init__(self, dimapath):
        self._dimapath = dimapath
        self._table_name = "tblPlots"
        self._join_key = "PlotKey"
        self.raw_table = arcno.MakeTableView(self._table_name, dimapath)
        self.table_pk = self.get_pk(3)

    def get_pk(self, custom_daterange):
        # join to pk source with
        self.pk_source = pk_appender(self._dimapath, custom_daterange)
        return pd.merge(self.raw_table, self.pk_source.loc[:,[self._join_key,'PrimaryKey']], how="inner", on=self._join_key)


class MultiplePlots:

    def __init__(self, dimadir):
        self.tables_dictionary = {f"list_{i}":Plots(os.path.join(dimadir,dimalist[i])).raw_table for i in range(0,len(dimalist))}
        self.raw_tables = pd.concat([j for i,j in self.tables_dictionary.items()],ignore_index=True)

        self.tables_dictionary_pk = {f"list_{i}":Plots(os.path.join(dimadir,dimalist[i])).table_pk for i in range(0,len(dimalist))}
        self.raw_table_pk = pd.concat([j for i,j in self.tables_dictionary_pk.items()],ignore_index=True)
