from src.utils.database_functions import arcno
from src.primarykeys.primary_key_functions import pk_appender
import os
import pandas as pd


class Lines:

    def __init__(self, dimapath):
        self._dimapath = dimapath
        self._table_name = "tblLines"
        self.raw_table = arcno.MakeTableView("tblLines", dimapath)
        self.lines_pk = self.line_pk(3)

    def line_pk(self, custom_daterange):
        # join to pk source with
        self.pk_source = pk_appender(self._dimapath, custom_daterange)
        return pd.merge(self.raw_table, self.pk_source.loc[:,['PlotKey','PrimaryKey']], how="inner", on="PlotKey")


class MultipleLines:

    def __init__(self, dimadir):
        self.lines_dictionary = {f"list_{i}":Lines(os.path.join(dimadir,dimalist[i])).raw_table for i in range(0,len(dimalist))}
        self.raw_table = pd.concat([j for i,j in self.lines_dictionary.items()],ignore_index=True)














#
