from src.utils.database_functions import arcno
from src.primarykeys.primary_key_functions import pk_appender, get_plotkeys
import os
import pandas as pd
import logging


class InfiltrationHeader:
    _table_name = "tblInfiltrationHeader"
    # _join_key = "LineKey"

    def __init__(self, dimapath, pk_formdate_range):
        self._dimapath = dimapath

        logging.info(f"Extracting the {self._table_name} from the dimafile..")
        self.raw_table = arcno.MakeTableView(self._table_name, dimapath)
        logging.info(f"Appending primary key to the {self._table_name}..")
        self.table_pk = self.get_pk(pk_formdate_range)
        logging.info("PrimaryKey added.")
        self.final_df = self.tbl_fixes(self.table_pk).drop_duplicates()

    def get_pk(self, custom_daterange):
        # primary key flow
        self.pk_source = pk_appender(
            self._dimapath,
            custom_daterange,
            self._table_name).drop_duplicates(ignore_index=True)

        cols = [i for i in self.raw_table.columns if '_x' not in i and '_y' not in i]
        cols.append('PrimaryKey')

        if self.pk_source is not None:
            # return pd.concat([self.raw_table, self.pk_source.loc[:,[self._join_key,'PrimaryKey']]],axis=1, join="inner").loc[:,cols]
            return pd.merge(
                self.raw_table,
                self.pk_source,
                suffixes=(None, '_y'),
                how="left",
                left_on=["PlotKey","RecKey"],
                right_on=["PlotKey","RecKey"]
            )[cols].drop_duplicates(["PlotKey", "RecKey"])
        else:
            return pd.DataFrame(columns=[i for i in self.raw_table.columns])


    def tbl_fixes(self, df):
        df = df.loc[:,~df.columns.duplicated()]
        return df
