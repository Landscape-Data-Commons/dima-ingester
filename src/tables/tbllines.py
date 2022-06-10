from src.utils.database_functions import arcno
from src.primarykeys.primary_key_functions import pk_appender,form_date_check, pk_appender_bsne
import os
import pandas as pd
import logging


class Lines:
    _table_name = "tblLines"
    _join_key = "LineKey"

    def __init__(self, dimapath, pk_formdate_range):

        self._dimapath = dimapath
        logging.info(f"Extracting the {self._table_name} from the dimafile..")
        self.raw_table = arcno.MakeTableView("tblLines", dimapath)
        logging.info(f"Appending primary key to the {self._table_name}..")
        self.table_pk = self.get_pk(pk_formdate_range)

        self.final_df = self.tbl_fixes(self.table_pk)

    def get_pk(self, custom_daterange):
        tables_with_formdate = form_date_check(self._dimapath)
        if any(tables_with_formdate.values())==False and any([i for i in tables_with_formdate.keys() if "tblBSNE" in i])==True:
            self.pk_source = pk_appender_bsne(
                self._dimapath,
                custom_daterange).drop_duplicates(ignore_index=True)
        else:
            self.pk_source = pk_appender(
                self._dimapath,
                custom_daterange,
                self._table_name).drop_duplicates(ignore_index=True)

        cols = [i for i in self.raw_table.columns if '_x' not in i and '_y' not in i]
        cols.append('PrimaryKey')

        if self.pk_source is not None:
            # return pd.concat([self.raw_table, self.pk_source.loc[:,[self._join_key,'PrimaryKey']]],axis=1, join="inner").loc[:,cols]
            missingpks=  pd.merge(
                            self.raw_table,
                            self.pk_source,
                            suffixes=(None, '_y'),
                            how="left",
                            left_on=["LineKey", "PlotKey"],
                            right_on=["LineKey","PlotKey"]
                        )[cols].drop_duplicates(["LineKey"],
                                                ignore_index=True)
                                                
            return missingpks[~pd.isna(missingpks.PrimaryKey)]
        else:
            return pd.DataFrame(columns=[i for i in self.raw_table.columns])


    def tbl_fixes(self, df):
        df = df.loc[:,~df.columns.duplicated()]
        if self._join_key in self.raw_table.columns:
            if ("888888888" in df[self._join_key].unique()) or ('999999999' in df[self._join_key].unique()):
                return df[(df[self._join_key] != "888888888") & (df[self._join_key] != "999999999")]
            else:
                return df
        return df
















#
