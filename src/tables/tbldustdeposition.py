from src.utils.database_functions import arcno
from src.primarykeys.primary_key_functions import dust_deposition_raw, \
pk_appender_bsne
import os
import pandas as pd
import logging

class DustDeposition:
    _table_name = "tblDustDeposition"
    _join_key = "StackID"

    def __init__(self, dimapath, pk_formdate_range):
        self._dimapath = dimapath
        logging.info(f"Extracting the {self._table_name} from the dimafile..")
        self.raw_table = dust_deposition_raw(self._dimapath)
        logging.info(f"Appending primary key to the {self._table_name}..")
        self.table_pk = self.get_pk(pk_formdate_range)
        logging.info("PrimaryKey added.")
        self.final_df = self.tbl_fixes(self.table_pk)


    def get_pk(self, custom_daterange):
        # primary key flow
        self.pk_source = pk_appender_bsne(self._dimapath,
            custom_daterange).drop_duplicates(ignore_index=True)

        cols = [i for i in self.raw_table.columns if '_x' not in i and '_y' not in i]
        cols.append('PrimaryKey')

        if self.pk_source is not None:
            # return pd.concat([self.raw_table, self.pk_source.loc[:,[self._join_key,'PrimaryKey']]],axis=1, join="inner").loc[:,cols]
            return pd.merge(
                self.raw_table,
                self.pk_source,
                suffixes=(None, '_y'),
                how="left",
                left_on=["StackID","collectDate"],
                right_on=["StackID","collectDate"]
            )[cols].drop_duplicates(["StackID","RecKey"], ignore_index=True)
        else:
            return pd.DataFrame(columns=[i for i in self.raw_table.columns])



    def tbl_fixes(self, df):
        df = df.loc[:,~df.columns.duplicated()]
        return df
