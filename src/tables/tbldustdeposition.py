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
        self.final_df = self.tbl_fixes(self.table_pk).drop_duplicates()

    def get_pk(self, custom_daterange):
        # primary key flow
        self.pk_source = pk_appender_bsne(self._dimapath, custom_daterange)
        if self.pk_source is not None:
            return pd.merge(self.raw_table, self.pk_source.loc[:,[self._join_key,'PrimaryKey']], how="inner", on=self._join_key)
        else:
            return pd.DataFrame(columns=[i for i in self.raw_table.columns])


    def tbl_fixes(self, df):
        return df
