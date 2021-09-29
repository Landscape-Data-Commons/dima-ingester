from src.utils.database_functions import arcno
from src.primarykeys.primary_key_functions import pk_appender_soil
import os
import pandas as pd
import logging

class SoilPitHorizons:
    _table_name = "tblSoilPitHorizons"
    _join_key = "HorizonKey"

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
        self.pk_source = pk_appender_soil(self._dimapath, custom_daterange)
        if self.pk_source is not None:
            return pd.merge(self.raw_table, self.pk_source.loc[:,[self._join_key,'PrimaryKey']], how="inner", on=self._join_key)
        else:
            return pd.DataFrame(columns=[i for i in self.raw_table.columns])


    def tbl_fixes(self, df):
        return df
