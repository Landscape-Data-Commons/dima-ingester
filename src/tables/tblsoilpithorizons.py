from src.utils.database_functions import arcno
from src.primarykeys.primary_key_functions import pk_appender_soil,form_date_check, pk_appender_bsne
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
        tables_with_formdate = form_date_check(self._dimapath)
        if any(tables_with_formdate.values())==False and any([i for i in tables_with_formdate.keys() if "tblBSNE" in i])==True:
            self.pk_source = pk_appender_bsne(self._dimapath, custom_daterange)
        else:
            self.pk_source = pk_appender_soil(self._dimapath, custom_daterange)

        cols = [i for i in self.raw_table.columns if '_x' not in i and '_y' not in i]
        cols.append('PrimaryKey')

        if self.pk_source is not None:
            return pd.concat([self.raw_table, self.pk_source.loc[:,[self._join_key,'PrimaryKey']]],axis=1, join="inner").loc[:,cols]
        else:
            return pd.DataFrame(columns=[i for i in self.raw_table.columns])


    def tbl_fixes(self, df):
        df = df.loc[:,~df.columns.duplicated()]
        return df
