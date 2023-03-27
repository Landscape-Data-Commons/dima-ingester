from src.utils.database_functions import arcno
from src.primarykeys.primary_key_functions import pk_appender, get_plotkeys
import os
import pandas as pd
import logging


class GapHeader:
    _table_name = "tblGapHeader"
    _join_key = "LineKey"

    def __init__(self, dimapath, pk_formdate_range):
        self._dimapath = dimapath
        logging.info(f"extracting the {self._table_name} from the {os.path.basename(self._dimapath).replace(' ','')} dimafile..")
        self.raw_table = arcno.MakeTableView(self._table_name, dimapath)
        logging.info(f"Appending primary key to the {self._table_name}..")
        self.table_pk = self.get_pk(pk_formdate_range)
        logging.info("PrimaryKey added.")
        self.final_df = self.tbl_fixes(self.table_pk)


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
            # return pd.merge(
            #     self.raw_table,
            #     self.pk_source,
            #     suffixes=(None, '_y'),
            #     how="inner", on=self._join_key)[cols]
            return pd.merge(
                self.raw_table,
                self.pk_source,
                how="left",
                suffixes=(None, '_y'),
                left_on=["LineKey", "RecKey"],
                right_on=["LineKey", "RecKey"])[cols]
        else:
            return pd.DataFrame(columns=[i for i in self.raw_table.columns])


    def tbl_fixes(self, df):
        df = alt_gapheader_check(df)
        df = df.loc[:,~df.columns.duplicated()]
        return df

def alt_gapheader_check(dataframe):
    for i in dataframe.columns:
        if "PerennialsCanopy" in i:
            return dataframe
        elif "Perennials" in i:
            df = dataframe.copy()
            df.rename(columns={
                "Perennials":"PerennialsCanopy",
                "AnnualGrasses":"AnnualGrassesCanopy",
                "AnnualForbs":"AnnualForbsCanopy",
                "Other":"OtherCanopy"},
                inplace=True)
            df["PerennialsBasal"] = pd.NA
            df["AnnualGrassesBasal"] = pd.NA
            df["AnnualForbsBasal"] = pd.NA
            df["OtherBasal"] = pd.NA

            return df
