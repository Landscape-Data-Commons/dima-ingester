from src.utils.database_functions import arcno
from src.primarykeys.primary_key_functions import dust_deposition_raw, \
pk_appender_bsne
import os
import pandas as pd
import logging

class DustDeposition:
    def __init__(self, dimapath):
        self._dimapath = dimapath
        self._table_name = "tblDustDeposition"
        self._join_key = "BoxID"
        logging.info(f"Extracting the {self._table_name} from the dimafile..")

        self.raw_table = dust_deposition_raw(self._dimapath)
        logging.info(f"Appending primary key to the {self._table_name}..")
        self.table_pk = self.get_pk(3)
        logging.info("PrimaryKey added.")

    def get_pk(self, custom_daterange):
        # primary key flow
        self.pk_source = pk_appender_bsne(self._dimapath, custom_daterange)
        return pd.merge(self.raw_table, self.pk_source.loc[:,[self._join_key,'PrimaryKey']], how="inner", on=self._join_key)



class MultipleDustDeposition:

    def __init__(self, dimadir):
        self.tables_dictionary = {f"list_{i}":Plots(os.path.join(dimadir,dimalist[i])).raw_table for i in range(0,len(dimalist))}
        self.raw_tables = pd.concat([j for i,j in self.tables_dictionary.items()],ignore_index=True)

        self.tables_dictionary_pk = {f"list_{i}":Plots(os.path.join(dimadir,dimalist[i])).table_pk for i in range(0,len(dimalist))}
        self.raw_table_pk = pd.concat([j for i,j in self.tables_dictionary_pk.items()],ignore_index=True)
