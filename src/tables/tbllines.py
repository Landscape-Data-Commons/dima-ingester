from src.utils.database_functions import arcno
import os
import pandas as pd

# no primary key: tblLines, tblPlots, tblSpecies
no_primary_key = ['tblPlots', 'tblLines', 'tblSpecies','tblSpeciesGeneric',\
                      'tblSites','tblPlotNotes', 'tblSites']


p = r"C:\Users\kbonefont\Downloads\dimas\DONE MCC Mongolia Fall Production 2017 DIMA 5.5a as of 2020-06-26.mdb"
dimadir = os.path.dirname(p)
dimalist = os.listdir(dimadir)
p1 = os.path.join(dimadir,dimalist[0])
p2 = os.path.join(dimadir,dimalist[1])

pp







class Lines:

    def __init__(self, dimapath):
        self._dimapath = dimapath
        self._table_name = "tblLines"
        self.raw_table = arcno.MakeTableView("tblLines", dimapath)

    def lines_table_fixes(self):
        if 'DateLoaded'

class MultipleLines:

    def __init__(self, dimadir):
        self.lines_dictionary = {f"list_{i}":Lines(os.path.join(dimadir,dimalist[i])).raw_table for i in range(0,len(dimalist))}
        self.raw_table = pd.concat([j for i,j in self.lines_dictionary.items()],ignore_index=True)

MultipleLines(dimadir).raw_table.drop_duplicates













#
