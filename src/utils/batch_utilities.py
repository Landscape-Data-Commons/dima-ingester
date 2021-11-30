import os
import pandas as pd
from datetime import datetime
from src.utils.database_functions import arcno, db
from src.utils.utility_functions import tablecheck, table_create
from src.utils.ingester import Ingester
import logging

from src.tables import (
    Lines, Plots, LPIHeader, LPIDetail, GapDetail, GapHeader,
    SoilStabilityHeader, SoilStabilityDetail,
    DustDeposition, HorizontalFlux, SoilPits,
    SoilPitHorizons, Sites, Species, SpeciesGeneric,
    PlantProdHeader, PlantProdDetail, PlotHistory
    )

def table_operations(tablename, dimapath, pk_formdate_range):
    """ handles which table-creating functions to call
    depending on which tablename you supply as argument.

    logic behind this is to make building composite tables
    which dont exist in dimas (such as dustdeposition and horizontal flux),
    while not considering their source tables (like bsne_box etc.) which
    are not to be ingested as they are.
    """
    pk_formdate_range = int(pk_formdate_range)
    table_handling = {
        # single tables with primarykey
        "tblLines":{
            "db_name":"tblLines",
            "operation": lambda: Lines(dimapath, pk_formdate_range).final_df
        },
        "tblPlots":{
            "db_name":"tblPlots",
            "operation": lambda: Plots(dimapath, pk_formdate_range).final_df
        },
        # no primarykey
        "tblSites":{
            "db_name":"tblSites",
            "operation": lambda: Sites(dimapath).final_df
        },
        "tblSpecies":{
            "db_name":"tblSpecies",
            "operation": lambda: Species(dimapath).final_df
        },
        "tblSpeciesGeneric":{
            "db_name" : "tblSpeciesGeneric",
            "operation": lambda: SpeciesGeneric(dimapath).final_df
        },
        "tblPlotNotes":{
            "db_name":"tblPlotNotes",
            "operation": lambda: PlotNotes(dimapath).final_df
        },
        # more complex primarykey tables
        "tblPlantProdHeader":{
            "db_name":"tblPlantProdHeader",
            "operation": lambda: PlantProdHeader(dimapath, pk_formdate_range).final_df
        },
        "tblPlantProdDetail":{
            "db_name":"tblPlantProdDetail",
            "operation": lambda: PlantProdDetail(dimapath, pk_formdate_range).final_df
        },
        "tblSoilPits":{
            "db_name":"tblSoilPits",
            "operation": lambda: SoilPits(dimapath, pk_formdate_range).final_df
        },
        "tblSoilPitHorizons":{
            "db_name":"tblSoilPitHorizons",
            "operation": lambda: SoilPitHorizons(dimapath, pk_formdate_range).final_df
        },
        "tblBSNE_TrapCollection":{
            "db_name":"tblDustDeposition",
            "operation": lambda: DustDeposition(dimapath, pk_formdate_range).final_df
        },
        "tblBSNE_BoxCollection":{
            "db_name":"tblhorizontalflux",
            "operation": lambda: HorizontalFlux(dimapath, pk_formdate_range).final_df
        },
        "tblGapHeader":{
            "db_name": "tblGapHeader",
            "operation": lambda: GapHeader(dimapath, pk_formdate_range).final_df
        },
        "tblGapDetail":{
            "db_name": "tblGapDetail",
            "operation": lambda: GapDetail(dimapath, pk_formdate_range).final_df
        },
        "tblLPIHeader":{
            "db_name": "tblLPIHeader",
            "operation": lambda:LPIHeader(dimapath, pk_formdate_range).final_df
        },
        "tblLPIDetail":{
            "db_name": "tblLPIDetail",
            "operation": lambda: LPIDetail(dimapath, pk_formdate_range).final_df
        },
        "tblPlotHistory":{
            "db_name": "tblPlotHistory",
            "operation": lambda: PlotHistory(dimapath).final_df
        },
    }
    return table_handling.get(tablename)

def looper(path2mdbs, tablename, projk=None, pk_formdate_range=None):
    """
    goes through all the files(.mdb or .accdb extensions) inside a folder,
    create a dataframe of the chosen table using the 'main_translate' function,
    adds the dataframe into a dictionary,a
    finally appends all the dataframes
    and returns the entire appended dataframe
    """
    # if pk_formdate_range is not None:
    #     pk_formdate_range = int(pk_formdate_range)

    containing_folder = path2mdbs
    contained_files = os.listdir(containing_folder)
    df_dictionary={}

    count = 1
    basestring = 'file_'

    for i in contained_files:
        if os.path.splitext(os.path.join(containing_folder,i))[1]=='.mdb' or os.path.splitext(os.path.join(containing_folder,i))[1]=='.accdb':
            countup = basestring+str(count)
            # df creation/manipulation starts here
            arc = arcno(os.path.join(containing_folder,i))
            if tablename not in arc.actual_list:
                logging.info(f"table {tablename} not found within '{i}'")
            else:
                tbl = table_operations(tablename, os.path.join(containing_folder,i), pk_formdate_range)['db_name']
                df = table_operations(tablename, os.path.join(containing_folder,i), pk_formdate_range)['operation']()
                # fix for extrafield in tblLPIHeader
                df = problem_fields(df,tbl)

                df = dateloaded_dbkey(df, i)

                if df.size>0:
                    df_dictionary[countup] = df
                else:
                    df_dictionary[countup] = None
                count+=1

    # return df_dictionary
    if len(df_dictionary)>0:
        final_df = pd.concat([j for i,j in df_dictionary.items()], ignore_index=True).drop_duplicates().reset_index(drop=True)

        if (tablename == 'tblPlots') and (projk is not None) :
            final_df["ProjectKey"] = projk
        obj = {
            'db_name':tbl,
            'dataframe':final_df
        }

        return obj
    else:
        logging.info(f"table '{tablename}' not found within this dima batch")

def batch_looper(dimacontainer, projkey=None, dev=False, pk_formdate_range=None):

    """
    addition
    creates an exhaustive list of tables across all dimas in a folder
    and then uses looper to gothrough the list of tables and ingest them
    """
    if dev==False:
        d = db('dima')
        keyword = "dima"
    else:
        d = db("dimadev")
        keyword = "dimadev"

    tablelist = None
    while tablelist is None:
        tablelist = table_collector(dimacontainer)
        logging.info(f"list of tables across current dimafiles: {[i for i in tablelist]}")
    else:
        for table in tablelist:

            obj = looper(dimacontainer,table, projkey, pk_formdate_range)
            df = obj['dataframe']
            if tablecheck(obj['db_name'], keyword):
                logging.info(f"table '{table}' found; ingesting..")

                Ingester.main_ingest(df, obj['db_name'], d.str, 10000)
            else:
                logging.info(f"table '{table}' not found; creating table and ingesting..")
                table_create(df, obj['db_name'], keyword)

                Ingester.main_ingest(df, obj['db_name'], d.str, 10000)



def dateloaded_dbkey(df, filename):
    """ appends DateLoadedInDB and dbkey to the dataframe
    """
    if 'DateLoadedInDB' in df.columns:
        df['DateLoadedInDB'] = df['DateLoadedInDB'].astype('datetime64')
        df['DateLoadedInDB'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    else:
        df['DateLoadedInDB'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df['DBKey'] = os.path.split(os.path.splitext(filename)[0])[1].replace(" ","")
    return df


def table_collector(path2mdbs):
    """
    returns a list of all tables present in a folder of dimas
    because dimas may each have a different set of tables, this function
    goes through the list of tables per dima and appends any table not previously
    seen into an internal list which is ultimately returned.
    """
    # containing_folder = path2mdbs
    contained_files = os.listdir(path2mdbs) if os.path.isdir(path2mdbs) else [path2mdbs]
    table_list = []
    for mdb_path in contained_files:
        if os.path.splitext(mdb_path)[1]=='.mdb' or os.path.splitext(mdb_path)[1]=='.accdb':
            pth = os.path.join(path2mdbs,mdb_path) if len(contained_files)>1 else os.path.join(path2mdbs,mdb_path)
            instance = arcno(pth)
            for tablename, size in instance.actual_list.items():
                if tablename not in table_list:
                    table_list.append(tablename)
    if "tblBSNE_Stack" in table_list:
        table_list.remove("tblBSNE_Stack")
    if "tblBSNE_Box" in table_list:
        table_list.remove("tblBSNE_Box")
    return table_list

def problem_fields(df, tblname):

    if "tblLPIHeader" in tblname:
        if "EveryNth_num" in df.columns:
            df = df.drop(columns="EveryNth_num", inplace=False).copy()
            return df
    else:
        return df
