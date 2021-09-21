import os
from src.utils.database_functions import arcno
import logging
logging.basicConfig(format='%(asctime)s | %(levelname)s: %(message)s', level=logging.NOTSET)


from src.tables import (
    Lines, Plots, LPIHeader, LPIDetail, GapDetail, GapHeader,
    SoilStabilityHeader, SoilStabilityDetail,
    DustDeposition, HorizontalFlux, SoilPits,
    SoilPitHorizons, Sites, Species, SpeciesGeneric,
    PlantProdHeader, PlantProdDetail
    )


def table_operations(tablename, dimapath):
    table_handling = {
        # single tables with primarykey
        "tblLines":{
            "db_name":"tblLines",
            "operation": lambda: Lines(dimapath).final_df
        },
        "tblPlots":{
            "db_name":"tblPlots",
            "operation": lambda: Plots(dimapath).final_df
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
            "operation": lambda: PlantProdHeader(dimapath).final_df
        },
        "tblPlantProdDetail":{
            "db_name":"tblPlantProdDetail",
            "operation": lambda: PlantProdDetail(dimapath).final_df
        },
        "tblSoilPits":{
            "db_name":"tblSoilPits",
            "operation": lambda: SoilPits(dimapath).final_df
        },
        "tblSoilPitHorizons":{
            "db_name":"tblSoilPitHorizons",
            "operation": lambda: SoilPitHorizons(dimapath).final_df
        },
        "tblBSNE_TrapCollection":{
            "db_name":"tblHorizontalFlux",
            "operation": lambda: DustDeposition(dimapath).final_df
        },
        "tblBSNE_BoxCollection":{
            "db_name":"tblDustDeposition",
            "operation": lambda: HorizontalFlux(dimapath).final_df
        },
        "tblGapHeader":{
            "db_name": "tblGapHeader",
            "operation": lambda: GapHeader(dimapath).final_df
        },
        "tblGapDetail":{
            "db_name": "tblGapDetail",
            "operation": lambda: GapDetail(dimapath).final_df
        },
        "tblLPIHeader":{
            "db_name": "tblLPIHeader",
            "operation": lambda:LPIHeader(dimapath).final_df
        },
        "tblLPIDetail":{
            "db_name": "tblLPIDetail",
            "operation": lambda: LPIDetail(dimapath).final_df
        },
    }
    return table_handling.get(tablename)


def looper(path2mdbs, tablename, projk=None, csv=False):
    """
    goes through all the files(.mdb or .accdb extensions) inside a folder,
    create a dataframe of the chosen table using the 'main_translate' function,
    adds the dataframe into a dictionary,a
    finally appends all the dataframes
    and returns the entire appended dataframe
    """
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
            print(i)
            # df = main_translate(tablename, os.path.join(containing_folder,i))
            df = table_operations(tablename, os.path.join(containing_folder,i))['operation']()
            # if its gapheader, this deals with different versions (the fun alt_gapheader_check)
            # if "tblGapHeader" in tablename:
            #     if tablename in arc.actual_list:
            #         df = alt_gapheader_check(df)
            #     else:
            #         df = None
            #
            # if df is not None:
            #     if 'DateLoadedInDB' in df.columns:
            #         df['DateLoadedInDB']=df['DateLoadedInDB'].astype('datetime64')
            #         df['DateLoadedInDB'] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
            #     else:
            #         df['DateLoadedInDB'] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
            #
            #     df['DBKey'] = os.path.split(os.path.splitext(i)[0])[1].replace(" ","")
            #     # df add to dictionary list
            #     df_dictionary[countup] = df.copy()

            else:
                pass
            count+=1
    # return df_dictionary
    if len(df_dictionary)>0:
        final_df = pd.concat([j for i,j in df_dictionary.items()], ignore_index=True).drop_duplicates()
        final_df = dateloadedcheck(final_df)
        # final_df = northing_round(final_df)

        if (tablename == 'tblPlots') and (projk is not None) :
            final_df["ProjectKey"] = projk
        if "tblLines" in tablename:
            for i in final_df.columns:
                if "PrimaryKey" in i:
                    final_df[i] = final_df[i].astype("object")


        return final_df if csv==False else final_df.to_csv(os.path.join(containing_folder,tablename+'.csv'))
    else:
        print(f"table '{tablename}' not found within this dima batch")



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
