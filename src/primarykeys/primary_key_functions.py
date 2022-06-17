from src.utils.database_functions import arcno
from src.utils.utility_functions import Timer
import pandas as pd
import re
import logging
from datetime import datetime, timedelta

"""
Primary key strategy:

1. find a table with a formdate  = formDateCheck
2. create a header/detail dataframe with formdate
3. create a new formdate field w custom daterange classes
4. create primary key from plot+ new formdate, return pk df
   to whatever function needs it

"""


@Timer(location="*** pk_appender ***")
def pk_appender(dimapath, date_range, tablename = None):
    """ create header/detail dataframe with the new formdate,
    then return a dataframe with a primary key made from
    plotkey + formdate

    works for plots, lines
    """
    arc = arcno()

    tables_with_formdate = form_date_check(dimapath) # returns dictionary

    #
    # if (tablename is not None) and ("tblLines" not in tablename or "tblPlots" not in tablename) :
    if (tablename is not None):
        tables_with_formdate= formdate_correction(tables_with_formdate, tablename)


    if any(tables_with_formdate.values()):
        header_detail_df = header_detail(tables_with_formdate, dimapath) # returns
                           # dataframe with old formdate
        new_formdate_df = new_form_date(header_detail_df, date_range)
                          # dataframe with new formdate range
        # all big tables need plotkey, which comes from lines+plot join

        line_plot = get_plotkeys(dimapath)

        if "SoilStab" in tablename or "DK" in tablename or "Infiltration" in tablename:
            full_join = pd.merge(new_formdate_df, line_plot, how="inner", on="PlotKey").drop_duplicates(["PlotKey", "RecKey"], ignore_index=True)
        elif "Plots" in tablename:
            full_join = pd.merge(new_formdate_df, line_plot, how="inner", on="LineKey").drop_duplicates(["PlotKey", "RecKey"],ignore_index=True)
        else:
            full_join = pd.merge(new_formdate_df, line_plot, how="inner", on="LineKey").drop_duplicates(["LineKey", "RecKey"], ignore_index=True)

        # if 'PlotKey_x' in full_join.columns:
        #     full_join.drop(['PlotKey_x'], axis=1, inplace=True)
        #     full_join.rename(columns={'PlotKey_y':"PlotKey"}, inplace=True)
        full_join["FormDatePK"] = full_join.FormDatePK.apply(lambda x: pd.Timestamp(x).date())
        final_df = arc.CalculateField(full_join,"PrimaryKey", "PlotKey", "FormDatePK")

        return final_df
    else:
        pass

def formdate_correction(obj, tablename = None):
    obj_copy = obj.copy()
    # default:
    st = f"{tablename}".split('Detail')[0]
    # special cases:
    if "Detail" in tablename:
        st = f"{tablename}".split('Detail')[0]
    elif "PlantDenQuads" in tablename:
        st = f"{tablename}".split('Quads')[0]
    elif "PlantDenSpecies" in tablename:
        st = f"{tablename}".split('Species')[0]
    elif "SpecRichDetail" in tablename:
        st = f"{tablename}".split('Detail')[0]
    elif "SoilPit" in tablename:
        st = "tblLPIHeader"
    elif "tblLines" in tablename:
        st = "tblLPIHeader"
    elif "Plots" in tablename or "Lines" in tablename:
        st = "tblLPIHeader"
    for i in obj_copy.keys():
        if obj_copy[i] is True:
            if st in i:
                obj_copy[i] = True
            else:
                obj_copy[i] = False
        else:
            pass
    return obj_copy
@Timer(location="*** BSNE_appender ***")
def pk_appender_bsne(dimapath, date_range):
    """ create header/detail dataframe with the new formdate,
    then return a dataframe with a primary key made from
    plotkey + collectdate

    works for dustdepostion tables like bsne_box, tblBSNE_Stack, tblBSNE_BoxCollection
    """
    arc = arcno(dimapath)
    # array_to_return = ["StackID","PlotKey", "PrimaryKey"] if "tblBSNE_TrapCollection" in arc.actual_list else ["BoxID","PlotKey", "PrimaryKey"]
    raw_bsne = dust_deposition_raw(dimapath) if "tblBSNE_TrapCollection" in arc.actual_list else horizontalflux_raw(dimapath)
    # raw_bsne = dust_deposition_raw(dimapath)
    new_formdate_df = new_form_date(raw_bsne, date_range)

    if 'PlotKey_x' in new_formdate_df.columns:
        new_formdate_df.drop(['PlotKey_x'], axis=1, inplace=True)
        new_formdate_df.rename(columns={'PlotKey_y':"PlotKey"}, inplace=True)
    new_formdate_df["collectDatePK"] = new_formdate_df.collectDatePK.apply(lambda x: pd.Timestamp(x).date())
    final_df = arc.CalculateField(new_formdate_df,"PrimaryKey", "PlotKey", "collectDatePK")

    return final_df

@Timer(location="*** soil_appender ***")
def pk_appender_soil(dimapath, date_range, tablename = None):
    arc = arcno()

    soilpk = soil_pits_raw(dimapath)
    new_formdate_df = new_form_date(soilpk, date_range)
    new_formdate_df.DateRecordedPK = new_formdate_df.DateRecordedPK.apply(lambda x: pd.Timestamp(x).date())

    final_df = arc.CalculateField(new_formdate_df,"PrimaryKey", "PlotKey", "DateRecordedPK")
    return final_df
@Timer(location=" *** Soilpits raw ***")
def soil_pits_raw(dimapath):
    logging.info("Creating raw table from soilpits and soilpithorizons..")
    horizons = arcno.MakeTableView("tblSoilPitHorizons",dimapath)
    pits = arcno.MakeTableView("tblSoilPits", dimapath)

    if (horizons.size>0) & (pits.size>0):
        # if both exist
        return pd.merge(pits,horizons, how="inner", on="SoilKey")
    elif (horizons.size>0) & (pits.size<=0):
        # if only horizons exist
        return horizons
    elif (horizons.size<=0) & (pits.size>0):
        # if only pits exist
        return pits
    else:
        # if neither exists
        pass
@Timer(location="*** dust_depo raw ***")
def dust_deposition_raw(dimapath):
    logging.info("Creating raw table from BSNE tblBSNE_TrapCollection and Stack")
    ddt = arcno.MakeTableView("tblBSNE_TrapCollection",dimapath)
    stack = arcno.MakeTableView("tblBSNE_Stack", dimapath)
    df = pd.merge(stack,ddt, how="inner", on="StackID")
    logging.info("raw bsne table done.")
    return df

@Timer(location="*** horflux raw ***")
def horizontalflux_raw(dimapath):
    logging.info("Creating raw table from BSNE Box, Box Collection and Stack")
    box = arcno.MakeTableView("tblBSNE_Box",dimapath)
    stack = arcno.MakeTableView("tblBSNE_Stack", dimapath)
    boxcol = arcno.MakeTableView('tblBSNE_BoxCollection', dimapath)

    plotted_boxes = pd.merge(box,stack, how="inner", on="StackID")
    collected_boxes = pd.merge(plotted_boxes,boxcol, how="inner", on="BoxID")
    logging.info("raw bsne table done.")
    return collected_boxes

def form_date_check(dimapath):
    """ returns dictionary with all the
    tables in dima file as keys, and
    booleans to describe if the table
    has a formdate field or not.
    """
    obj = dict()
    arc = arcno(dimapath)
    for i in arc.actual_list:
        df = arcno.MakeTableView(i,dimapath)
        if 'FormDate' in df.columns:
            obj[i] = True
        else:
            obj[i] = False
    return obj


def date_grp(target_date, formdate_df, window_size):
    """ given a daterange size (date_spread),
    de formdate field will be broken down into
    date classes.
    given a target_date, the function will
    return which custom date class it belongs to.
    """
    # logging.info("gathering dates from table..")
    if isinstance(target_date,str):
        target_date_ts = datetime.strptime(target_date, '%Y-%m-%d %H:%M:%S')
    else:
        target_date_ts = target_date.to_pydatetime().date()
    try:

        if "FormDate" in formdate_df.columns:
            lst = formdate_df.FormDate.unique()

        elif "collectDate" in formdate_df.columns:
            lst = formdate_df.collectDate.unique()

        else:
            lst = formdate_df.DateRecorded.unique()

    except Exception as e:
        logging.error(e)

    finally:
        for i in lst:

            start=pd.to_datetime(i)-timedelta(days=window_size)
            end=pd.to_datetime(i)+timedelta(days=window_size)
            if start<=target_date_ts<=end:
                return i
            else:
                pass


# def within(date, range):
#     start = range[0]
#     end =  range[len(range)-1]
#     if start<= pd.to_datetime(date) <=end:
#         return True
#     else:
#         return False
@Timer(location="*** date_grp column ***")
def new_form_date(old_formdate_dataframe, window_size):
    """ given a dataframe with a formdate field,
    returns a dataframe with a new formdate field with custom daterange classes
    for primarykey creation
    """
    logging.info("inferring field to apply custom daterange from dataframe..")

    try:
        if "FormDate" in old_formdate_dataframe.columns:
            which_field = 'FormDatePK'
            which_field_original = 'FormDate'
        elif "collectDate" in old_formdate_dataframe.columns:
            which_field = 'collectDatePK'
            which_field_original = 'collectDate'
        elif "DateRecorded" in old_formdate_dataframe.columns:
            which_field = "DateRecordedPK"
            which_field_original = "DateRecorded"

    except Exception as e:
        logging.error("no usable daterange field found in dataframe!")
    finally:

        old_formdate_dataframe[which_field] = old_formdate_dataframe[which_field_original].apply(
            lambda x: date_grp(x, old_formdate_dataframe,int(window_size))
        )
        logging.info("dataframe with custom daterange done.")
        return old_formdate_dataframe

@Timer(location="*** pk_appender ***")
def header_detail(formdate_dictionary, dimapath):
    """ create a header/detail dataframe with with a formdate dataframe
    """
    pattern_to_remove = re.compile(r'(tbl)|(Header)')
    for key in formdate_dictionary.keys():
        if formdate_dictionary[key]==True:
            simple_table_name = pattern_to_remove.sub('',key)
            header = arcno.MakeTableView(f'tbl{simple_table_name}Header', dimapath)
            detail = arcno.MakeTableView(f'tbl{simple_table_name}Detail', dimapath)
            # this conditional handles dimas with missing details table
            if detail.shape[0]>1:
                df = pd.merge(header,detail, how="inner", on="RecKey")
                # df = header
                return df
            else:
                return header
        else:
            pass


@Timer(location="*** getting plotkeys ***")
def get_plotkeys(dimapath):
    line = arcno.MakeTableView("tblLines", dimapath)
    plots = arcno.MakeTableView("tblPlots", dimapath)
    line_plot = pd.merge(line, plots, how="inner", on="PlotKey")
    return line_plot
