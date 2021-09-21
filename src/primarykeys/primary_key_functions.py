from src.utils.database_functions import arcno
import pandas as pd
import re
import logging

"""
Primary key strategy:

1. find a table with a formdate  = formDateCheck
2. create a header/detail dataframe with formdate
3. create a new formdate field w custom daterange classes
4. create primary key from plot+ new formdate, return pk df
   to whatever function needs it

"""



def pk_appender(dimapath, date_range):
    """ create header/detail dataframe with the new formdate,
    then return a dataframe with a primary key made from
    plotkey + formdate

    works for plots, lines
    """
    arc = arcno()
    tables_with_formdate = form_date_check(dimapath) # returns dictionary
    header_detail_df = header_detail(tables_with_formdate, dimapath) # returns
                       # dataframe with old formdate
    new_formdate_df = new_form_date(header_detail_df, date_range) # returns
                      # dataframe with new formdate range

    # all big tables need plotkey, which comes from lines+plot join

    line_plot = get_plotkeys(dimapath)
    full_join = pd.merge(new_formdate_df, line_plot, how="inner", on="LineKey")
    if 'PlotKey_x' in full_join.columns:
        full_join.drop(['PlotKey_x'], axis=1, inplace=True)
        full_join.rename(columns={'PlotKey_y':"PlotKey"}, inplace=True)
    final_df = arc.CalculateField(full_join,"PrimaryKey", "PlotKey", "FormDatePK")

    return final_df.loc[:,["LineKey","RecKey","PlotKey", "PrimaryKey"]]

def pk_appender_bsne(dimapath, date_range):
    """ create header/detail dataframe with the new formdate,
    then return a dataframe with a primary key made from
    plotkey + collectdate

    works for dustdepostion tables like bsne_box, tblBSNE_Stack, tblBSNE_BoxCollection
    """
    arc = arcno(dimapath)
    array_to_return = ["StackID","PlotKey", "PrimaryKey"] if "tblBSNE_TrapCollection" in arc.actual_list else ["BoxID","PlotKey", "PrimaryKey"]
    raw_bsne = horizontalflux_raw(dimapath) if "tblBSNE_TrapCollection" in arc.actual_list else dust_deposition_raw(dimapath)
    # raw_bsne = dust_deposition_raw(dimapath)
    new_formdate_df = new_form_date(raw_bsne, date_range)

    if 'PlotKey_x' in new_formdate_df.columns:
        new_formdate_df.drop(['PlotKey_x'], axis=1, inplace=True)
        new_formdate_df.rename(columns={'PlotKey_y':"PlotKey"}, inplace=True)
    final_df = arc.CalculateField(new_formdate_df,"PrimaryKey", "PlotKey", "collectDatePK")

    return final_df.loc[:,array_to_return]


def dust_deposition_raw(dimapath):
    logging.info("Creating raw table from BSNE Box, Box Collection and Stack")
    box = arcno.MakeTableView("tblBSNE_Box",dimapath)
    stack = arcno.MakeTableView("tblBSNE_Stack", dimapath)
    boxcol = arcno.MakeTableView('tblBSNE_BoxCollection', dimapath)

    plotted_boxes = pd.merge(box,stack, how="inner", on="StackID")
    collected_boxes = pd.merge(plotted_boxes,boxcol, how="inner", on="BoxID")
    logging.info("raw bsne table done.")
    return collected_boxes

def horizontalflux_raw(dimapath):
    logging.info("Creating raw table from BSNE tblBSNE_TrapCollection and Stack")
    ddt = arcno.MakeTableView("tblBSNE_TrapCollection",dimapath)
    stack = arcno.MakeTableView("tblBSNE_Stack", dimapath)
    df = pd.merge(stack,ddt, how="inner", on="StackID")
    logging.info("raw bsne table done.")
    return df


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

def date_grp(target_date, formdate_df, date_spread):
    """ given a daterange size (date_spread),
    de formdate field will be broken down into
    date classes.
    given a target_date, the function will
    return which custom date class it belongs to.

    some dates may be out of the custom classes created if the class division:
    3 day division =  01-01-00 (from day 01 to 03), 01-03-00 (from day 03 to 06)
    and 01-06-00 (from day 06 to 09) does not include target_date = 10

    the else statement includes any remnant numbers on that extreme of
    date_range inside the last class.

    """
    try:
        logging.info("gathering dates from table..")
        if "FormDate" in formdate_df.columns:
            lst = formdate_df.FormDate.unique()
        else:
            lst = formdate_df.collectDate.unique()

    except Exception as e:
        logging.error(e)

    finally:
        date_range = pd.date_range(start=lst.min(), end=lst.max(), freq=f'{date_spread}D')
        for i in range(0,len(date_range.tolist())):
            if i < len(date_range)-1:
                if target_date in pd.date_range(start=date_range[i], end=date_range[i+1]):
                    return date_range[i]
            else:
                which_max=formdate_df.FormDate.max() if "FormDate" in formdate_df.columns else formdate_df.collectDate.max()
                if target_date in pd.date_range(start=date_range[i], end=which_max):
                    return date_range[i]

def new_form_date(old_formdate_dataframe, custom_daterange):
    """ given a dataframe with a formdate field,
    returns a dataframe with a new formdate field with custom daterange classes
    for primarykey creation
    """
    try:
        logging.info("inferring field to apply custom daterange from dataframe..")
        if "FormDate" in old_formdate_dataframe.columns:
            which_field = 'FormDatePK'
            which_field_original = 'FormDate'
        elif "collectDate" in old_formdate_dataframe.columns:
            which_field = 'collectDatePK'
            which_field_original = 'collectDate'
    except Exception as e:
        logging.error("no usable daterange field found in dataframe!")
    finally:

        old_formdate_dataframe[which_field] = old_formdate_dataframe.apply(
            lambda x: date_grp(x[which_field_original], old_formdate_dataframe,custom_daterange),
            axis=1
        )
        logging.info("dataframe with custom daterange done.")
        return old_formdate_dataframe

def header_detail(formdate_dictionary, dimapath):
    """ create a header/detail dataframe with with a formdate dataframe
    """
    pattern_to_remove = re.compile(r'(tbl)|(Header)')
    for key in formdate_dictionary.keys():
        if formdate_dictionary[key]==True:
            simple_table_name = pattern_to_remove.sub('',key)
            header = arcno.MakeTableView(f'tbl{simple_table_name}Header', dimapath)
            detail = arcno.MakeTableView(f'tbl{simple_table_name}Detail', dimapath)
            break
    df = pd.merge(header,detail, how="inner", on="RecKey")
    return df



def get_plotkeys(dimapath):
    line = arcno.MakeTableView("tblLines", dimapath)
    plots = arcno.MakeTableView("tblPlots", dimapath)
    line_plot = pd.merge(line, plots, how="inner", on="PlotKey")
    return line_plot
