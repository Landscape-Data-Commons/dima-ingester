from src.utils.database_functions import arcno
import pandas as pd
import re
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
    """
    arc = arcno()
    tables_with_formdate = form_date_check(dimapath) # returns dictionary
    header_detail_df = header_detail(tables_with_formdate, dimapath) # returns
                       # dataframe with old formdate
    new_formdate_df = new_form_date(header_detail_df, date_range) # returns
                      # dataframe with new formdate range
    final_df = arc.CalculateField(new_formdate_df,"PrimaryKey", "PlotKey", "FormDatePK")
    return final_df



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
    lst = formdate_df.FormDate.unique()
    date_range = pd.date_range(start=lst.min(), end=lst.max(), freq=f'{date_spread}D')
    for i in range(0,len(date_range.tolist())):
        if i < len(date_range)-1:
            if target_date in pd.date_range(start=date_range[i], end=date_range[i+1]):
                return date_range[i]
        else:
            if target_date in pd.date_range(start=date_range[i], end=formdate_df.FormDate.max()):
                return date_range[i]

def new_form_date(old_formdate_dataframe, custom_daterange):
    """ given a dataframe with a formdate field,
    returns a dataframe with a new formdate field with custom daterange classes
    for primarykey creation
    """
    old_formdate_dataframe['FormDatePK'] = old_formdate_dataframe.apply(
        lambda x: date_grp(x.FormDate, old_formdate_dataframe,custom_daterange),
        axis=1
    )
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
