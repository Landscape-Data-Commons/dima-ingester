import os, os.path
# os.chdir(os.path.join(os.getcwd(),'src')) if os.path.basename(os.getcwd())!='src' else None
import pandas as pd
import numpy as np
from sqlalchemy import *
from src.utils.database_functions import db, arcno
from src.project.project_tables import fields_dict
from src.utils.utility_functions import table_create, sql_command, tablecheck



type_translate = {np.dtype('int64'):'int',
    'Int64':'int',
    np.dtype("object"):'text',
    np.dtype('datetime64[ns]'):'timestamp',
    np.dtype('bool'):'boolean',
    np.dtype('float64'):'float(5)',}


"""
pulling db.str
configparser to create connection string for sqlalchemy engine

1. template() - creates an empty dataframe with the fields in "fields_dict"
2. read_template(directory_with_xls, dataframe) - reads the excel into template!
3. update_project(directory, project_key, public_or_dev_db)- sends to postgres!

"""


def engine_conn_string(string):
    d = db(string)
    return f'postgresql://{d.params["user"]}:{d.params["password"]}@{d.params["host"]}:{d.params["port"]}/{d.params["dbname"]}'

# engine_conn_string("dimadev")

def send_proj(df, conn):
    schema={
    "dimadev":"dimadev",
    "dima":"Public"
    }
    eng = create_engine(engine_conn_string(conn),
            connect_args={'options': '-csearch_path={}'.format(schema[conn])})
    df.to_sql(con=eng, name="Projects", if_exists="append", index=False)

def template():
    """ creating an empty dataframe with a specific
    set of fields and field types
    """
    df = pd.DataFrame(fields_dict)
    return df

def read_template(dir, maindf):
    """ creates a new dataframe with the data on the metadata excel file,
    appending it to an empty dataframe be uploaded to the projects table in pg.
    """
    maindfcopy = maindf.copy()
    maindf.drop(maindf.index,inplace=True)
    for path in os.listdir(dir):
        if (os.path.splitext(path)[1]==".xlsx") & ('project' in path):
            df = pd.read_excel(os.path.join(dir,path))

            data = [i for i in df.Value]

        elif os.path.splitext(path)[1]!=".xlsx":
            pass
        else:
            print("No metadata '.xlsx'(excel) file found within directory. Please provide project metadata file.")
    maindf.loc[len(maindf),:] = data
    return maindf


def update_project(path_in_batch,projectkey, database=None):
    """ ingests a project metadata file if project key does not exist.

    - would be better if it would automatically pull projectkey from
    the excel file is handling to check
    - projectkey is made up of what?
    - creates datafrmae from excel metadata table
    - adds projectkey field
    - ingests/updates project table in pg

    1. check if table exists, if not create it
    2. check if project key exists, if not update it

    """
    tempdf = template()

    # check if table exists
    if tablecheck("Projects", database):
        # print("the table exists")
        if project_key_check(projectkey, database):
            print("projectkey exists, aborting update to 'Projects' table")
        else:
            update = read_template(path_in_batch,tempdf)
            update['project_key'] = projectkey
            send_proj(update, database)

    # if no, create table and update pg
    else:
        # print("the table does not exist")
        table_create(tempdf,"Projects",database)
        add_projectkey_to_pg(database)
        update = read_template(path_in_batch, tempdf)
        update['project_key'] = projectkey

        send_proj(update, database)


def project_key_check(projectkey, database):
    d = db(database)
    projectkey+="%"

    try:
        con = d.str
        cur = con.cursor()
        exists_query = '''
        select exists (
            select 1
            from "Projects"
            where "project_key" like %s
        )'''
        cur.execute (exists_query, (projectkey,))
        return cur.fetchone()[0]

    except Exception as e:
        print(e)
        con = d.str
        cur = con.cursor()


def projkeyfield_existence(database):
    d = db(database)
    schema={
    "dimadev":"dimadev",
    "dima":"Public"
    }
    try:
        con = d.str
        cur = con.cursor()
        exists_query = '''
        select exists (
            select 1
            from information_schema.columns

            where table_schema = %s
            AND table_name = 'Projects'
            AND column_name = 'project_key'
        )'''
        cur.execute (exists_query, (schema[database],))
        return cur.fetchone()[0]

    except Exception as e:
        print(e)
        con = d.str
        cur = con.cursor()




def add_projectkey_to_pg(database):
    d = db(database)
    add_query = '''
        ALTER TABLE IF EXISTS "Projects"
        ADD COLUMN "project_key" TEXT;
        '''
    if projkeyfield_existence(database):
        pass
    else:
        try:
            con = d.str
            cur = con.cursor()
            cur.execute(add_query)
            con.commit()

        except Exception as e:
            print(e)
            con = d.str
            cur = con.cursor()



def gettables(conn=None):
    keyword = "dimadev" if conn=="dimadev" else "public"
    d = db("dimadev") if conn=="dimadev" else db("dima")

    sql = f"""SELECT table_name
        FROM information_schema.tables
        WHERE (
        table_schema = '{keyword}'
        )
        ORDER BY table_name;"""
    try:
        con = d.str
        cur = con.cursor()
        cur.execute(sql)
        lst = cur.fetchall()
        return [i[0] for i in lst]

    except Exception as e:
        print(e)
        con = d.str
        cur = con.cursor()

def getdbkeys(key ,conn=None):
    keyword = "dimadev" if conn=="dimadev" else "public"
    d = db("dimadev") if conn=="dimadev" else db("dima")

    sql = f'''
        SELECT
        DISTINCT "DBKey"
        FROM "{keyword}"."{key}";

    '''
    try:
        con = d.str
        cur = con.cursor()
        cur.execute(sql)
        lst = cur.fetchall()
        return [i[0] for i in lst]

    except Exception as e:
        print(e)
        con = d.str
        cur = con.cursor()


def all_dimas(conn=None):
    keyword = "dimadev" if conn=="dimadev" else "public"
    unique_dbkey = []
    tablelist = gettables(keyword)
    for table in tablelist:
        if 'Projects' not in table:
            dbkeys=getdbkeys(table, keyword)
            for i in dbkeys:
                if i not in unique_dbkey:
                    unique_dbkey.append(i)
    return unique_dbkey
