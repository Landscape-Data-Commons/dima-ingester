from datetime import datetime
import os, os.path, sys


def csv_report(df, table, batch_path):
    """
    list of project keys,
    list of primarykeys
    """
    today= datetime.now()
    current_time = today.strftime('%Y-%m-%d-%H:%M:%S')
    filename = f'{table}_report_{current_time}.csv'
    filepath = os.path.normpath(os.path.join(batch_path,filename))
    df = df['dataframe'].loc[:,['DBKey','PrimaryKey']].drop_duplicates('PrimaryKey')
    df.to_csv(filepath)
