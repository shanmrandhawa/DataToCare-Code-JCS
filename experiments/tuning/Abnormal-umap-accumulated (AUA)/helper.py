import pandas as pd
import os
import sys
path = os.getcwd()
path = path.split('experiments')[0] + 'common'
# setting path for importing scripts
sys.path.insert(1, path)
import db_handler


# Read all measurements type information by itemid
# from table d3sv1_measurement_items_type
# type 0 means numerical, 1 means categorical 
def get_measurements_type():
    conn = db_handler.intialize_database_handler()
    meas_type_df_query = "SELECT itemid, type FROM d3sv1_measurement_items_type;"
    meas_type_df = db_handler.make_selection_query(conn, meas_type_df_query)
    db_handler.close_db_connection(conn, conn.cursor())
    return meas_type_df

# return categorical or numerical measurement itemid's in a list
def get_meas_list(type=0):
    meas_types_df = get_measurements_type()
    meas_types_df['itemid'] = pd.to_numeric(meas_types_df['itemid'])
    if type:
        return meas_types_df[meas_types_df.type == 1].itemid.unique().tolist()
    else:
        return meas_types_df[meas_types_df.type == 0].itemid.unique().tolist()