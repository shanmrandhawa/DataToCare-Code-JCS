import pandas as pd
import numpy as np
from scipy.stats import sem, t
import os
import sys
path = os.getcwd()
path = path.split('experiments')[0] + 'common'
# setting path for importing scripts
sys.path.insert(1, path)
import db_handler


# Get dataframe corresponding to specific table and where clause
def get_table_df(connection, table, where):
    table_df_query = "Select * from {0} where {1}".format(table, where)
    return db_handler.make_selection_query(connection, table_df_query)


# Get discharge measurements data frame for numerical or categrical data
# depending upon meas_type, if meas_type = 0 then return numerical dataframe
# if meas_type = 1 then return categorical dataframe
def get_disch_measr(connection, meas_type):
    tables = ['d3sv1_chartevents_mv_dm',
              'd3sv1_labevents_mv_dm']
    where_num = 'valuenum is not null'
    where_cat = 'valuenum is null'
    if meas_type == 0:
        df_numeric_chartevents = get_table_df(connection, tables[0], where_num)
        df_numeric_labevents = get_table_df(connection, tables[1], where_num)
        df_numeric = df_numeric_chartevents.append(
            df_numeric_labevents, ignore_index=True)
        # to make sure only tuning patients are included in measurement abnormality calculation
        valid_pats = pd.read_csv('valid_admissions_wo_holdout.csv')
        df_numeric = df_numeric[df_numeric.hadm_id.isin(valid_pats.hadm_id.unique().tolist())]
        return df_numeric


# Calculate percentile corrosponding to middle range
# For passed data series
def percentile_confidence_interval(dataseries, vrange):
    a = 1.0 * np.array(dataseries)  # change to one dimensional array
    o = (100-vrange)/2 # offset to calculate above and below
    up = np.percentile(a, vrange+o) # above limit
    lp = np.percentile(a, o)
    return (lp,up)

# For each numerical measurement(itemid), compute mean, std and P% confidence interval
# return dataframe with abovementioned stats for all itemids
def compute_numeric(connection, p):
    df_num_cmpt = pd.DataFrame(columns=['itemid', 'lp', 'up'])
    df_numeric = get_disch_measr(connection, 0)
    df_numeric_itemids = df_numeric.itemid.unique()
    for itemid in df_numeric_itemids:
        df_itemid_valnum = df_numeric[df_numeric.itemid == itemid].valuenum
        itemid_cmpt = percentile_confidence_interval(df_itemid_valnum, p)
        itemid_cmpt_vals = [*itemid_cmpt]
        itemid_cmpt_dict = {
            'itemid': itemid,
            'lp': itemid_cmpt_vals[0],
            'up': itemid_cmpt_vals[1]}
        df_num_cmpt = df_num_cmpt.append(itemid_cmpt_dict, ignore_index=True)
    return df_num_cmpt

# Call function to compute numerical measurements stats
# and save them in a pickle file
def num_comp_save(connection, p):
    df_num_cmpt = compute_numeric(connection, p)
    df_num_cmpt.to_pickle('numeric_computaion.pkl')


# Call functions to compute measurements statistics
def compute(conn):
    p = 80 # normal_quantile
    num_comp_save(conn, p)
    print('Abnormal quantile ranges calculated')
