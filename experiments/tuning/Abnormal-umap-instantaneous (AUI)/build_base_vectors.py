import pandas as pd
import copy
import numpy as np
from scipy.stats import sem, t
import os
import sys
path = os.getcwd()
path = path.split('experiments')[0] + 'common'
# setting path for importing scripts
sys.path.insert(1, path)
import db_handler


# Create and intialize features and treatment vectors columns
def intialize_base_vectors(times_df, meas_itms_vec_num, meas_itms_vec_cat, treatments):

    base_cols = ['hadm_id','time']

    base_vectors = times_df
    
    base_vectors['age'] = [0] * len(times_df)
    base_vectors['gender'] = ['F'] * len(times_df)
    base_vectors['ethnicity'] = ['WHITE'] * len(times_df)
    base_vectors['insurance'] = ['Private'] * len(times_df)
    base_vectors['icu_type'] = [None] * len(times_df)
    demo_cols = copy.deepcopy(base_cols)
    demo_cols.append('age')
    demo_cols.append('gender')
    demo_cols.append('ethnicity')
    demo_cols.append('insurance')
    demo_cols.append('icu_type')

    diag_cols = copy.deepcopy(base_cols)
    for i in range(0, 18):
        col = 'diagnosis_group_' + str(i + 1)
        diag_cols.append(col)
        base_vectors[col] = [0] * len(times_df)
    
    meas_cols = copy.deepcopy(base_cols)
    for item in meas_itms_vec_num['itemid']:
        col = 'meas_' + str(item)
        meas_cols.append(col)
        base_vectors[col] = [np.nan] * len(times_df)

    for item in meas_itms_vec_cat['itemid']:
        col = 'meas_' + str(item)
        meas_cols.append(col)
        base_vectors[col] = [None] * len(times_df)
    
    treat_cols = copy.deepcopy(base_cols)
    for trmnt in treatments:
        col_rec = str(trmnt) + '_recency'
        col_tms = str(trmnt) + '_given_times'
        col_nxt = str(trmnt) + '_given_nxt'
        treat_cols.append(col_rec)
        treat_cols.append(col_tms)
        treat_cols.append(col_nxt)
        base_vectors[col_rec] = [-1] * len(times_df)
        base_vectors[col_tms] = [0] * len(times_df)
        base_vectors[col_nxt] = [0] * len(times_df)
    
    return base_vectors, demo_cols, diag_cols, meas_cols, treat_cols


# Get items which will be used as measuremnts vector
# Return numrical and categorical measures seperately
def get_meas_items_features(conn, hadm_id, t, similar_patients):

    tmp_pats_query = '('
    for pat in similar_patients['hadm_id'].unique().tolist():
        tmp_pats_query = tmp_pats_query + str(pat) + ', '
    tmp_pats_query = ", ".join(tmp_pats_query.split(", ")[0:-1])
    tmp_pats_query = tmp_pats_query + ')'

    meas_itms_vec_num_chart_query = "SELECT DISTINCT itemid FROM d3sv1_chartevents_mv "\
        "WHERE valuenum IS NOT null and hadm_id IN " + tmp_pats_query + " INTERSECT "\
        "SELECT DISTINCT itemid FROM d3sv1_chartevents_mv "\
        "WHERE valuenum IS NOT null and hadm_id = {0} and charttime <= \'{1}\'; ".format(hadm_id, t)
    
    meas_itms_vec_cat_chart_query = "SELECT DISTINCT itemid FROM d3sv1_chartevents_mv "\
        "WHERE valuenum IS null and hadm_id IN " + tmp_pats_query + " INTERSECT "\
        "SELECT DISTINCT itemid FROM d3sv1_chartevents_mv "\
        "WHERE valuenum IS null and hadm_id = {0} and charttime <= \'{1}\'; ".format(hadm_id, t)

    meas_itms_vec_num_lab_query = "SELECT DISTINCT itemid FROM d3sv1_labevents_mv "\
        "WHERE valuenum IS NOT null and hadm_id IN " + tmp_pats_query + " INTERSECT "\
        "SELECT DISTINCT itemid FROM d3sv1_labevents_mv "\
        "WHERE valuenum IS NOT null and hadm_id = {0} and charttime <= \'{1}\'; ".format(hadm_id, t)

    meas_itms_vec_cat_lab_query = "SELECT DISTINCT itemid FROM d3sv1_labevents_mv "\
        "WHERE valuenum IS null and hadm_id IN " + tmp_pats_query + " INTERSECT "\
        "SELECT DISTINCT itemid FROM d3sv1_labevents_mv "\
        "WHERE valuenum IS null and hadm_id = {0} and charttime <= \'{1}\'; ".format(hadm_id, t)

    meas_itms_vec_num_chart = db_handler.make_selection_query(
        conn, meas_itms_vec_num_chart_query)
    meas_itms_vec_cat_chart = db_handler.make_selection_query(
        conn, meas_itms_vec_cat_chart_query)
    meas_itms_vec_num_lab = db_handler.make_selection_query(
        conn, meas_itms_vec_num_lab_query)
    meas_itms_vec_cat_lab = db_handler.make_selection_query(
        conn, meas_itms_vec_cat_lab_query)

    return meas_itms_vec_num_chart.append(meas_itms_vec_num_lab, ignore_index=True), meas_itms_vec_cat_chart.append(meas_itms_vec_cat_lab, ignore_index=True)


# Get all times for patients at which diagnosis or measurement was taken
# Return times for patients as a dataframe
def get_all_times(conn, hadm_id, t, similar_patients):
    
    all_time_df = pd.DataFrame()
    for index, row in similar_patients.iterrows():
        h_id = int(row['hadm_id'])
        offset = float(row['offset']) + 0.0001

        #here integrate pruning all times where they are outside of window
        time_query = "with cte as ( "\
        "SELECT hadm_id,charttime AS time FROM d3sv1_chartevents_mv "\
        "WHERE hadm_id = {0} UNION "\
        "SELECT hadm_id,timestamp AS time FROM d3sv1_patient_diagnosis_time "\
        "WHERE hadm_id = {0} UNION "\
        "SELECT hadm_id,charttime AS time FROM d3sv1_labevents_mv "\
        "WHERE hadm_id = {0} ) "\
        "SELECT cte.hadm_id, cte.time  FROM cte, admissions WHERE "\
        "cte.hadm_id = admissions.hadm_id and EXTRACT(EPOCH FROM time-admittime)/3600 <= {1};"
        time_query = time_query.format(h_id,offset)
        
        all_time_df = all_time_df.append(db_handler.make_selection_query(conn, time_query), ignore_index=True)

    t = pd.to_datetime(t)
    time_dict_pat_tmp = {
        'hadm_id': hadm_id,
        'time': t
    }
    all_time_df = all_time_df.append(time_dict_pat_tmp, ignore_index=True)
    return all_time_df


# Build base vectors by calling all required functions
def build(conn, hadm_id, t, similar_patients, treatments_df):
    times_df = get_all_times(conn, hadm_id, t, similar_patients)
    meas_itms_vec_num, meas_itms_vec_cat = get_meas_items_features(
        conn, hadm_id, t, similar_patients)
    base_vectors, demo_cols, diag_cols, meas_cols, treat_cols = intialize_base_vectors(
        times_df, meas_itms_vec_num, meas_itms_vec_cat, treatments_df.mapped_id.unique())
    return meas_itms_vec_num, meas_itms_vec_cat, base_vectors, demo_cols, diag_cols, meas_cols, treat_cols