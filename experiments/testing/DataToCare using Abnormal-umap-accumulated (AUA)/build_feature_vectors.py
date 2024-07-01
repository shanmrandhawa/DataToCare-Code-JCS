import pandas as pd
import numpy as np
import multiprocessing
import time
import pickle
import os
import sys
path = os.getcwd()
path = path.split('experiments')[0] + 'common'
# setting path for importing scripts
sys.path.insert(1, path)
import db_handler



#global variables to be used instead of filing
demo_vector_data = []
diag_vector_data = []
meas_vector_data = []
treat_vector_data = []

def clear(): 
    del demo_vector_data[:]
    del diag_vector_data[:]
    del meas_vector_data[:]
    del treat_vector_data[:]

def collect_demo_data(result):
    demo_vector_data.extend(result)

def collect_diag_data(result):
    diag_vector_data.extend(result)

def collect_meas_data(result):
    meas_vector_data.extend(result)

def collect_treat_data(result):
    # print('appending data')
    treat_vector_data.extend(result)

def get_demo_vector_data(cols):
    dvd_df = pd.DataFrame(demo_vector_data, columns=cols)
    return dvd_df

def get_diag_vector_data(cols):
    divd_df = pd.DataFrame(diag_vector_data, columns=cols)
    return divd_df

def get_meas_vector_data(cols):
    mvd_df = pd.DataFrame(meas_vector_data, columns=cols)
    return mvd_df

def get_treat_vector_data(cols):
    tvd_df = pd.DataFrame(treat_vector_data, columns=cols)
    return tvd_df


# For a given admission, return ICU types patient is admitted.
def get_icu_types(conn, hadm_id):
    icu_type_query = "SELECT curr_careunit, intime, outtime FROM transfers WHERE hadm_id = {0}"\
        ";".format(hadm_id)
    icu_type_df = db_handler.make_selection_query(conn, icu_type_query)
    return icu_type_df


# Take feature vectors by patient and incooperate corrosponding demagrohic data in it
def enrich_demographic_features(demo_vectors_amd_id, adm_id, demo_data_adm_id):
    
    conn = db_handler.intialize_database_handler()

    val_pats = pd.read_csv('valid_admissions_wo_holdout.csv')
    val_pats = val_pats[val_pats.hadm_id == adm_id]

    if len(val_pats) < 1:
        val_pats = pd.read_csv('experiment_micu_testing.csv')
        val_pats = val_pats[val_pats.hadm_id == adm_id]

    demo_vectors_amd_id['age'] = val_pats['age'].iloc[0]
    demo_vectors_amd_id['ethnicity'] = demo_data_adm_id['ethnicity']
    demo_vectors_amd_id['gender'] = demo_data_adm_id['gender']
    demo_vectors_amd_id['insurance'] = demo_data_adm_id['insurance']
    icu_type_df = get_icu_types(conn, adm_id)

    for row in demo_vectors_amd_id.itertuples():
        t = getattr(row, 'time')
        icu_type_df_tmp = icu_type_df[ (icu_type_df.intime <= t) & (icu_type_df.outtime >= t)]

        if not icu_type_df_tmp.empty:
            icu_type = icu_type_df_tmp['curr_careunit'].iloc[0]
            demo_vectors_amd_id.at[row.Index, 'icu_type'] = icu_type

    db_handler.close_db_connection(conn, conn.cursor())

    return demo_vectors_amd_id.values.tolist()


# Read all diagnosis performed on a specific admission unitll some time t
# called by build_output_group_diagnosis_vector_specific
def get_diagnosis_time_data_specific(conn, hadm_id):
    diag_time_df_query = "SELECT * FROM d3sv1_patient_diagnosis_time "\
        "WHERE hadm_id = {0}".format(hadm_id)
    return db_handler.make_selection_query(conn, diag_time_df_query)


# Take feature vectors by patient and incooperate corrosponding diagnosis data in it
def enrich_diagnosis_features(diag_vectors_amd_id, adm_id):
    
    conn = db_handler.intialize_database_handler()

    pat_diag_df = get_diagnosis_time_data_specific(
            conn, adm_id)

    for row in diag_vectors_amd_id.itertuples():
        t = getattr(row, 'time')

        pat_diag_df_tmp = pat_diag_df[pat_diag_df.timestamp <= t]
        if not pat_diag_df_tmp.empty:
            for j in range(0, 18):
                pat_diag_df_grp = pat_diag_df_tmp[pat_diag_df_tmp.higher_group == (
                    j + 1)]
                if len(pat_diag_df_grp) > 0:
                    diag_vectors_amd_id.at[row.Index, 'diagnosis_group_' + str(j + 1)] = 1

    db_handler.close_db_connection(conn, conn.cursor())

    return diag_vectors_amd_id.values.tolist()


# Take feature vectors by patient chunk and incooperate corrosponding measurement data in it
def enrich_measurement_features(features_vector_adm_id, pat_meas_df):

    for row in features_vector_adm_id.itertuples():
        t = getattr(row, 'time')
        pat_meas_df_tmp = pat_meas_df[pat_meas_df.charttime <= t]

        if not pat_meas_df_tmp.empty:

            itm_grps = pat_meas_df_tmp.groupby('itemid').first().reset_index()
            cols_itms = list(map(lambda x:'meas_' + str(x), itm_grps['itemid'].tolist()))
            lst_meas = itm_grps['value'].tolist()

            features_vector_adm_id.at[row.Index,  cols_itms] = lst_meas

    return features_vector_adm_id.values.tolist()


# Take feature vectors by patient chunk and incooperate corrosponding treatment data in it
def enrich_treatment_features(features_vector_adm_id, pat_treat_df):

    all_treats_pats = pat_treat_df.mapped_id.unique()

    for row in features_vector_adm_id.itertuples():
        t = getattr(row, 'time')
    
        pat_treat_df_tmp = pat_treat_df[(pat_treat_df.starttime <= t)]

        trmts_grps = pat_treat_df_tmp.groupby('mapped_id').size().reset_index(name='counts')
        cols_trmts_gvn_times = list(map(lambda x:str(x) + '_given_times', trmts_grps['mapped_id'].tolist()))    

        count_trmts = trmts_grps['counts'].tolist()

        features_vector_adm_id.at[row.Index,  cols_trmts_gvn_times] = count_trmts

        pat_treat_df_tmp = pat_treat_df_tmp[pat_treat_df_tmp.endtime >= t]

        unique_trmts = []
        if not pat_treat_df_tmp.empty:
            unique_trmts = pat_treat_df_tmp.mapped_id.unique()
            
            cols_trmts_rcncy = list(map(lambda x: str(x) + '_recency' , unique_trmts))
            cols_trmts_gvn_nxt = list(map(lambda x: str(x) + '_given_nxt' , unique_trmts))
            features_vector_adm_id.at[row.Index,  cols_trmts_rcncy] = 0
            features_vector_adm_id.at[row.Index,  cols_trmts_gvn_nxt] = 1

        tgn = pd.to_datetime(t) + pd.DateOffset(hours=2)

        othr_trmts = list(set(all_treats_pats) - set(unique_trmts))

        pat_treat_df_tmp_trmts = pat_treat_df[pat_treat_df.mapped_id.isin(othr_trmts)]
        pat_treat_df_tmp = pat_treat_df_tmp_trmts[pat_treat_df_tmp_trmts.endtime < t]

        if not pat_treat_df_tmp.empty:

            trmts_grps = pat_treat_df_tmp.groupby('mapped_id').first().reset_index()
            cols_trmts_rcncy = list(map(lambda x:str(x) + '_recency', trmts_grps['mapped_id'].tolist()))

            rts = trmts_grps['endtime'].tolist()
            rmins = list(map(lambda x:round(((t-x)/np.timedelta64(1,'s'))/60), rts))

            features_vector_adm_id.at[row.Index,  cols_trmts_rcncy] = rmins

        pat_treat_df_tmp = pat_treat_df_tmp_trmts[pat_treat_df_tmp_trmts.starttime > t]
        pat_treat_df_tmp = pat_treat_df_tmp[pat_treat_df_tmp.starttime <= tgn]

        if not pat_treat_df_tmp.empty:
            cols_trmts_gvn_nxt = list(map(lambda x: str(x) + '_given_nxt' , pat_treat_df_tmp.mapped_id.unique()))
            features_vector_adm_id.at[row.Index,  cols_trmts_gvn_nxt] = 1

    return features_vector_adm_id.values.tolist()


#Start K threads to process demographic vector of each patient
def process_demogrphic_vectors(demo_vectors, unique_adm_ids, demo_main_pool):

    print("in function process_demogrphic_vectors")

    conn = db_handler.intialize_database_handler()
    
    tmp_pats_query = '('
    for pat in unique_adm_ids:
        tmp_pats_query = tmp_pats_query + str(pat) + ', '
    tmp_pats_query = ", ".join(tmp_pats_query.split(", ")[0:-1])
    tmp_pats_query = tmp_pats_query + ')'

    demo_data_query = " SELECT ethnicity, gender, insurance, hadm_id FROM admissions "\
        "INNER JOIN d3sv1_patients_mv "\
        "on admissions.subject_id = d3sv1_patients_mv.subject_id "\
        "WHERE hadm_id IN " + tmp_pats_query + ';'
    demo_data = db_handler.make_selection_query(conn, demo_data_query)
    
    for adm_id in unique_adm_ids: 
        demo_data_adm_id = demo_data[demo_data.hadm_id == adm_id].iloc[0]
        demo_vectors_amd_id = demo_vectors[demo_vectors.hadm_id == adm_id]
        if not demo_data_adm_id.empty:
            demo_main_pool.apply_async(enrich_demographic_features, args=(demo_vectors_amd_id, adm_id, demo_data_adm_id,), callback=collect_demo_data)

    db_handler.close_db_connection(conn, conn.cursor())

    print('Demographic vector calculation started, all sub-processes spawned')


#Start K threads to process diagnosis vector of each patient
def process_diagnosis_vectors(diag_vectors, unique_adm_ids, diag_main_pool):
    
    print("in function process_diagnosis_vectors")

    for adm_id in unique_adm_ids: 
        
        diag_vectors_amd_id = diag_vectors[diag_vectors.hadm_id == adm_id]
        diag_main_pool.apply_async(enrich_diagnosis_features, args=(diag_vectors_amd_id, adm_id,), callback=collect_diag_data)

    print('Diagnosis vector calculation started, all sub-processes spawned')


# Read all measurements of a specific admission
# for specific itemids or measurements
#return measurements as dataframe
def get_measurement_data_specific_items(conn, unique_adm_ids, items, meas_type, table):

    meas_time_df_query = ''
    if not meas_type:
        meas_time_df_query = "SELECT hadm_id,itemid,valuenum as value,charttime, 0 as type FROM {0} "\
        "WHERE hadm_id in (".format(table)
    else:
        meas_time_df_query = "SELECT hadm_id,itemid,value,charttime, 1 as type FROM {0} "\
        "WHERE hadm_id in (".format(table)
    
    for adm_id in unique_adm_ids:
        meas_time_df_query = meas_time_df_query + str(adm_id) + ', '
    meas_time_df_query = ", ".join(meas_time_df_query.split(", ")[0:-1])
    meas_time_df_query = meas_time_df_query + ') '
    
    if not meas_type:
        meas_time_df_query = meas_time_df_query + 'and valuenum is not null and itemid in ('
    else:
        meas_time_df_query = meas_time_df_query + 'and valuenum is null and itemid in ('

    if not len(items):
        return pd.DataFrame()
    
    meas_time_df_query_itm = ''
    for itm in items:
        meas_time_df_query_itm = meas_time_df_query_itm + str(itm) + ', '
    meas_time_df_query_itm = ", ".join(meas_time_df_query_itm.split(", ")[0:-1])

    if meas_time_df_query_itm:
        meas_time_df_query = meas_time_df_query + meas_time_df_query_itm + ');'
        return db_handler.make_selection_query(conn, meas_time_df_query)
    else:
        return pd.DataFrame()


#Start K processes to process measurement vector of each patient
def process_measurement_vectors(meas_vectors, unique_adm_ids, items_num, items_cat, meas_main_pool):

    print("in function process_measurement_vectors")

    conn = db_handler.intialize_database_handler()

    items_num_lab = items_num[items_num.itemid < 220000]
    items_num_chart = items_num[items_num.itemid > 220000]
    items_cat_lab = items_cat[items_cat.itemid < 220000]
    items_cat_chart = items_cat[items_cat.itemid > 220000]

    all_pat_meas_num_chart_df = get_measurement_data_specific_items(
        conn, unique_adm_ids, items_num_chart.itemid, 0, 'd3sv1_chartevents_mv')
    all_pat_meas_num_lab_df = get_measurement_data_specific_items(
        conn, unique_adm_ids, items_num_lab.itemid, 0, 'd3sv1_labevents_mv')
    all_pat_meas_cat_chart_df = get_measurement_data_specific_items(
        conn, unique_adm_ids, items_cat_chart.itemid, 1, 'd3sv1_chartevents_mv')
    all_pat_meas_cat_lab_df = get_measurement_data_specific_items(
        conn, unique_adm_ids, items_cat_lab.itemid, 1, 'd3sv1_labevents_mv')

    all_pat_meas_num = all_pat_meas_num_chart_df.append(all_pat_meas_num_lab_df, ignore_index=True)
    all_pat_meas_cat = all_pat_meas_cat_chart_df.append(all_pat_meas_cat_lab_df, ignore_index=True)
    all_pat_meas = all_pat_meas_num.append(all_pat_meas_cat, ignore_index=True)
    all_pat_meas.sort_values(by=['charttime'], ascending=False,  inplace=True)

    for adm_id in unique_adm_ids: 
        all_pat_meas_mp = all_pat_meas[all_pat_meas.hadm_id == adm_id]
        meas_vectors_amd_id = meas_vectors[meas_vectors.hadm_id == adm_id]
        if len(meas_vectors_amd_id) > 5000:
            csize = 2500
            list_of_dfs = [meas_vectors_amd_id.iloc[i:i+csize-1] for i in range(0, len(meas_vectors_amd_id),csize)]
            for i in range(0, len(list_of_dfs)):
                meas_vectors_amd_id_tmp = list_of_dfs[i]
                meas_main_pool.apply_async(enrich_measurement_features, args=(meas_vectors_amd_id_tmp, all_pat_meas_mp,), callback=collect_meas_data)
        else:
            meas_main_pool.apply_async(enrich_measurement_features, args=(meas_vectors_amd_id, all_pat_meas_mp,), callback=collect_meas_data)

    db_handler.close_db_connection(conn, conn.cursor())
    print('measurement vector calculation started, all sub-processes spawned')


#Start K processes to process treatment vector of each patient
def process_treatment_vectors(treat_vectors, unique_adm_ids, treatments_df, treat_main_pool):
    
    print("in function process_treatment_vectors")

    treatments_df.sort_values(by=['endtime'], ascending=False,  inplace=True)

    for adm_id in unique_adm_ids: 
        treatment_df_mp = treatments_df[treatments_df.hadm_id == adm_id]
        treat_vectors_amd_id = treat_vectors[treat_vectors.hadm_id == adm_id]

        if len(treat_vectors_amd_id) > 5000:
            csize = 2500
            list_of_dfs = [treat_vectors_amd_id.iloc[i:i+csize-1] for i in range(0, len(treat_vectors_amd_id),csize)]
            for i in range(0, len(list_of_dfs)):
                treat_vectors_amd_id_tmp = list_of_dfs[i]
                treat_main_pool.apply_async(enrich_treatment_features, args=(treat_vectors_amd_id_tmp, treatment_df_mp,), callback=collect_treat_data)
        else:
            treat_main_pool.apply_async(enrich_treatment_features, args=(treat_vectors_amd_id, treatment_df_mp,), callback=collect_treat_data)

    print('treatement vector calculation started, all sub-processes spawned')