import pandas as pd
import pickle
import multiprocessing
import copy
import gc
import os
import sys
path = os.getcwd()
path = path.split('experiments')[0] + 'common'
# setting path for importing scripts
sys.path.insert(1, path)
import db_handler


# Get label and itemid from d_items, d_labitems
def get_label_itemid(conn, table):
    df_query = "Select itemid, label from {0}".format(table)
    return db_handler.make_selection_query(conn, df_query)


# Output patients stats for given admission and time
def output_patient_stats(conn, df_user_meas_abnr_num, hadm_id):
    d_items_df = get_label_itemid(conn, 'd_items')
    d_labitems_df = get_label_itemid(conn, 'd_labitems')
    df_labels = d_items_df.append(d_labitems_df, ignore_index=True)
    direc = 'results_sim_pats/'
    with open(direc + str(hadm_id) + '_patient_stats.txt', 'w') as fp:
        for index, row in df_user_meas_abnr_num.iterrows():
            out_str = ''
            label = df_labels[df_labels.itemid ==
                              row['itemid']].iloc[0]['label']
            out_str = '{0} : {1} is {2} \n'.format(
                row['itemid'], label, row['valstatus'])
            fp.write(out_str) 


 # Output similar patients as subject id, admission id as hadm_id
 # and similarity score
def output_similar_patient(similar_patients, hadm_id):
    print('outputting similar patients')
    direc = 'results_sim_pats/'
    with open(direc + str(hadm_id) + '_similar_patient.txt', 'w') as fp:
        for pat in similar_patients:
            out_str = 'Hadm_id : {0}, Offset : {1}, and Score = {2} \n'.format(
                pat['hadm_id'], pat['offset'], pat['score'])
            fp.write(out_str)
    out_sim_pat_with_scores = str(hadm_id) + "_similar_patients.pkl"
    with open(direc + out_sim_pat_with_scores, "wb") as f:
        pickle.dump(similar_patients, f)


# Find input user measurements from measurement table given
# chartevents or labevents
def find_user_meas_table(connection, table, hadm_id, charttime, meas_type):
    if meas_type == 0:
        table_df_query = "WITH T1 AS ( "\
                "SELECT itemid, max(charttime) AS latest_charttime "\
                "FROM {0} "\
                "WHERE hadm_id = {1} and charttime <= {2} and valuenum is not null "\
                "GROUP BY itemid ) "\
            "SELECT T2.itemid, T2.valuenum "\
            "FROM {0} AS T2 "\
            "JOIN T1 ON T2.itemid = T1.itemid AND T2.charttime = T1.latest_charttime "\
            "WHERE hadm_id = {1} ;".format(table, hadm_id, charttime)
        # print(table_df_query)
    return db_handler.make_selection_query(connection, table_df_query)


# Find input user measurements from measurement tables
def find_user_meas(connection, hadm_id, charttime, meas_type):
    tables = ['d3sv1_chartevents_mv', 'd3sv1_labevents_mv']
    df_chart_events = find_user_meas_table(
        connection, tables[0], hadm_id, charttime, meas_type)
    df_lab_events = find_user_meas_table(
        connection, tables[1], hadm_id, charttime, meas_type)
    #return df_chart_events
    return df_chart_events.append(df_lab_events, ignore_index=True)


# Label input user measurements for numerical as high, low
# based on upper and lower percentile
# using them we will build query to find similar patients
# for categorical (meas_type > 0) we will find categories that are abnormal for
# input user, return labeled dataframes with abnormal values
def label_abnormal_user_meas(df_user_meas, meas_type):
    if not df_user_meas.empty:
        if not meas_type:
            df_user_meas_abnr = pd.DataFrame(
                columns=['itemid', 'valstatus', 'pb', 'symbol'])
            df_num_cmpt = pd.read_pickle('numeric_computaion.pkl')
            #print(len(df_num_cmpt.itemid.unique()))
            for index, row in df_user_meas.iterrows():
                valstatus = ''
                pb = 0
                symbol = ''
                meas = df_num_cmpt[df_num_cmpt.itemid == row['itemid']].iloc[0]
                if (row['valuenum'] <= meas['up']) and (
                        row['valuenum'] >= meas['lp']):
                    pass
                elif (row['valuenum'] > meas['up']):
                    valstatus = 'high'
                    pb = meas['up']
                    symbol = '>'
                elif (row['valuenum'] < meas['lp']):
                    valstatus = 'low'
                    pb = meas['lp']
                    symbol = '<'
                user_meas_abnr_dict = {
                    'itemid': row['itemid'],
                    'valstatus': valstatus,
                    'pb': pb,
                    'symbol': symbol,
                }
                if valstatus:
                    df_user_meas_abnr = df_user_meas_abnr.append(
                        user_meas_abnr_dict, ignore_index=True)
            return df_user_meas_abnr
    return pd.DataFrame()


# Take input user abnormal values as input and make queries to find
# similar patients
def build_query_similar_patients(df_user_meas_abnr, meas_type, table):
    if not meas_type:

        query_similar_patients_itms = ''

        for index, row in df_user_meas_abnr.iterrows():
            qstr = '{0},'.format(
                int(row['itemid']))
            query_similar_patients_itms = query_similar_patients_itms + qstr
        query_similar_patients_itms = ','.join(query_similar_patients_itms.split(',')[0:-1])
        if query_similar_patients_itms == '':
            return ''
        query_similar_patients_itms = 'and itemid IN (' + query_similar_patients_itms + ')'
        query_similar_patients_tmp = "SELECT T2.hadm_id, itemid, valuenum, charttime, EXTRACT(EPOCH FROM charttime-admittime)/3600 as states "\
                                "FROM {0} AS T2, admissions "\
                                "WHERE T2.hadm_id = admissions.hadm_id and valuenum is not null ".format(table)
        query_similar_patients = query_similar_patients_tmp + query_similar_patients_itms
        return query_similar_patients


# For a particular patient, find the best similar state using most
# update values at that state and also rank them using jaccard distance
def cal_best_state_abnormals(df_sim_patients_num_tmp,h_id,total_items,similar_patients):

    df_sim_patients_num_tmp = df_sim_patients_num_tmp.sort_values(by=['states'], ascending=True)
    abnormal_states = df_sim_patients_num_tmp[df_sim_patients_num_tmp.abnormal == 1].states.unique().tolist()

    count = 0
    state = -1

    for s in abnormal_states:

        df_max_value_group = df_sim_patients_num_tmp[df_sim_patients_num_tmp.states <= s]
        df_max_value_group = df_max_value_group.loc[df_max_value_group.groupby('itemid')['charttime'].idxmax()]
        df_max_value_group = df_max_value_group[df_max_value_group.abnormal == 1]
        abrsc = len(df_max_value_group)
        if abrsc >= count:
            count = abrsc
            state = s

    score = (count / total_items) * 100
    if score > 0:
        tmp_dict = {
                'offset': state,
                'hadm_id': h_id,
                'score': score,
                }

        similar_patients.append(tmp_dict)


# Find similar patients by first building queries separately for dealing with
# numerical and categorical data for both chart and lab events tables
# After getting potential patients for numerical/categorical measurements
# We evaluate all patients matching measures(items) against similarity
# percentage
def find_similar_patients(
        connection,
        df_user_meas_abnr_num,
        hadm_id):
    tables = ['d3sv1_chartevents_mv', 'd3sv1_labevents_mv']
    df_sim_patients_num = pd.DataFrame()

    if len(df_user_meas_abnr_num) > 0:

        bq_smp_num_chart = build_query_similar_patients(
                df_user_meas_abnr_num[df_user_meas_abnr_num.itemid>220000], 0, tables[0])


        bq_smp_num_lab = build_query_similar_patients(
                df_user_meas_abnr_num[df_user_meas_abnr_num.itemid<220000], 0, tables[1])

        tmpn_df_lab = pd.DataFrame()
        tmpn_df_chart = pd.DataFrame()

        if bq_smp_num_lab:
            tmpn_df_lab = db_handler.make_selection_query(connection, bq_smp_num_lab)
        if bq_smp_num_chart:
            tmpn_df_chart = db_handler.make_selection_query(connection, bq_smp_num_chart)        

        df_sim_patients_num = tmpn_df_chart.append(
            tmpn_df_lab, ignore_index=True)

        vals_dict = {}
        chart_lab_clashes = pd.read_csv('clashes_abnormal.csv')
        for index, row in chart_lab_clashes.iterrows():
            vals_dict[row['cid']] = row['itemid_new']
            vals_dict[row['lid']] = row['itemid_new']

        total_items = 0
        similar_patients = []

        if not df_user_meas_abnr_num.empty:
            df_user_meas_abnr_num['itemid'].replace(vals_dict, inplace=True)
            total_items = total_items + len(df_user_meas_abnr_num.itemid.unique())

        if not df_sim_patients_num.empty:
            df_sim_patients_num['itemid'].replace(vals_dict, inplace=True)

            df_sim_patients_num.reset_index(inplace=True)
            df_sim_patients_num['abnormal'] = 0

            for index, row in df_user_meas_abnr_num.iterrows():
                itemid = int(row['itemid'])
                val = row['pb']
                if row['symbol'] == '<':
                    df_sim_patients_num.loc[df_sim_patients_num.index[(df_sim_patients_num['itemid'] == itemid) & (df_sim_patients_num['valuenum'] < val)], 'abnormal'] = 1
                elif row['symbol'] == '>':
                    df_sim_patients_num.loc[df_sim_patients_num.index[(df_sim_patients_num['itemid'] == itemid) & (df_sim_patients_num['valuenum'] > val)], 'abnormal'] = 1

            df_sim_patients_num_abr = df_sim_patients_num[df_sim_patients_num.abnormal == 1]
            
            df_similar_patients_score_filtr = df_sim_patients_num_abr.groupby(
            ['hadm_id'])['itemid'].nunique().reset_index().rename(columns={'itemid': 'counts'})
            df_similar_patients_score_filtr = df_similar_patients_score_filtr[df_similar_patients_score_filtr.counts > 0]

            df_sim_patients_num = df_sim_patients_num[df_sim_patients_num.hadm_id.isin(df_similar_patients_score_filtr.hadm_id.tolist())]

            manager = multiprocessing.Manager()
            similar_patients = manager.list()

            val_pats = pd.read_csv('valid_admissions_wo_holdout.csv')
            valid_hadmids = val_pats.hadm_id.tolist()
            df_sim_patients_num = df_sim_patients_num[df_sim_patients_num['hadm_id'].isin(valid_hadmids)]

                    
            sim_ids = df_sim_patients_num.hadm_id.unique().tolist()
            sim_ids.remove(hadm_id)
            
            print('total patients with states in consideration ' + str(len(sim_ids)))

            chunks = [sim_ids[x:x+250] for x in range(0, len(sim_ids), 250)]

            grouped = df_sim_patients_num.groupby(df_sim_patients_num.hadm_id)

            for adm_chunk in chunks:
                jobs = []
                for h_id in adm_chunk:
                    p = multiprocessing.Process(target=cal_best_state_abnormals, args=(grouped.get_group(h_id),h_id,total_items,similar_patients,))
                    p.start()
                    jobs.append(p)

                for proc in jobs:
                    proc.join()
                    proc.close()
                    gc.collect()
                    
                print('similar patient finding sub-processes chunk processed')
            
            gc.collect()
            
            if len(similar_patients) > 0:

                output_similar_patient(copy.deepcopy(similar_patients), hadm_id)
                del similar_patients
            else:
                output_similar_patient([], hadm_id)
        else:
            output_similar_patient([], hadm_id)
    else:
        output_similar_patient([], hadm_id)


# Call functions that evaluate given patient state and and determine close patients.
def evaluate(conn, hadm_id = 0, t = ''):
    # directory that will contain each testing patient abnormals at time t and similar patients
    result_directory = 'results_sim_pats' 
    if not os.path.exists(result_directory):
        os.makedirs(result_directory)
    t = "\'" + t + "\'"
    user_meas_num = find_user_meas(conn, hadm_id, t, 0)
    df_user_meas_abnr_num = label_abnormal_user_meas(user_meas_num, 0)
    output_patient_stats(conn, df_user_meas_abnr_num, hadm_id)
    find_similar_patients(
        conn,
        df_user_meas_abnr_num,
        hadm_id)
