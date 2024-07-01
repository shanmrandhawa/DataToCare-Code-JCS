import pandas as pd
import pickle
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
    direc = 'results_sim_pats/'
    with open(direc + str(hadm_id) + '_similar_patient.txt', 'w') as fp:
        for pat in similar_patients:
            out_str = 'Subject_id : {0}, Hadm_id : {1}, and Score = {2} \n'.format(
                pat['subject_id'], pat['hadm_id'], pat['score'])
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
            "WHERE hadm_id = {1} and charttime <= {2} and valuenum is not null;".format(table, hadm_id, charttime)
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
        query_similar_patients = 'select distinct subject_id, hadm_id, itemid from {0} where '.format(
            table)
        for index, row in df_user_meas_abnr.iterrows():
            qstr = ' (itemid = {0} and valuenum {1} {2}) or'.format(
                int(row['itemid']), row['symbol'], round(row['pb'], 2))
            query_similar_patients = query_similar_patients + qstr
        return ' or'.join(query_similar_patients.split(' or')[0:-1])


# Find similar patients by first building queries separately for dealing with
# numerical and categorical data for both chart and lab events tables
# After getting potential patients for numerical/categorical measurements
# We evaluate all patients matching measures(items) against similarity
# percentage
def find_similar_patients(
        connection,
        df_user_meas_abnr_num,
        sim_per,
        hadm_id):
    tables = ['d3sv1_chartevents_mv', 'd3sv1_labevents_mv']
    df_sim_patients_num = pd.DataFrame()

    if len(df_user_meas_abnr_num) > 0:
        bq_smp_num_chart = build_query_similar_patients(
                df_user_meas_abnr_num[df_user_meas_abnr_num.itemid>220000], 0, tables[0])
        bq_smp_num_lab = build_query_similar_patients(
                df_user_meas_abnr_num[df_user_meas_abnr_num.itemid<220000], 0, tables[1])

        bnempty = '' in [bq_smp_num_chart,bq_smp_num_lab]

        if not bnempty:
            bq_smp_num = bq_smp_num_chart + '\nUNION\n' + bq_smp_num_lab
        elif bq_smp_num_chart:
            bq_smp_num = bq_smp_num_chart
        elif bq_smp_num_lab:
            bq_smp_num = bq_smp_num_lab

        if bq_smp_num:
            #print(bq_smp_num)
            tmpn_df = db_handler.make_selection_query(connection, bq_smp_num)
            df_sim_patients_num = df_sim_patients_num.append(
            tmpn_df, ignore_index=True)
        

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
            print(len(df_sim_patients_num))
            df_sim_patients_num = df_sim_patients_num.drop_duplicates()
            print(len(df_sim_patients_num))
            df_similar_patients_score = df_sim_patients_num.groupby(
            ['subject_id', 'hadm_id']).size().reset_index().rename(columns={0: 'count'})

            for index, row in df_similar_patients_score.iterrows():
                score = (int(row['count']) / total_items) * 100
                if score > sim_per:
                    if hadm_id != int(row['hadm_id']):
                        s_id = str(int(row['subject_id']))
                        h_id = str(int(row['hadm_id']))
                        similar_patients_dict = {
                                'subject_id': s_id,
                                'hadm_id': h_id,
                                'score': score
                                }
                        similar_patients.append(similar_patients_dict)
        output_similar_patient(similar_patients, hadm_id)
    else:
        output_similar_patient([], hadm_id)

# Call functions that evaluate given patient state and and determine close patients.
def evaluate(conn, hadm_id = 0, t = ''):
    # directory that will contain each testing patient abnormals at time t and similar patients
    result_directory = 'results_sim_pats' 
    if not os.path.exists(result_directory):
        os.makedirs(result_directory)
    t = "\'" + t + "\'"
    lower_sim_per = 0
    user_meas_num = find_user_meas(conn, hadm_id, t, 0)
    df_user_meas_abnr_num = label_abnormal_user_meas(user_meas_num, 0)
    output_patient_stats(conn, df_user_meas_abnr_num, hadm_id)
    find_similar_patients(
        conn,
        df_user_meas_abnr_num,
        lower_sim_per,
        hadm_id)
