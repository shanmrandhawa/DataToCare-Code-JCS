import pickle
import pandas as pd
import evaluate
import os
import sys
path = os.getcwd()
path = path.split('experiments')[0] + 'common'
# setting path for importing scripts
sys.path.insert(1, path)
import db_handler


# read similar patients hadm_ids from pickel created by evaluate of task 1
def read_similar_patients(hadm_id):

    similar_patients_pkl = 'results_sim_pats/' + str(hadm_id) + "_similar_patients.pkl"
    similar_patients_df = pd.DataFrame(columns = ['hadm_id','offset','score'])
    with open(similar_patients_pkl, "rb") as f:
        similar_patients = pickle.load(f)
        for pat in similar_patients:
            tmp_pat_dict = {
                'hadm_id' : pat['hadm_id'],
                'offset' : pat['offset'],
                'score' : pat['score']
            }
            similar_patients_df = similar_patients_df.append(
                        tmp_pat_dict, ignore_index=True)

    similar_patients_df['hadm_id'] = similar_patients_df['hadm_id'].astype(int)
    similar_patients_df['offset'] = similar_patients_df['offset'].astype(float)
    similar_patients_df['score'] = similar_patients_df['score'].astype(float)
    similar_patients_df = similar_patients_df[similar_patients_df.offset > 0]
    similar_patients_df = similar_patients_df.sort_values(by=['score'], ascending=False)

    val_pats = pd.read_csv('valid_admissions_wo_holdout.csv')
    valid_hadmids = val_pats.hadm_id.tolist()
    similar_patients_df = similar_patients_df[similar_patients_df['hadm_id'].isin(valid_hadmids)]
    similar_patients_df = similar_patients_df[['hadm_id','offset']]

    sim_pat_list = []
    similar_patients_df_tmp = similar_patients_df.head(200)
    sim_pat_list.extend(similar_patients_df_tmp['hadm_id'].tolist())
    print('Total number of similar patients found before integrating random patients = %.0f' % (len(similar_patients_df_tmp)))
        
    if len(sim_pat_list) < 200:
        rem_len = 200 - len(sim_pat_list)
        val_pats = val_pats[~val_pats.hadm_id.isin(sim_pat_list)]
        val_pats = val_pats[val_pats.hadm_id != hadm_id]
        val_pats = val_pats.sample(n=rem_len)
        val_pats['offset'] = (val_pats['dischtime'] - val_pats['admittime']) / pd.Timedelta(hours=1)
        val_pats = val_pats[['hadm_id','offset']]
        similar_patients_df_tmp = similar_patients_df_tmp.append(val_pats, ignore_index=True)

    print('Total number of similar patients found = %.0f' % (len(similar_patients_df_tmp)))
    return similar_patients_df_tmp


# Find all treatments given to K/all-close patients
# Return treatments as a dataframe
def get_all_treatments(conn, hadm_id, similar_patients):
    all_treat_df = pd.DataFrame()
    for index, row in similar_patients.iterrows():
        h_id = int(row['hadm_id'])
        offset = float(row['offset']) + 0.0001
        treat_query = "SELECT t1.hadm_id,t1.starttime,t1.endtime,t1.itemid,t2.label,t2.mapped_id "\
        "FROM inputevents_mv t1, d3sv1_drugs_mapping t2, admissions WHERE t1.hadm_id = admissions.hadm_id and t1.itemid = t2.itemid "\
        "and EXTRACT(EPOCH FROM t1.starttime-admittime)/3600 <= {0} and t2.mapping_level = 1 and t1.hadm_id = {1};".format(offset,h_id)
        all_treat_df = all_treat_df.append(db_handler.make_selection_query(conn, treat_query), ignore_index=True)

    mapped_ids = list(all_treat_df.mapped_id.unique())
    print('treatment in consideration : ' + str(len(mapped_ids)))

    if len(mapped_ids) > 0:
        target_pat_query = "SELECT t1.hadm_id,t1.starttime,t1.endtime,t1.itemid,t2.label,t2.mapped_id "\
            "FROM inputevents_mv t1 INNER JOIN d3sv1_drugs_mapping t2 ON t1.itemid = t2.itemid "\
            "WHERE t1.hadm_id = " + str(hadm_id) + " and t2.mapped_id IN ("
        for mpid in mapped_ids:
            target_pat_query =  target_pat_query + str(mpid) + ', '
        target_pat_query = ", ".join(target_pat_query.split(", ")[0:-1])
        target_pat_query = target_pat_query + ');'
        target_treat_df = db_handler.make_selection_query(conn, target_pat_query)
        
        all_treat_df = all_treat_df.append(target_treat_df, ignore_index=True)
        return all_treat_df
    return pd.DataFrame()



# find similar patients and treatments given to them
def find(conn, hadm_id,t):
    evaluate.evaluate(conn, hadm_id,t)
    similar_patients = read_similar_patients(hadm_id)
    if len(similar_patients) > 0:
        return similar_patients,get_all_treatments(conn, hadm_id, similar_patients)
    else:
        return [], pd.DataFrame()
