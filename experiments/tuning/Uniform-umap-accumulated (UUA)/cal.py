import pandas as pd
import copy
import pickle
import random
import math
import os
import sys
path = os.getcwd()
path = path.split('experiments')[0] + 'common'
# setting path for importing scripts
sys.path.insert(1, path)
import db_handler


# for each patient, Compile results to output actual and predicted treatment
def compile_results(hadm_id,time,td, tdf_tmp):
    

    rdf = pd.DataFrame()
    df_pat = pd.DataFrame(columns=['patient','treatment','t','time_diff_from_admission','h-value','actual','predicted'])

    directory = 'results_treat_predict/' + str(hadm_id) + '/'

    if os.path.isdir(directory):
        for filename in os.listdir(directory):
            if 'rf_2' in filename:
                file_path = os.path.join(directory, filename)
                
                tdf = pd.read_pickle(file_path)
            
                rdf = rdf.append(tdf, ignore_index=True)

    if len(rdf) > 0:
        rdf = rdf[rdf.state == 0]
        rdf = rdf[rdf.time == time]

        rdf_tmp_2 = rdf[rdf.predict==1]

    actual_treats = []
    try:
        if not tdf_tmp.empty:
            actual_treats = tdf_tmp.mapped_id.unique().tolist()
            actual_treats = [int(x) for x in actual_treats]
    except:
        pass
    treatments = copy.deepcopy(actual_treats)

    predicted_treats = []
    if len(rdf) > 0:
        predicted_treats = rdf_tmp_2.treatment.unique().tolist()
        predicted_treats = [int(x) for x in predicted_treats]
        treatments.extend(predicted_treats)

    treatments = list(set(treatments))


    if len(treatments) == 0:
        trt_dict = {
            'patient':hadm_id,
            'treatment': 00,
            't':time,
            'time_diff_from_admission':td,
            'h-value':2,
            'actual': 1,
            'predicted' : 1
            }
        df_pat = df_pat.append(trt_dict, ignore_index=True)
    else:
        for treat in treatments:
    
            actual = 0
            if treat in actual_treats:
                actual = 1
                
            predicted = 0
            if treat in predicted_treats:
                predicted = 1
            
            trt_dict = {
            'patient':hadm_id,
            'treatment': treat,
            't':time,
            'time_diff_from_admission':td,
            'h-value':2,
            'actual': actual,
            'predicted' : predicted
            }
            df_pat = df_pat.append(trt_dict, ignore_index=True)

    return df_pat


# get treatment data to determine treatment actually given not from potential treatment set which was given to similar patient 
def get_treatment_data(conn):

    treatment_data_query = "SELECT hadm_id,starttime,endtime,mapped_id, label "\
                    "FROM mimiciii.inputevents_mv, mimiciii.d3sv1_drugs_mapping "\
                    "WHERE inputevents_mv.itemid = d3sv1_drugs_mapping.itemid AND d3sv1_drugs_mapping.mapping_level = 1 "

    treatment_df = db_handler.make_selection_query(conn, treatment_data_query)

    return treatment_df


# calculate metrics for predictions of testing patients
def calculate_results(conn):
    experiment = 'experiment_micu_eval.csv'
    pset = pd.read_csv(experiment)

    tdf = get_treatment_data(conn)
    pset['evaltime'] = pd.to_datetime(pset['evaltime'])
    tdf['starttime'] = pd.to_datetime(tdf['starttime'])
    tdf['endtime'] = pd.to_datetime(tdf['endtime'])
    df = pd.DataFrame()

    for row in pset.itertuples():

        hadm_id = getattr(row, 'hadm_id')
        time = getattr(row, 'evaltime')
        
        time_horizon = time + pd.Timedelta(2, unit='h')

        td = getattr(row, 'timediff')
        
        tdf_tmp_h = tdf[tdf.hadm_id == hadm_id]
        tdf_tmp_1 = tdf_tmp_h[tdf_tmp_h.starttime >= time]
        tdf_tmp_1 = tdf_tmp_1[tdf_tmp_1.starttime <= time_horizon]
        tdf_tmp_2 = tdf_tmp_h[tdf_tmp_h.starttime <= time]
        tdf_tmp_2 = tdf_tmp_2[tdf_tmp_2.endtime >= time]
        
        tdf_tmp_1 = tdf_tmp_1.append(tdf_tmp_2, ignore_index=True)

        df_pat = compile_results(hadm_id,time,td, tdf_tmp_1)
        if len(df_pat) > 0:
            df = df.append(df_pat, ignore_index=True)


    df = df[df.treatment != 0]

    val_pats = pd.read_csv('valid_admissions_wo_holdout.csv')
    tdf = pd.merge(tdf,val_pats)
    all_treats = tdf.mapped_id.unique().tolist()

    #process the compile result to calculate average precision recall and F1-score predictions using the confusion matrix
    print('calculating metric of Accuracy with respect to patients average (precision, recall, F1-score) for the experiment.')

    TP = 0
    TN = 0
    FN = 0
    FP = 0


    for pat in df.patient.unique():
        
        
        rdf_tmp = df[df.patient == pat]
        tn_treats = rdf_tmp.treatment.tolist()
        
        tn_treats = list(set(all_treats) - set(tn_treats))
        
        TN = TN + len(tn_treats)
        

    pairs = []
    for row in df.itertuples():
        actual = getattr(row, 'actual')
        predict = getattr(row, 'predicted')
       
        pairs.append((actual,predict))
        

    TP = pairs.count((1,1))
    FN = pairs.count((1,0))
    FP = pairs.count((0,1)) 

    p = TP / (TP + FP)

    r =  TP/ (TP + FN)

    f = ((2* p* r)/(p + r))

    print('Average Precision is: ', p)

    print('Average Recall is: ', r)

    print('Average F1-Score is: ', f)

    print('calculating metric of Accuracy with respect to treatments (accumulated F1-score) for the experiment.')


    tdf = tdf[['hadm_id','mapped_id']]

    tdf = tdf.groupby(['mapped_id']).size().reset_index(name='counts')

    total = tdf.counts.sum()
    total_items = tdf.mapped_id.nunique()
    sz = tdf['counts'].size-1

    print()
    tdf['percentile'] = tdf['counts'].rank(method='max').apply(lambda x: 100.0*(x-1)/sz)
    # df = df[df.percentile <= 75]

    average = tdf.counts.mean()
    tdf = tdf.rename({'mapped_id': 'treatment'}, axis='columns')
    df = pd.merge(tdf, df, on='treatment')

    df = df.sort_values(by=['percentile'], ascending=True)

    treatments = []
    percentile = []
    rrecall = []
    rprecision = []
    rf1score = []

    for per in df.percentile.unique():
        rdf_tmp = df[df.percentile <= per]
        percentile.append(per)
        given_times = len(rdf_tmp[rdf_tmp.actual == 1])
        predicted_correctly = len(rdf_tmp[(rdf_tmp.actual == 1) & (rdf_tmp.predicted == 1)])
        predicted_incorrectly = len(rdf_tmp[(rdf_tmp.actual == 0) & (rdf_tmp.predicted == 1)])

        recall = 0
        precision = 0
        try:
            recall = predicted_correctly/given_times
            precision = predicted_correctly/(predicted_correctly + predicted_incorrectly)
        except:
            pass
        rrecall.append(recall)
        rprecision.append(precision)
        fscore = 0
        if (precision) or (recall):
            fscore = (2 * precision * recall) / (precision + recall)
            
        rf1score.append(fscore)
        

    result = pd.DataFrame()

    result['percentile'] =  percentile
    result['precision'] =  rprecision
    result['recall'] =  rrecall
    result['fscore'] =  rf1score
    result.to_csv('accumulated_results.csv')

    print('Result of Accuracy with respect to treatments (accumulated F1-score, precision, recall against percentile) is generated in accumulated_results.csv ')

