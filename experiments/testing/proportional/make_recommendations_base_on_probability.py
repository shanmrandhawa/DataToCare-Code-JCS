import pandas as pd
import os
import sys
path = os.getcwd()
path = path.split('experiments')[0] + 'common'
# setting path for importing scripts
sys.path.insert(1, path)
import db_handler


# get treatment data to determine probabilities to make predictions 
def get_treatment_data(conn):

    treatment_test_pat_data_query = "SELECT hadm_id,starttime,endtime,mapped_id, label "\
                    "FROM mimiciii.inputevents_mv, mimiciii.d3sv1_drugs_mapping "\
                    "WHERE inputevents_mv.itemid = d3sv1_drugs_mapping.itemid AND d3sv1_drugs_mapping.mapping_level = 1; "\

    treatment_test_pat_df = db_handler.make_selection_query(conn, treatment_test_pat_data_query)

    return treatment_test_pat_df

# process each testing patient and make treatment predictions for them based on probability
def process_predict(conn, pset):

	print('Processing Patients to make treatment predictions for them based on probability')

	all_prediction = {}

	simset = pd.read_csv('valid_admissions_wo_holdout.csv')
	simset['admittime'] = pd.to_datetime(simset['admittime'])
	simset['dischtime'] = pd.to_datetime(simset['dischtime'])
	simset['timespent'] = (simset['dischtime'] - simset['admittime']) / pd.Timedelta(hours=1)
	
	tdf = get_treatment_data(conn)

	treat_w_admit = pd.merge(tdf,simset, on='hadm_id')
	treat_w_admit['admittime'] = pd.to_datetime(treat_w_admit['admittime'])
	treat_w_admit['starttime'] = pd.to_datetime(treat_w_admit['starttime'])
	treat_w_admit['endtime'] = pd.to_datetime(treat_w_admit['endtime'])

	treat_w_admit['timediffstart'] = (treat_w_admit['starttime'] - treat_w_admit['admittime']) / pd.Timedelta(hours=1)
	treat_w_admit['timediffend'] = (treat_w_admit['endtime'] - treat_w_admit['admittime']) / pd.Timedelta(hours=1)

	pset['evaltime'] = pd.to_datetime(pset['evaltime'])

	for row in pset.itertuples():

	    hadm_id = getattr(row, 'hadm_id')
	    
	    td = getattr(row, 'timediff')
	    ptd = td+2

	    val_pats = simset[simset.timespent >= ptd].hadm_id.unique().tolist()
	    
	    pdf_tmp = treat_w_admit[treat_w_admit.hadm_id != hadm_id]
	    pdf_tmp = pdf_tmp[pdf_tmp.hadm_id.isin(val_pats)]
	    
	    total_pats = len(val_pats)
	    
	    pdf_tmp_1 = pdf_tmp[pdf_tmp.timediffstart >= td]
	    pdf_tmp_1 = pdf_tmp_1[pdf_tmp_1.timediffstart <= ptd]
	    pdf_tmp_2 = pdf_tmp[pdf_tmp.timediffstart <= td]
	    pdf_tmp_2 = pdf_tmp_2[pdf_tmp_2.timediffend >= td]
	    
	    pdf_tmp_1 = pdf_tmp_1.append(pdf_tmp_2, ignore_index=True)
	    
	    prediction = []
	    for treat in pdf_tmp_1.mapped_id.unique().tolist():   
	    
	        pdf_tmp_1_trt = pdf_tmp_1[pdf_tmp_1.mapped_id == treat]
	        
	        pat_trt = len(pdf_tmp_1_trt.hadm_id.unique())
	        
	        prob = pat_trt/total_pats
	        if prob >= 0.5:
	            prediction.append(treat)

	    all_prediction[hadm_id] = prediction

	print('All Patients Processed with treatments predicted for them.')
	return all_prediction