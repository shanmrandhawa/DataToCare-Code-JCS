import find_treatments
import build_feature_vectors
import build_base_vectors
import build_models_predictions
import compute
import cal
import time
import multiprocessing 
import copy
import pickle
import pandas as pd
import gc
import os
import sys
path = os.getcwd()
path = path.split('experiments')[0] + 'common'
# setting path for importing scripts
sys.path.insert(1, path)
import db_handler



# start pipeline, by intiating connection to database
# return connection as conn and cursor as cur
def start():
    print("Pipeline started for testing DataToCare using Abnormal-umap-accumulated (AUA).")
    conn = db_handler.intialize_database_handler()
    cur = conn.cursor()
    return conn, cur


# stop pipeline, by closing open connection and cursor to database
# return connection and cursor
def stop(conn, cur):
    db_handler.close_db_connection(conn, cur)
    print("Pipeline ended for testing DataToCare using Abnormal-umap-accumulated (AUA).")


if __name__ == "__main__":
    
    conn, cur = start()

    #compute abnormal ranges
    compute.compute(conn)

    # read testing patients and run the pipeline for each 500 patients
    experiment = 'experiment_micu_testing.csv'

    pats_set = pd.read_csv(experiment)
    print('Started pipeline to process each testing Patient')

    for row in pats_set.itertuples():

        hadm_id = getattr(row, 'hadm_id')
        print('Paitent Hospital Admission ID = %.0f' % (hadm_id))

        t = getattr(row, 'evaltime')

        print('Evaluation time t = ' + str(t))

        start = time.time()

        similar_patients, treatments_df = find_treatments.find(conn, cur, hadm_id, t)
        if len(similar_patients) > 0:
            try:
                similar_patients = [int(x) for x in similar_patients]
                print("Treatments and similar patients calculated")

                meas_itms_vec_num, meas_itms_vec_cat, base_vectors, demo_cols, diag_cols, meas_cols, treat_cols = build_base_vectors.build(conn, hadm_id, t, similar_patients, treatments_df)
                gc.collect()

                print("About to start processes for adding feature vectors (demo, diag, meas and treatment) ")

                sim_adm_ids = copy.deepcopy(similar_patients)
                sim_adm_ids.append(hadm_id)

                build_feature_vectors.clear()

                demo_main_pool = multiprocessing.Pool(processes=50)
                diag_main_pool = multiprocessing.Pool(processes=50)

                build_feature_vectors.process_demogrphic_vectors(base_vectors[demo_cols], sim_adm_ids,demo_main_pool)
                build_feature_vectors.process_diagnosis_vectors(base_vectors[diag_cols], sim_adm_ids,diag_main_pool)

                demo_main_pool.close()
                demo_main_pool.join()
                diag_main_pool.close()
                diag_main_pool.join()
                
                print('Demographic and Diagnosis vectors calculated')


                meas_main_pool = multiprocessing.Pool(processes=100)
                treat_main_pool = multiprocessing.Pool(processes=100)

                build_feature_vectors.process_measurement_vectors(base_vectors[meas_cols], sim_adm_ids, meas_itms_vec_num, meas_itms_vec_cat, meas_main_pool)
                build_feature_vectors.process_treatment_vectors(base_vectors[treat_cols], sim_adm_ids, treatments_df, treat_main_pool)

                meas_main_pool.close()
                meas_main_pool.join()
                treat_main_pool.close()
                treat_main_pool.join()
                
                print('Treatment and Measurement vectors calculated')

                demo_df = build_feature_vectors.get_demo_vector_data(demo_cols)
                diag_df = build_feature_vectors.get_diag_vector_data(diag_cols)
                treat_df = build_feature_vectors.get_treat_vector_data(treat_cols)
                meas_df = build_feature_vectors.get_meas_vector_data(meas_cols)

                gc.collect()

                features_vectors_demo_diag_meas = pd.merge(demo_df, diag_df, on=['time','hadm_id'])
                features_vectors_demo_diag_meas = pd.merge(features_vectors_demo_diag_meas, meas_df, on=['time','hadm_id'])

                features_vectors_demo_diag_meas.sort_values(['hadm_id', 'time'], ascending=[True, True], inplace=True)
                treat_df.sort_values(['hadm_id', 'time'], ascending=[True, True], inplace=True)

                training_meas_diag_demo = features_vectors_demo_diag_meas[features_vectors_demo_diag_meas.hadm_id != hadm_id]
                testing_meas_diag_demo = features_vectors_demo_diag_meas[features_vectors_demo_diag_meas.hadm_id == hadm_id]
                training_treat = treat_df[treat_df.hadm_id != hadm_id]
                testing_treat = treat_df[treat_df.hadm_id == hadm_id]            

                print('Length of Feature Vectors = %.0f' % (len(features_vectors_demo_diag_meas)))

                gc.collect()
                
                print('Vectors combined for processing by prediction script')

                build_models_predictions.build(hadm_id,training_meas_diag_demo, training_treat, testing_meas_diag_demo, testing_treat)
                build_models_predictions.cal_potential_results(hadm_id)
                done = time.time()
                elapsed = done - start
                print('Total time taken to process patient : ' + str(elapsed))
            except Exception as e: 
                print(e)
        else:
            print('No Similar Patient found.')

    cal.calculate_results(conn)

    stop(conn, cur)
    input('Press anything to continue....')