import make_recommendations_base_on_probability
import cal
import pandas as pd
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
    print("Pipeline started for testing proportional experiment.")
    conn = db_handler.intialize_database_handler()
    cur = conn.cursor()
    return conn, cur


# stop pipeline, by closing open connection and cursor to database
# return connection and cursor
def stop(conn, cur):
    db_handler.close_db_connection(conn, cur)
    print("Pipeline ended for testing proportional experiment.")


if __name__ == "__main__":
    
    conn, cur = start()

    # read testing patients and run the pipeline for each 500 patients
    experiment = 'experiment_micu_testing.csv'

    pats_set = pd.read_csv(experiment)
    predictions = make_recommendations_base_on_probability.process_predict(conn, pats_set)

    cal.calculate_results(conn, pats_set, predictions)

    stop(conn, cur)
    input('Press anything to continue....')