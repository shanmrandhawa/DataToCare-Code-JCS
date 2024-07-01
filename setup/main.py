import check_db_setup
import clean
import build_measurement
import standardize
import build_diagnosis
import sys
# setting path for importing scripts
sys.path.insert(1, '../common')
import db_handler


# start setup, by initiating a connection to database
# return connection as conn and cursor as cur
def start():
    conn = db_handler.intialize_database_handler()
    cur = conn.cursor()
    return conn, cur


# stop setup, by closing open connection and cursor to database
# return connection and cursor
def stop(conn, cur):
    db_handler.close_db_connection(conn, cur)
    print("Setup Successful.")


if __name__ == "__main__":
    conn, cur = start()
    if check_db_setup.check(cur):
        clean.clean_optimize_dataset(cur)
        build_measurement.build(cur)
        standardize.standardize(conn, cur)
        build_diagnosis.build(conn, cur)
    stop(conn, cur)
    input('Press anything to continue....')
