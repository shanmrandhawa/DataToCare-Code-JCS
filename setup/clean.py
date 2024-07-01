import sys
# setting path for importing scripts
sys.path.insert(1, '../common')
import db_handler

# check if already table exists with measurement data exclusive to mv patients
def check_dataset_optimized(cur):
    tables = ['d3sv1_chartevents_mv', 'd3sv1_labevents_mv']
    for table in tables:
        if db_handler.perform_table_check(cur, table):
            pass
        else:
            return False
    return True


# Creating measurement tables with data exclusive to mv patients
# and with only columns required by setup and pipeline
def optimize(cur):
    tables_for_optimization = ['chartevents', 'labevents']
    for table in tables_for_optimization:
        print('Creating measurement table {0}'.format(table))
        optimize_table_query = "CREATE TABLE d3sv1_{0}_mv AS "\
            "SELECT {0}.row_id, {0}.subject_id, {0}.hadm_id, {0}.itemid, {0}.charttime, "\
            "{0}.value, {0}.valuenum, {0}.valueuom FROM {0} INNER JOIN d3sv1_patients_mv "\
            "ON {0}.subject_id = d3sv1_patients_mv.subject_id;".format(table)
        if db_handler.make_opertional_query(cur, optimize_table_query):
            print('Table created {0}'.format(table))
            print('Creating index on table {0}'.format(table))
            add_hadm_charttime_item_index_query = "CREATE INDEX d3sv1_{0}_mv_idx01 "\
                "ON d3sv1_{0}_mv (hadm_id, itemid, charttime);".format(table)
            db_handler.make_opertional_query(
                cur, add_hadm_charttime_item_index_query)
            print('Index created on table {0}'.format(table))


# check if already table exists with patients data exclusive to mv patients
def check_dataset_cleaned(cur):
    table = 'd3sv1_patients_mv'
    if db_handler.perform_table_check(cur, table):
        count_patients_mv_query = "SELECT COUNT(subject_id) FROM {0};".format(
            table)
        count_patients_mv = db_handler.make_aggregate_query(
            cur, count_patients_mv_query)
        if count_patients_mv > 20000:
            return False
        else:
            return True
    else:
        return False


# Creating tables d3sv1_patients_mv and d3sv1_admissions_mv with patients exclusive to mv
def clean(cur):
    print('Creating table d3sv1_patients_mv')
    clean_patients_query = "CREATE TABLE d3sv1_patients_mv AS "\
        "with cte as ( SELECT subject_id FROM CHARTEVENTS where ITEMID < 220000 "\
        "UNION "\
        "SELECT subject_id FROM INPUTEVENTS_CV ) "\
        "SELECT * FROM PATIENTS WHERE subject_id NOT IN (SELECT subject_id FROM cte);"
    db_handler.make_opertional_query(cur, clean_patients_query)
    clean_admissions_query = "CREATE TABLE d3sv1_admissions_mv AS "\
        "SELECT * FROM ADMISSIONS WHERE subject_id IN (SELECT subject_id FROM d3sv1_patients_mv);"
    db_handler.make_opertional_query(cur, clean_admissions_query)
    print('Tables created')


# clean and optimize the dataset, if not already, by
# making tables that have only mv patients data
def clean_optimize_dataset(cur):
    if check_dataset_cleaned(cur):
        print('Dataset is already filtered.')
    else:
        clean(cur)
        print('Dataset is filtered.')
    if check_dataset_optimized(cur):
        print('Dataset is already optimized.')
    else:
        optimize(cur)
        print('Dataset is optimized.')
