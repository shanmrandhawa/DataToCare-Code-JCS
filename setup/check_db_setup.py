import sys
# setting path for importing scripts
sys.path.insert(1, '../common')
import db_handler

# Check whether all the essential tables for setup  exist in the database
def check_tables_exist(cur):
    tables = [
        'chartevents',
        'labevents',
        'inputevents_cv',
        'patients',
        'd_items',
        'd_labitems']
    for table in tables:
        if db_handler.perform_table_check(cur, table):
            pass
        else:
            print(
                'Error: Required table{0} for Setup does not exists in database.\n'.format(table))
            return False
    return True


# Check whether all the essential tables for setup loaded
# correctly in the database
def check_tables_loaded(cur):
    table_rows_count_expected = {
        # table name : row count
        'chartevents': 330712483,
        'labevents': 27854055,
        'inputevents_cv': 17527935,
        'patients': 46520,
        'd_items': 12487,
        'd_labitems': 753,
    }
    for key, value in table_rows_count_expected.items():
        # key = table name, value = row_count
        rows_count_query = "SELECT COUNT(*) FROM {0};".format(key)
        rows_count = db_handler.make_aggregate_query(cur, rows_count_query)
        if rows_count == value:
            pass
        else:
            print(
                'Error: Required table{0} for Setup is not loaded correctly database.\n'.format(key))
            return False
    return True


# Return whether both essential tables exist and loaded completely or not
def check(cur):
    return (check_tables_exist(cur) and check_tables_loaded(cur))
