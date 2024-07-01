import sys
import pandas as pd
# setting path for importing scripts in external folder
sys.path.insert(1, '../common')
import db_handler


# Read regex cleaning sql scripts to apply
def read_regex_sql_scripts():
    with open('regex_cleaning_sql_scripts.txt') as fp:
        sql_scripts = fp.read().splitlines()
    return sql_scripts


# Check whether tables with drug information exist in the database
def check_drug_tables_exist(cur):
    tables = ['inputevents_mv', 'd_items']
    for table in tables:
        if db_handler.perform_table_check(cur, table):
            pass
        else:
            print('Error: Required table {0} for standardizing '\
                  'does not exists in database.\n'.format(table))
            return False
    return True


# Create mapping table if not exists with
# unclean drug labels using d_items and inputevents_mv
def create_mapping_table(cur, table):
    if not db_handler.perform_table_check(cur, table):
        print('Creating mapping table {0}'.format(table))
        create_mapping_table_query = "CREATE TABLE {0} AS SELECT DISTINCT "\
            "d_items.itemid, d_items.label, 0 AS mapped_id, 0 AS mapping_level, "\
            "'clean' AS puprose FROM d_items INNER JOIN inputevents_mv on "\
            "d_items.itemid = inputevents_mv.itemid;".format(table)
        db_handler.make_opertional_query(cur, create_mapping_table_query)
        print('Table created {0}'.format(table))


# Apply regex cleaning scripts to unclean label column in maaping table
def clean_labels(cur, table):
    regex_clean_scripts = read_regex_sql_scripts()
    for recs in regex_clean_scripts:
        clean_query = "UPDATE {0} SET label = {1};".format(table, recs)
        db_handler.make_opertional_query(cur, clean_query)


# Get unmapped dataframe of drug id (itemid) and its cleaned label
def get_clean_unmap_df(conn, table):
    unmap_df_query = "Select itemid, label from {0}".format(table)
    return db_handler.make_selection_query(conn, unmap_df_query)


# Get maximum drug id (itemid) from drugs parent table (d_items)
def get_max_itemid(curr):
    max_itemid_query = "Select max(itemid) from d_items"
    return db_handler.make_aggregate_query(curr, max_itemid_query)


# Get maximum mapped drug id (itemid) from mapping table (d3sv1_drugs_mapping)
def get_max_mappedid(curr):
    max_itemid_query = "Select max(mapped_id) from d3sv1_drugs_mapping"
    return db_handler.make_aggregate_query(curr, max_itemid_query)


# Find similar itemids based on label and
# map them to a unified higher level code
# clean mapping is at level 0 (base)
def map_data(conn, cur, table):
    max_itemid = get_max_itemid(cur)
    offset = max_itemid + 100000
    unmap_df = get_clean_unmap_df(conn, table)
    unique_labels = unmap_df.label.unique()
    for label in unique_labels:
        offset = offset + 1
        label_itemids = list(unmap_df[unmap_df.label == label].itemid)
        for itemid in label_itemids:
            map_itemid_query = "UPDATE {0} SET mapped_id = {1} "\
                "where itemid = {2};".format(table, offset, itemid)
            db_handler.make_opertional_query(cur, map_itemid_query)


# Find similar itemids based on aelous and
# map them to a unified higher level code
# aeolus mapping is at level 1
def map_aelous_data(conn, cur, table):
    max_mappedid = get_max_mappedid(cur)
    offset = max_mappedid + 100000
    unmap_df = get_clean_unmap_df(conn, table)
    unique_items = unmap_df.itemid.unique()

    aelous_map_df = pd.read_csv('mapping_inputevents_itemid_parent.csv')
    aelous_map_df['itemid'] = pd.to_numeric(aelous_map_df['itemid'])
    aelous_map_mv_df = aelous_map_df[aelous_map_df.itemid > 220000]
    aelous_map_mv_df_filtr = aelous_map_mv_df[aelous_map_mv_df.itemid.isin(unique_items)]
    unique_aelous_labels = aelous_map_mv_df_filtr.aeolus.unique()

    for label in unique_aelous_labels:
        offset = offset + 1
        label_itemids = list(aelous_map_mv_df_filtr[aelous_map_mv_df_filtr.aeolus == label].itemid)
        for itemid in label_itemids:
            map_aelous_itemid_query = "INSERT INTO {0} (itemid, label, mapped_id, mapping_level, puprose) "\
                "VALUES({1},'{2}', {3}, 1, 'aeolus');".format(table, itemid, label, offset)
            db_handler.make_opertional_query(cur, map_aelous_itemid_query)
    print('Drug Data Strandardized and entered in new mapping table, ')


# Call functions to create mapping table that unify similar drugs
# with same clean label and different drug ids (itemid)
def standardize(conn, cur):
    table = 'd3sv1_drugs_mapping'
    if check_drug_tables_exist(cur):
        create_mapping_table(cur, table)
        clean_labels(cur, table)
        map_data(conn, cur, table)
        map_aelous_data(conn, cur, table)
