import sys
# setting path for importing scripts
sys.path.insert(1, '../common')
import db_handler


# Delete rows for items which are both
# numrical and categorical and also delete items which dont 
# change with time 
def remove_inconsistencies(cur):
    tables = ['d3sv1_labevents_mv','d3sv1_chartevents_mv']
    for table in tables:
        query = "DELETE FROM {0} "\
            "where itemid in (SELECT DISTINCT itemid from {0} where valuenum is not null) "\
            "and itemid in (SELECT DISTINCT itemid from {0} where valuenum is null) "\
            "and valuenum is null;".format(table)
        db_handler.make_opertional_query(cur, query)
    query = "DELETE FROM {0} "\
            "where itemid in (226543, 226544, 226707, 226381, 226185, 226228, 226179, 225416, 225417, "\
            "225418, 225419, 225420, 225421, 225422, 225424, 225209);".format(tables[1])
    db_handler.make_opertional_query(cur, query)
    print('Inconsistencies removed')


# Create measurement type table to
# encompass information whether
#itemid is categorical or numerical
def create_measurement_type(cur):
    table = 'd3sv1_measurement_items_type'
    if not db_handler.perform_table_check(cur, '{0}'.format(table)):
        remove_inconsistencies(cur)
        print('Creating Measurement type table as {0}'.format(table))
        meas_type_query = "CREATE TABLE {0} as "\
            "SELECT DISTINCT itemid, 0 AS type from d3sv1_chartevents_mv where valuenum is not null "\
            "UNION "\
            "SELECT DISTINCT itemid, 1 AS type from d3sv1_chartevents_mv where valuenum is null "\
            "UNION "\
            "SELECT DISTINCT itemid, 0 AS type from d3sv1_labevents_mv where valuenum is not null "\
            "UNION "\
            "SELECT DISTINCT itemid, 1 AS type from d3sv1_labevents_mv where valuenum is null;".format(table)
        db_handler.make_opertional_query(cur, meas_type_query)
        print('Measurement Type table created')
        return True
    return False

#Create discharge mesaurements tables as 
#d3sv1_chartevents_mv_dm corrosponding to d3sv1_chartevents_mv and as
#d3sv1_labevents_mv_dm corrosponding to d3sv1_labevents_mv
def calc_discharge_meas(cur):
    tables_with_meas_data = ['d3sv1_chartevents_mv','d3sv1_labevents_mv']
    for table in tables_with_meas_data:
        if not db_handler.perform_table_check(cur, '{0}_dm'.format(table)):
            print('Creating discharge_measurements table for {0}'.format(table))
            disch_meas_table_query = "CREATE TABLE {0}_dm AS "\
                                    "WITH cte AS (SELECT hadm_id as hid, itemid AS iid, max(charttime) AS mct "\
                                    "FROM {0} group by hadm_id, itemid) "\
                                    "SELECT {0}.* from {0},cte "\
                                    "WHERE {0}.charttime = cte.mct AND {0}.hadm_id = cte.hid "\
                                    "AND {0}.itemid = cte.iid;".format(table)
            db_handler.make_opertional_query(cur, disch_meas_table_query)
            print('Discharge measurements table created for {0}'.format(table))


#Add labels corresponding to measurements (itemids) in measurement tables       
def add_labels(cur):
    tables_info = {
            #key = destination_table_name : value = source_table_name
            'd3sv1_chartevents_mv_dm' : 'd_items',
            'd3sv1_labevents_mv_dm' : 'd_labitems'
            }
    for key, value in tables_info.items():
        if not db_handler.perform_table_column_check(cur, key, 'label'):
            print('Adding column label to table : {0}'.format(key))
            add_label_col_query = "ALTER TABLE {0} "\
                            "ADD COLUMN label VARCHAR(200) NOT NULL DEFAULT 'in_process'".format(key);
            if db_handler.make_opertional_query(cur, add_label_col_query):
                add_label_data_query = "UPDATE {0} "\
                            "SET label=subquery.label "\
                            "FROM (SELECT itemid, label "\
                                  "FROM  {1}) AS subquery "\
                            "WHERE {0}.itemid=subquery.itemid;".format(key, value)
                if db_handler.make_opertional_query(cur, add_label_data_query):
                    print('Column label added in table : {0}'.format(key))


#Usings labels it creates a table that contains possible clashes 
#between chartevents and labevents discharge measurements
def find_possible_clashes(cur):
    tabels_for_clash = ['d3sv1_chartevents_mv_dm',
                          'd3sv1_labevents_mv_dm']
    tmp_clash_table = 'd3sv1_poss_clash_chart_lab_dm'
    if not db_handler.perform_table_check(cur, tmp_clash_table):
        print('Creating Temporary table {0} for finding clashes between '\
              'discharge measurements'.format(tmp_clash_table))
        possible_clashes_table_query = "CREATE TABLE {2} AS "\
                                "select {0}.hadm_id as hid, {0}.itemid as iid, "\
                                "{0}.charttime as cht, {0}.value as cval, "\
                                "{1}.value as lval, {0}.valuenum as cvalnum, {1}.valuenum as lvalnum "\
                                "from {0}, {1} "\
                                "where {0}.hadm_id = {1}.hadm_id "\
                                "and {0}.label = {1}.label "\
                                "and {0}.charttime = {1}.charttime "\
                                "and {0}.value != {1}.value;".format(tabels_for_clash[0], tabels_for_clash[1], tmp_clash_table)
        db_handler.make_opertional_query(cur, possible_clashes_table_query)
        print('Table Created :'.format(tmp_clash_table))
        return True
    return False

    
#Usings possible clashes table it resolves clashes by trumping
#chart events measurements with lab events measurements
#it first resolve numerical measurements clash using valuenum
#and then resolve categorical measurement clash using value                    
def resolve_clashes(cur):
    if find_possible_clashes(cur):
        print('Resolving clashes btw chart and lab events discharge measurements')
        afftet_table = 'd3sv1_chartevents_mv_dm'
        src_clash_table = 'd3sv1_poss_clash_chart_lab_dm'
        resl_num_clash_set_param = 'value=subquery.lval, valuenum=subquery.lvalnum'
        resl_cat_clash_set_param = 'value=subquery.lval'
        resl_num_clash_sub_query_where = 'cvalnum is not null and lvalnum is not null and cvalnum != lvalnum'
        resl_cat_clash_sub_query_where = 'cvalnum is null and lvalnum is null'
        resl_gen_query = "UPDATE {0} SET {2} "\
                    "FROM (select * from {1} "\
                    "WHERE {3}) AS subquery "\
                    "WHERE {0}.hadm_id=subquery.hid and {0}.itemid=subquery.iid and "\
                    "{0}.charttime=subquery.cht;"
        resl_num_clash_query = resl_gen_query.format(afftet_table, src_clash_table, resl_num_clash_set_param, resl_num_clash_sub_query_where)
        db_handler.make_opertional_query(cur, resl_num_clash_query)
        resl_cat_clash_query = resl_gen_query.format(afftet_table, src_clash_table, resl_cat_clash_set_param, resl_cat_clash_sub_query_where)
        db_handler.make_opertional_query(cur, resl_cat_clash_query)
        print('Clashes resolved')


#Call functions to calculate discharge measurements from measurement tables
#and also include labels to identify and resolve potential clashes
#between measurements.
def build(cur):
    calc_discharge_meas(cur)
    add_labels(cur)
    resolve_clashes(cur)
    create_measurement_type(cur)

