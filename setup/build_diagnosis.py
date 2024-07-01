import pandas as pd
import sys
# setting path for importing scripts in external folder
sys.path.insert(1, '../common')
import db_handler


# Read diagnosis code mappings for high level groups
def read_diagnosis_mappings():
    with open('diagnosis_group_mappings.txt') as fp:
        mappings = fp.read().splitlines()
    return mappings


# Map diagnosis icd9_code to higher level group using group mappings
def map_code_higher_group(mappings, code):
    code_int = 0
    if 'V' in code:
        return 18
    if 'E' in code:
        return 19
    if not code.isdigit():
        code_int = int(code[1:4])
    elif len(code) == 3:
        code_int = int(code)
    else:
        code_int = int(code[0:3])
    if code_int:
        for mapp in mappings:
            lwr_grp_bnd = int(mapp.split('\t')[0])
            hgr_grp_bnd = int(mapp.split('\t')[1])
            if code_int >= lwr_grp_bnd and code_int <= hgr_grp_bnd:
                return int(mapp.split('\t')[2])
    return 0


# Read diagnosis with shot title information table in a data frame
def get_diagnosis_table_with_icd_info(conn):
    diag_df_query = "SELECT distinct t1.icd9_code,t2.short_title FROM diagnoses_icd t1 " \
        "INNER JOIN d_icd_diagnoses t2 "\
        "ON t1.icd9_code = t2.icd9_code;"
    return db_handler.make_selection_query(conn, diag_df_query)


# Read notevents table in a data frame
def get_noteevents_table(conn):
    note_df_query = "SELECT row_id, subject_id, hadm_id, chartdate, "\
        "charttime, text FROM noteevents;"
    return db_handler.make_selection_query(conn, note_df_query)


# Insert matching note for a specific diagnosis relating to some admission
def insert_patient_diagnosis_time_table(cur, hadm_id,
                                        icd9_code, higher_group,
                                        timestamp, noteid,
                                        note_text_matched):
    pat_diag_time_insrt_row_query = "INSERT INTO d3sv1_patient_diagnosis_time "\
        "(hadm_id, icd9_code, higher_group, "\
        "timestamp, noteid, note_text_matched) "\
        "VALUES ({0}, '{1}', {2}, '{3}', {4}, '{5}');"\
        .format(hadm_id, icd9_code, higher_group, timestamp, noteid, note_text_matched)
    db_handler.make_opertional_query(cur, pat_diag_time_insrt_row_query)


# Create empty table which will encompass information for diagnosis
# matching with some note
def create_patient_diagnosis_time_table(cur):
    table = 'd3sv1_patient_diagnosis_time'
    if not db_handler.perform_table_check(cur, '{0}'.format(table)):
        print('Creating patients diagnosis time table as {0}'.format(table))
        pat_diag_time_table_query = "CREATE TABLE {0} ( "\
            "hadm_id INT NOT NULL, icd9_code VARCHAR(10), "\
            "higher_group INT NOT NULL, timestamp TIMESTAMP(0), "\
            "noteid INT NOT NULL, note_text_matched VARCHAR(50));".format(table)
        db_handler.make_opertional_query(cur, pat_diag_time_table_query)
        print('Patients diagnosis time table created')
        return True
    return False


# Find notes with matching diagnosis information and add into patient
# diagnosis table
def add_match_diagnosis_with_notes(conn, cur):
    diag_df = get_diagnosis_table_with_icd_info(conn)
    diag_map = read_diagnosis_mappings()
    note_df = get_noteevents_table(conn)
    note_df = note_df[note_df['hadm_id'].notna()]

    print('Matching notes to find individual diagnoses and their times for each patient end-diagnosis and populating Patients diagnosis time table, it will take a while')

    for row in diag_df.itertuples():

        icd9_code = getattr(row, 'icd9_code')
        higher_group = map_code_higher_group(diag_map, icd9_code)
        note_text_matched_org = getattr(row, 'short_title')
        note_text_matched = note_text_matched_org.replace("'", "_")

        note_df_tmp = note_df[note_df['text'].str.contains(
            note_text_matched_org, regex=False)]

        for nrow in note_df_tmp.itertuples():
            hadm_id = getattr(nrow, 'hadm_id')
            noteid = getattr(nrow, 'row_id')
            timestamp = getattr(nrow, 'charttime')

            if pd.isnull(timestamp):
                timestamp = getattr(nrow, 'chartdate')

            insert_patient_diagnosis_time_table(
                cur,
                hadm_id,
                icd9_code,
                higher_group,
                timestamp,
                noteid,
                note_text_matched)


# Build diagnosis vector by calling all required functions
def build(conn, cur):
    if create_patient_diagnosis_time_table(cur):
        add_match_diagnosis_with_notes(conn, cur)
