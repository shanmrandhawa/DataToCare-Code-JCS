import psycopg2
import pandas as pd
import os
path = os.getcwd()
path = path.split('experiments')[0] + 'common'


# Terminate the program as fatal query error occur
def terminate():
    print('Error in Query. Check terminal logs above.\n')
    input("Press Enter to continue...")
    sys.exit(1)  # existing with error as we cannot proceed further


# Establish a connection to the database and returning it on success and
# on query failure terminate.
def connect_db(hostname, port, username, password, database, schema):
    try:
        connection = psycopg2.connect(
            host=hostname,
            port=port,
            database=database,
            user=username,
            password=password,
            options=f'-c search_path={schema}'
        )
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    else:
        connection.autocommit = True
        return connection
    terminate()


# Make count, min, max, avg, sum  Query and return value as result on success and on query
# ffailure terminate.
def make_aggregate_query(cursor, query):
    try:
        cursor.execute(query)
        result = cursor.fetchone()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while making count query", error)
    else:
        return result[0]
    terminate()


# Make query that do changing operations like create_insert, delete,
# update and return operation status as 1 = success, and
# on query failure terminate.
def make_opertional_query(cursor, query):
    try:
        cursor.execute(query)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while making opertional query", error)
    else:
        return 1
    terminate()


# Make query that check if table exists and return table status as 1 =
# table exist, 0 = table not exists and
# on query failure terminate.
def make_checking_query(cursor, query):
    try:
        cursor.execute(query)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while making opertional query", error)
    else:
        return int(cursor.fetchone()[0])
    return None


# check whether table exists or not and return its status
def perform_table_check(cursor, table):
    exist_table_query = "SELECT EXISTS(SELECT * FROM information_schema.tables "\
        "WHERE table_name='{0}');".format(table)
    exist_table_status = make_checking_query(cursor, exist_table_query)
    return exist_table_status


# check whether column exists in a particular table or not and return its
# status
def perform_table_column_check(cursor, table, column):
    exist_table_column_query = "SELECT EXISTS (SELECT * FROM information_schema.columns "\
        "WHERE table_name='{0}' and column_name='{1}');".format(table, column)
    exist_table_column_status = make_checking_query(
        cursor, exist_table_column_query)
    return exist_table_column_status


# Make query and return selection in a dataframe on success, and
# on query failure terminate
def make_selection_query(connection, query):
    try:
        df = pd.read_sql_query(query, connection)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while getting dataframe through read query", error)
    else:
        return df
    terminate()


# Close both existing connection and cursor to PostgreSQL database.
def close_db_connection(connection, cursor):
    if (cursor):
        cursor.close()
        # print("Cursor is closed \n")
    if (connection):
        cursor.close()
        # print("PostgreSQL connection is closed \n")


# Read database connection parameters from file and return them in a tuple.
def read_db_parameters():
    with open(path + '/databse_connection_parameters.txt') as fp:
        parameters = fp.read().splitlines()
        hostname = parameters[0].split('= ')[1]
        port = int(parameters[1].split('= ')[1])
        username = parameters[2].split('= ')[1]
        password = parameters[3].split('= ')[1]
        database = parameters[4].split('= ')[1]
        schema = parameters[5].split('= ')[1]
    return (hostname, port, username, password, database, schema)


# Call read_db_parameters function and get parameters in a tuple, then
# unpack tuple and give parameters to connect_db function for database
# connection, in the end return this connection.
def intialize_database_handler():
    return connect_db(*read_db_parameters())
