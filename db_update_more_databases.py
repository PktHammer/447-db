import pandas as pd
import db_config
import sqlalchemy
import db_utils
import json
import db_logger
import db_return_codes

reserved_tables = [{db_config.COVID_DATA_TBL_NAME},
                   {db_config.PRISON_DATA_TBL_NAME},
                   {db_config.USER_DB_UPLOADS_TBL_NAME},
                   {db_config.USER_ACCOUNTS_TBL_NAME}]


# UAT: Remove User Table
def remove_user_table(rm_table_name: str, requesting_user: str= "NO_USER_SPECIFIED"):
    dbConnection = db_utils.db_connect()
    tn = json.dumps(rm_table_name)
    un = json.dumps(requesting_user)
    result = pd.read_sql(f"SELECT table_name FROM {db_config.USER_DB_UPLOADS_TBL_NAME} where table_name={tn} and username={un};", dbConnection)
    if not result.empty:
        delete_this_table = result['table_name'].tolist()[0]
        dbConnection.execute(f"DROP TABLE IF EXISTS {delete_this_table}")
        dbConnection.execute(f"DELETE FROM {db_config.USER_DB_UPLOADS_TBL_NAME} WHERE table_name={tn}")
        db_logger.log_message(f"Success: Deleted Table {delete_this_table}, requesting_user={requesting_user}")
        return f"Success: Deleted Table {delete_this_table}"
    else:
        db_logger.log_message(f"Invalid Delete of table {rm_table_name}, requesting_user={requesting_user}")
        return f"Invalid delete of table {rm_table_name}"


# UAT: Create new table
def create_new_table(csv_url: str, new_table_name: str, requesting_user: str="NO_USER_SPECIFIED"):
    # Perhaps check if the url is bad here:
    if new_table_name in reserved_tables:
        print(f"Sorry, {new_table_name} is reserved")
        return f"Sorry, {new_table_name} is reserved"

    # Connect
    dbConnection = db_utils.db_connect()

    # Check if table is in used_tables
    result = pd.read_sql(f"SELECT table_name FROM {db_config.USER_DB_UPLOADS_TBL_NAME};", dbConnection)
    list_of_used_tables = result['table_name'].tolist()
    if new_table_name in list_of_used_tables:
        # Fail
        print(f"Error, the table{new_table_name} is currently in use.  Please select a different table name.")
        return f"Error, the table{new_table_name} is currently in use.  Please select a different table name."
    else:
        # Successful login, return token perhaps?  (SESS_ID token?)
        pass
        # Continue

    try:
        df_new_table = pd.read_csv(csv_url)
    except Exception as e:  # TODO: Actually split the errors, currently it just fails
        return f"Error, URL could not be read"

    # Make new table
    df_new_table.to_sql(new_table_name, dbConnection, if_exists='replace')

    # Update USER_DB_UPLOADS_TBL
    tn = json.dumps(new_table_name)
    du = json.dumps(csv_url)
    un = json.dumps(requesting_user)

    try:
        result = dbConnection.execute(f"INSERT INTO {db_config.USER_DB_UPLOADS_TBL_NAME}(table_name, data_url, username) "
                                      f"VALUES ({tn}, {du}, {un});")
    except Exception as e: # On fail, rollback
        result = dbConnection.execute(f"DROP TABLE IF EXISTS {new_table_name}")
    dbConnection.close()
    db_logger.log_message(f"UAT Success: Created New Table {new_table_name} by {requesting_user}")
    return f"Success, Table Created: {new_table_name}"


# UAT: Update/Overwrite Table, written in SQLA Core
def update_table(table_name: str, requesting_user: str="NO_USER_SPECIFIED"):
    if table_name in reserved_tables:
        return "Sorry, this is managed"
    meta = sqlalchemy.MetaData()
    dbConnection, engine = db_utils.db_connect(ret_engine=True)
    user_uploads_table = sqlalchemy.Table(db_config.USER_DB_UPLOADS_TBL_NAME, meta, autoload_with=engine)
    s = sqlalchemy.select(user_uploads_table).where(sqlalchemy.and_(user_uploads_table.c.username == requesting_user, user_uploads_table.c.table_name == table_name))
    try:
        result = dbConnection.execute(s)
        if not result:
            return db_return_codes.UNHANDLED_ERROR
    except sqlalchemy.exc.IntegrityError as e:
        db_logger.log_error(e, "Error: DB SELECT failed")
        return db_return_codes.UAT_ERROR_SELECT_FAILED

    if result.rowcount == 0:
        return f"Error: no table {table_name} found created by {requesting_user}."

    elif result.rowcount == 1: # Execute
        table_name, target_url, username = result.fetchone()
        df_table_replacement = pd.read_csv(target_url)
        df_table_replacement.to_sql(table_name, dbConnection, if_exists='replace')
        return f"Successfully updated table {table_name} from {target_url}"
    else:
        return f"Error: Multiple matches found under {table_name}, please contact an administrator."


if __name__ == "__main__":
    pass
