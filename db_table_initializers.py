import db_config
import db_utils

#########################
# Table initializations


def initialize_user_table():
    dbConnection = db_utils.db_connect()
    result = dbConnection.execute(f"DROP TABLE IF EXISTS {db_config.USER_ACCOUNTS};")
    result = dbConnection.execute(f"CREATE TABLE {db_config.USER_ACCOUNTS} ("
                                  f"username varchar(255) NOT NULL,"
                                  f"password varchar(64),"
                                  f"PRIMARY KEY(username)"
                                  f");")
    dbConnection.close()
    return 0


# covid_user_accounts(username) -> user_db_uploads(username)
# Deleting a username from the accounts table will remove all user tables by a user
def initialize_user_db_uploads():
    dbConnection = db_utils.db_connect()
    result = dbConnection.execute(f"DROP TABLE IF EXISTS {db_config.USER_DB_UPLOADS}")
    result = dbConnection.execute(f"CREATE TABLE {db_config.USER_DB_UPLOADS} ("
                                  f"table_name varchar(255) NOT NULL,"
                                  f"data_url varchar(500) NOT NULL,"
                                  f"username varchar(255) NOT NULL,"
                                  f"FOREIGN KEY(username) REFERENCES {db_config.USER_ACCOUNTS}(username) ON DELETE RESTRICT"
                                  f");")
    dbConnection.close()

def admin_drop_user_table_list(list_of_table_names: list) -> None:
    dbConnection = db_utils.db_connect()
    for item in list_of_table_names:
        result = dbConnection.execute(f"DROP TABLE IF EXISTS {item}")
        result = dbConnection.execute(f"DELETE FROM {db_config.USER_DB_UPLOADS} WHERE table_name={item}")
    dbConnection.close()

def admin_get_list_user_tables(username: str) -> list:
    pass



if __name__ == "__main__":
    tables = ["delme_1", "delme_2", "delme_3"]
    admin_drop_user_table_list(list_of_table_names=tables)
    # initialize_user_table()
    # initialize_user_db_uploads()