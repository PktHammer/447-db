import db_utils

#########################
# Table initializations


def initialize_user_table():
    dbConnection = db_utils.db_connect()
    result = dbConnection.execute("DROP TABLE IF EXISTS covid_user_accounts;")
    result = dbConnection.execute("CREATE TABLE covid_user_accounts ("
                                  "username varchar(255) NOT NULL,"
                                  "password varchar(64),"
                                  "PRIMARY KEY(username)"
                                  ");")
    dbConnection.close()
    return 0


# covid_user_accounts(username) -> user_db_uploads(username)
# Deleting a username from the accounts table will remove all user tables by a user
def initialize_user_db_uploads():
    dbConnection = db_utils.db_connect()
    result = dbConnection.execute("DROP TABLE IF EXISTS user_db_uploads")
    result = dbConnection.execute("CREATE TABLE user_db_uploads ("
                                  "table_name varchar(255) NOT NULL,"
                                  "data_url varchar(500) NOT NULL,"
                                  "username varchar(255) NOT NULL,"
                                  "FOREIGN KEY(username) REFERENCES covid_user_accounts(username) ON DELETE RESTRICT"
                                  ");")
    dbConnection.close()

def admin_drop_user_table_list(list_of_table_names: list) -> None:
    dbConnection = db_utils.db_connect()
    for item in list_of_table_names:
        result = dbConnection.execute(f"DROP TABLE IF EXISTS {item}")
        result = dbConnection.execute(f"DELETE FROM user_db_uploads WHERE table_name={item}")
    dbConnection.close()

def admin_get_list_user_tables(username: str) -> list:
    pass



if __name__ == "__main__":
    tables = ["delme_1", "delme_2", "delme_3"]
    admin_drop_user_table_list(list_of_table_names=tables)
    # initialize_user_table()
    # initialize_user_db_uploads()