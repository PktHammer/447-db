import db_utils
import json
import db_config
import pandas as pd


def admin_drop_all_tables_in_list(list_of_table_names: list) -> None:
    dbConnection = db_utils.db_connect()
    for item in list_of_table_names:
        item_parsed = json.dumps(item)
        result = dbConnection.execute(f"DROP TABLE IF EXISTS {item}")
        result = dbConnection.execute(f"DELETE FROM {db_config.USER_DB_UPLOADS} WHERE table_name={item_parsed}")
    dbConnection.close()


def admin_get_user_tables_by_un(username: str) -> list:
    dbConnection = db_utils.db_connect()
    username_parsed = json.dumps(username)
    # Check if table is in used_tables
    result = pd.read_sql(f"SELECT table_name FROM {db_config.USER_DB_UPLOADS} where username={username_parsed};",
                         dbConnection)
    dbConnection.close()
    return result['table_name'].tolist()


def admin_get_all_user_tables() -> list:
    dbConnection = db_utils.db_connect()
    # Check if table is in used_tables
    result = pd.read_sql(f"SELECT table_name FROM {db_config.USER_DB_UPLOADS};", dbConnection)
    dbConnection.close()
    return result['table_name'].tolist()


def admin_drop_all_user_tables() -> None:
    list_of_user_tables = admin_get_all_user_tables()
    admin_drop_all_tables_in_list(list_of_user_tables)


def admin_drop_all_user_tables_by_un(username: str) -> None:
    tables_to_drop = admin_get_user_tables_by_un(username=username)
    admin_drop_all_tables_in_list(list_of_table_names=tables_to_drop)


if __name__ == "__main__":
    tables = ["delme_1", "delme_2", "delme_3"]
    admin_drop_all_user_tables()
    print(admin_get_all_user_tables())