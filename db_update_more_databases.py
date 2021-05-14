import pandas as pd
from sqlalchemy import create_engine
import db_config
import datetime
import sqlalchemy
import db_utils

blacklist = []

used_tables = ["main_prison_data", "main_covid_data", "user_added_tables"]

# Test csv
vaccine_data_csv = "https://data.chhs.ca.gov/dataset/e283ee5a-cf18-4f20-a92c-ee94a2866ccd/resource/130d7ba2-b6eb-438d-a412-741bde207e1c/download/covid19vaccinesbycounty.csv"
vaccine_tbl_name = "main_vaccine_by_cty"

# TODO: Convert blacklist to database
# TODO: Convert new table update to store in database with url and requester name
# TODO: Restrict table removal to requester name

def initialize_user_added_tables():
    dbConnection = db_utils.db_connect()
    result = dbConnection.execute("CREATE TABLE user_added_tables ("
                                  "table_name varchar(255),"
                                  "url varchar(512),"
                                  "created_by_user varchar(255)"
                                  ");")
    dbConnection.close()
    return 0

def remove_from_blacklist(blacklist_url: str):
    if blacklist_url not in blacklist:
        blacklist.remove(blacklist_url)
        return f"Successfully removed {blacklist_url} from blacklist"
    else:
        return f"{blacklist_url} does not exist in blacklist"


def add_to_blacklist(blacklist_url: str):
    if blacklist_url not in blacklist:
        blacklist.append(blacklist_url)
        return f"Successfully added {blacklist_url} to blacklist"
    else:
        return f" Table {blacklist_url} already exists in blacklist"


def remove_table_restriction(table_name: str, requesting_user: str="NO_USER_SPECIFIED"):
    if table_name in used_tables:
        used_tables.remove(table_name)
        return f"Successfully removed table {table_name}"
    else:
        return f"Table {table_name} does not exist."

def remove_table_admin(table_name: str):
    if table_name in used_tables: # IF table match, drop
        used_tables.remove(table_name)
        return f"Successfully removed table {table_name}"
    else:
        return f"Table {table_name} does not exist."

def create_new_table(csv_url: str, table_name: str, requesting_user: str="NO_USER_SPECIFIED"):
    # Perhaps check if the url is bad here:
    if csv_url in blacklist:
        return "Error, this URL is not allowed"
    df_new_table = pd.read_csv(csv_url)

    if table_name in used_tables:
        return "Error, the table is in use"

    # Connect
    dbConnection = db_utils.db_connect()
    send_new_df = df_new_table.to_sql(table_name, dbConnection, if_exists='replace')
    dbConnection.close()



if __name__ == "__main__":
    # update_vaccine_data()
    pass
    # join_vaccine_covid()