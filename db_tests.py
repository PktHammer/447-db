import requests
import db_config
import db_utils
import db_logger
import db_table_initializers
import db_update_database
import db_update_more_databases
import database_queries_covid
import database_queries_users

#######################
# Testing

def create_user_test():
    database_queries_users.insert_user(username="TEST_USER_1", password="PASSWORD_1")
    database_queries_users.insert_user(username="TEST_USER_2", password="PASSWORD_2")

def query_user_tests():
    print(database_queries_users.query_user("TEST_USER_1", "PASSWORD_1"))
    print(database_queries_users.query_user("NOT_A_TEST_USER_1", "PASSWORD_2"))

def db_main_url_tests():
    test_1 = requests.get(db_config.MAIN_COVID_DATA_URL)
    if test_1.status_code != 200:
        db_logger.log_message("Test 1 Failed: Could not reach Main COVID County Data URL")
    test_2 = requests.get(db_config.MAIN_PRISON_DATA_URL)
    if test_2.status_code != 200:
        db_logger.log_message("Test 2 Failed: Could not reach Main COVID County Data URL")


if __name__ == "__main__":
    query_user_tests()
    # create_user_test()
    #  db_main_url_tests()
