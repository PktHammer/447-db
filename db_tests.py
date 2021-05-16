import requests
import db_config
import db_utils
import db_logger
import db_table_initializers
import db_update_database
import db_update_more_databases
import database_queries_covid
import database_queries_users as database_queries_users
import random
import string
#######################
# Testing

def generate_user():
    test_user_un = ''.join(random.choice(string.ascii_letters) for i in range(100))
    test_user_pw = ''.join(random.choice(string.printable) for i in range(60))
    return test_user_un, test_user_pw

def perform_user_tests():

    # Generate random unique username & passwords
    test_user_1_un, test_user_1_pw = generate_user()
    test_user_2_un, test_user_2_pw = generate_user()

    while test_user_1_un == test_user_2_un or test_user_1_pw == test_user_2_pw:
        test_user_1_un, test_user_1_pw = generate_user()
        test_user_2_un, test_user_2_pw = generate_user()

    # Test valid
    database_queries_users.insert_user(username=test_user_1_un, password=test_user_1_pw)
    database_queries_users.insert_user(username=test_user_2_un, password=test_user_2_pw)

    # Test invalid
    database_queries_users.query_user(username=test_user_1_un, password=test_user_2_pw)
    database_queries_users.query_user(username=test_user_2_un, password=test_user_1_pw)



def query_user_tests():
    database_queries_users.query_user("TEST_USER_1", "PASSWORD_1")
    database_queries_users.query_user("NOT_A_TEST_USER_1", "PASSWORD_2")


def db_main_url_tests():
    test_1 = requests.get(db_config.MAIN_COVID_DATA_URL)
    if test_1.status_code != 200:
        db_logger.log_message("Test 1 Failed: Could not reach Main COVID County Data URL")
    test_2 = requests.get(db_config.MAIN_PRISON_DATA_URL)
    if test_2.status_code != 200:
        db_logger.log_message("Test 2 Failed: Could not reach Main COVID Prison Data URL")


if __name__ == "__main__":
    # print(query_user_tests())
    perform_user_tests()
    # create_user_test()
    #  db_main_url_tests()
