import requests
import db_config
# import db_utils
import db_logger
# import db_update_database
import db_update_more_databases
import database_queries_covid
import database_queries_users as database_queries_users
import random
import string
import db_return_codes

#######################
# Test URLs
vaccine_data_csv = "https://data.chhs.ca.gov/dataset/e283ee5a-cf18-4f20-a92c-ee94a2866ccd/resource/130d7ba2-b6eb-438d-a412-741bde207e1c/download/covid19vaccinesbycounty.csv"
vaccine_tbl_name = "main_vaccine_by_cty"


#######################
# Test Helper Functions

support_set = string.ascii_letters + string.digits + string.punctuation

def generate_user():
    test_user_un = ''.join(random.choice(support_set) for i in range(100))
    test_user_pw = ''.join(random.choice(support_set) for i in range(60))
    return test_user_un, test_user_pw


def test_is_failing(param1, param2, expected_retcode):
    if param1 != expected_retcode or param2 != expected_retcode:
        return True
    else:
        return False


#######################
# Tests
def perform_user_tests() -> list:
    failed_tests = []

    # Generate random unique username & passwords
    test_user_1_un, test_user_1_pw = generate_user()
    test_user_2_un, test_user_2_pw = generate_user()

    while test_user_1_un == test_user_2_un or test_user_1_pw == test_user_2_pw:
        test_user_1_un, test_user_1_pw = generate_user()
        test_user_2_un, test_user_2_pw = generate_user()

    # Test valid insert
    test_1 = "User Account Insert User Test: Valid Test"
    res1 = database_queries_users.insert_user(username=test_user_1_un, password=test_user_1_pw)
    res2 = database_queries_users.insert_user(username=test_user_2_un, password=test_user_2_pw)
    if test_is_failing(res1, res2, db_return_codes.UA_INSERT_SUCCESS):
        failed_tests.append(test_1)

    # Test invalid insert
    test_2 = "User Account Insert User Test: Error Test"
    res1 = database_queries_users.insert_user(username=test_user_1_un, password=test_user_2_pw)
    res2 = database_queries_users.insert_user(username=test_user_2_un, password=test_user_1_pw)
    if test_is_failing(res1, res2, db_return_codes.UA_INSERT_FAILED_DUPLICATE):
        failed_tests.append(test_2)

    # Test valid login
    test_3 = "User Account Login: Valid Test"
    res1 = database_queries_users.query_user(username=test_user_1_un, password=test_user_1_pw)
    res2 = database_queries_users.query_user(username=test_user_2_un, password=test_user_2_pw)
    if test_is_failing(res1, res2, db_return_codes.UA_LOGIN_SUCCESS):
        failed_tests.append(test_3)

    # Test invalid login
    test_4 = "User Account Login: Error Test"
    res1 = database_queries_users.query_user(username=test_user_1_un, password=test_user_2_pw)
    res2 = database_queries_users.query_user(username=test_user_2_un, password=test_user_1_pw)
    if test_is_failing(res1, res2, db_return_codes.UA_LOGIN_FAILED):
        failed_tests.append(test_4)

    # Test invalid delete (Failed deletes - invalid username, invalid password)
    test_5 = "User Account Delete: Error Test"
    test_fail_un, _ = generate_user()
    res1 = database_queries_users.delete_user(username=test_fail_un, password=test_user_1_pw)
    res2 = database_queries_users.delete_user(username=test_user_1_un, password=test_user_2_pw)
    if test_is_failing(res1, res2, db_return_codes.UA_DELETE_USER_FAILED):
        failed_tests.append(test_5)

    # Test valid delete (Successful deletes - user 1 & user 2)
    test_6 = "User Account Delete: Valid Test - WARNING: MAY REQUIRE MANUAL CLEANING IF INSERT PASSED"
    res1 = database_queries_users.delete_user(username=test_user_1_un, password=test_user_1_pw)
    res2 = database_queries_users.delete_user(username=test_user_2_un, password=test_user_2_pw)
    if test_is_failing(res1, res2, db_return_codes.UA_DELETE_USER_SUCCESS):
        failed_tests.append(test_6)
    return failed_tests


def db_main_url_tests() -> list:
    failed_tests = []
    test_1 = requests.get(db_config.MAIN_COVID_DATA_URL)
    if test_1.status_code != 200:
        db_logger.log_message("Test 1 Failed: Could not reach Main COVID County Data URL")
        failed_tests.append("DB_MAIN_URL TEST: County URL/Connectivity Test FAILED")
    test_2 = requests.get(db_config.MAIN_PRISON_DATA_URL)
    if test_2.status_code != 200:
        db_logger.log_message("Test 2 Failed: Could not reach Main COVID Prison Data URL")
        failed_tests.append("DB_MAIN_URL TEST: Prison URL/Connectivity Test FAILED")
    return failed_tests


def db_main_query_tests() -> list:
    failed_tests = []
    # Single Param Tests
    t1_name = "Prep query Single Param: Valid Test"
    test_1 = database_queries_covid.prepare_one(return_type='df',
                                                prepname="sdtest",
                                                tbl_name="main_covid_data",
                                                where_clause="where county=? limit 10",
                                                var_a='"Alameda"')
    if test_1.empty:
        failed_tests.append(t1_name)
    t1_f_name = "Prep query Single Param: No Result Test"
    test_1_f = database_queries_covid.prepare_one(return_type='df',
                                                  prepname="sdtest2",
                                                  tbl_name="main_covid_data",
                                                  where_clause="where county=? limit 10",
                                                  var_a='"NowhereNotHere"')
    if not test_1_f.empty:
        failed_tests.append(t1_f_name)

    # Double Param Tests
    t2_name = "Prep query Double Param: Valid Test"
    test_2 = database_queries_covid.prepare_two(return_type='df',
                                                prepname="sdtest3",
                                                tbl_name="main_covid_data",
                                                where_clause="where county=? and date=? limit 10",
                                                var_a='"Alameda"',
                                                var_b="'2020-12-15'")
    if test_2.empty:
        failed_tests.append(t2_name)

    t2_f_name = "Prep query Double Param: No Result Test"
    test_2_f = database_queries_covid.prepare_two(return_type='df',
                                                  prepname="sdtest4",
                                                  tbl_name="main_covid_data",
                                                  where_clause="where county=? and date=? limit 10",
                                                  var_a='"Alameda"',
                                                  var_b="'1315-12-15'")
    if not test_2_f.empty:
        failed_tests.append(t2_f_name)

    # Return failed tests list
    return failed_tests


def db_UAT_tests():
    failed_tests = []
    # Test csv
    test_data_csv_url = "https://data.chhs.ca.gov/dataset/e283ee5a-cf18-4f20-a92c-ee94a2866ccd/resource/130d7ba2-b6eb-438d-a412-741bde207e1c/download/covid19vaccinesbycounty.csv"
    test_tbl_name = "oaisjdfoajsodifjoaiwaskdjfei"

    test_username, test_password = generate_user()

    # update_vaccine_data()
    test_0 = f"UAT Test 0: Valid Make User"
    res0 = database_queries_users.insert_user(username=test_username,
                                              password=test_password)
    if res0 != db_return_codes.UA_INSERT_SUCCESS:
        failed_tests.append(test_0)

    test_1 = f"UAT Test 1: Valid Create Table"
    res1 = db_update_more_databases.create_new_table(csv_url=test_data_csv_url,
                                                     new_table_name=test_tbl_name,
                                                     requesting_user=test_username)
    if res1 != f"Success, Table Created: {test_tbl_name}":
        failed_tests.append(test_1)

    test_2 = f"UAT Test 2: Valid Update Table"
    res2 = db_update_more_databases.update_table(table_name=test_tbl_name,
                                                 requesting_user=test_username)
    if res2 != f"Successfully updated table {test_tbl_name} from {test_data_csv_url}":
        failed_tests.append(test_2)

    test_3 = f"UAT Test 3: Valid Remove User Table"
    res3 = db_update_more_databases.remove_user_table(rm_table_name=test_tbl_name,
                                                      requesting_user=test_username)
    if res3 != f"Success: Deleted Table {test_tbl_name}":
        failed_tests.append(test_3)

    test_0_2 = f"UAT Test 0-2: Remove Make User: WARNING: IF THIS FAILS AND TEST 0 SUCCEEDS, JUNK USER {test_username} MADE"
    res4 = database_queries_users.delete_user(username=test_username,
                                              password=test_password)
    if res4 != db_return_codes.UA_DELETE_USER_SUCCESS:
        failed_tests.append(test_0_2)
    return failed_tests


# If ran, do test suite
if __name__ == "__main__":
    failed_test_list = []
    failed_test_list.extend(perform_user_tests())
    failed_test_list.extend(db_main_url_tests())
    failed_test_list.extend(db_main_query_tests())
    failed_test_list.extend(db_UAT_tests())
    print(f"Failing tests: " + str(failed_test_list))
    if len(failed_test_list) > 0:
        db_logger.log_message(f"Failing tests: {str(failed_test_list)}", output_location=db_logger.LOG_LOCATION_TESTS)