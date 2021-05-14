import db_update_database
import requests
import db_logger
import db_config

#######################
# Testing
def db_tests():
    test_1 = requests.get(db_config.MAIN_COVID_DATA_URL)
    if test_1.status_code != 200:
        db_logger.log_message("Test 1 Failed: Could not reach Main COVID County Data URL")
    test_2 = requests.get(db_config.MAIN_PRISON_DATA_URL)
    if test_2.status_code != 200:
        db_logger.log_message("Test 2 Failed: Could not reach Main COVID County Data URL")


if __name__ == "__main__":
    db_tests()
