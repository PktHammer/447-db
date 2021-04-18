from sqlalchemy import create_engine
import pymysql
import pandas as pd
import secrets_ignore
# Set UN/PW

# Global error codes
# Insert PW
ERRNO_PW_EXISTS = -1
# Query User
ERROR_USER_DNE = -2

def insert_user(username, password):
    # Error message
    ERROR_MESSAGE = "Sorry, this username has been taken"

    # Query
    engine_string = 'mysql+pymysql://' + \
                    secrets_ignore.user + ":" + \
                    secrets_ignore.password + "@" + \
                    secrets_ignore.ip_endpoint + "/" + \
                    secrets_ignore.db_name
    print(engine_string)
    engine = create_engine(engine_string)
    dbConnection = engine.connect()

    # Setup
    stmt = "SET @a = " + username + ";"
    result = dbConnection.execute(stmt)
    setup = "SET @b = " + password + ";"
    result = dbConnection.execute(stmt)

    # Check if UN exists
    result = dbConnection.execute("PREPARE cov_att_login from 'SELECT * from covid_user_accounts where username=?;';")
    result = pd.read_sql("EXECUTE cov_att_login using @a, @b;", dbConnection)

    # If the UN exists, fail
    fail = True
    if result.empty:
        fail = False
    if fail:
        print(ERROR_MESSAGE)
        dbConnection.close()
        return ERRNO_PW_EXISTS

        # If UN DNE, set UN/PW
    result = dbConnection.execute(
        "PREPARE cov_insert_user from 'INSERT INTO covid_user_accounts(username, password) VALUES(?,?);';")
    result = pd.read_sql("EXECUTE cov_insert_user using @a, @b;", dbConnection)
    dbConnection.close()
    return 1


# Get UN/PW
def query_user(username, password):
    engine_string = 'mysql+pymysql://' + \
                    secrets_ignore.user + ":" + \
                    secrets_ignore.password + "@" + \
                    secrets_ignore.ip_endpoint + "/" + \
                    secrets_ignore.db_name
    print(engine_string)
    engine = create_engine(engine_string)
    dbConnection = engine.connect()

    # Query
    stmt = "SET @a = " + username + ";"
    result = dbConnection.execute(stmt)
    setup = "SET @b = " + password + ";"
    result = dbConnection.execute(stmt)
    result = dbConnection.execute(
        "PREPARE cov_att_login from 'SELECT * from covid_user_accounts where username=? AND password=?;';")
    result = pd.read_sql("EXECUTE cov_att_login using @a, @b;", dbConnection)
    if result.empty:
        # Failed login, return error
        dbConnection.close()
        return ERROR_USER_DNE
    else:
        # Successful login, return token perhaps?  (SESS_ID token?)
        print("Remove me when this is completed")  # TODO: Do login work here

    dbConnection.close()
    return 1
