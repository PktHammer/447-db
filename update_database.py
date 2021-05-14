import pandas as pd
from sqlalchemy import create_engine
import secrets_ignore
import sqlalchemy
import logging
import sys
import uuid

#######################
# Log write location
LOG_LOCATION = "./db.log"

#######################
# Defined Data Types
DTYPE_COVID_DATA = {
    "date": sqlalchemy.types.DATE,
    "county": sqlalchemy.types.VARCHAR(length=50),
    "state": sqlalchemy.types.VARCHAR(length=50),
    "fips": sqlalchemy.types.INT,
    "cases": sqlalchemy.types.INT,
    "death": sqlalchemy.types.INT
}

DTYPE_PRISON_DATA = {
    "name": sqlalchemy.types.VARCHAR(length=50),
    "date": sqlalchemy.types.DATE,
    "address": sqlalchemy.types.VARCHAR(length=100),
    "county": sqlalchemy.types.VARCHAR(length=100),
    "residents_confirmed": sqlalchemy.types.INT,
    "staff_confirmed": sqlalchemy.types.INT,
    "residents_active": sqlalchemy.types.INT,
    "staff_active": sqlalchemy.types.INT,
    "residents_death": sqlalchemy.types.INT,
    "staff_deaths": sqlalchemy.types.INT,
}

# Error codes
RETURN_ERROR_DB_CSV_READ = -1
RETURN_ERROR_DB_DATA_PROCESSING = -2
RETURN_ERROR_TOSQL = -3

# Table names
COVID_DATA_TABLE_NAME = "main_covid_data"
PRISON_DATA_TABLE_NAME = "main_prison_data"

#######################
# Helper Functions


# Connection function - TODO: Place in a util file
def db_connect():
    engine_string = 'mysql+pymysql://' + secrets_ignore.user + ":" + secrets_ignore.password + "@" + secrets_ignore.ip_endpoint + "/" + secrets_ignore.db_name
    engine = create_engine(engine_string)
    dbConnection = engine.connect()
    return dbConnection


# Error logging function, DEP: uuid
def log_error(exception, err_type, optional_message=""):
    with open(LOG_LOCATION, 'a') as f:
        err_uuid = uuid.uuid1() # Device + Timestamp in one
        f.write(f"ERROR: {err_uuid} :"
                f"{err_type}: {optional_message}. :\n"
                f"Exception: {exception}\n"
                f"END_ERROR: {err_uuid}\n")


def log_message(message):
    with open(LOG_LOCATION, 'a') as f:
        msg_uuid = uuid.uuid1()
        f.write(f"MESSAGE: {msg_uuid} {message} END_MESSAGE: {msg_uuid}\n")

#######################
# Main update function
def update():
    main_covid_data_url = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv"
    main_prison_data_url = "https://raw.githubusercontent.com/uclalawcovid19behindbars/historical-data/main/data/CA-historical-data.csv"

    try:
        df_covid = pd.read_csv(main_covid_data_url)
        df_prison = pd.read_csv(main_prison_data_url)
    except Exception as err:
        log_error(exception=err,
                  err_type="DB_CSV Read Error",
                  optional_message="Check Main COVID Data/Prison Data URLs")
        return RETURN_ERROR_DB_CSV_READ

    # Data Processing: Covid table
    try:
        df_covid = df_covid[df_covid['state'] == "California"]
        numeric_values = ['cases', 'deaths']
        df_covid[numeric_values] = df_covid[numeric_values].astype(int)

        df_covid.drop_duplicates(subset=['county', 'date'], keep='last')

        # Data Processing: Prison table
        kept_columns = ['Name', 'Date', 'Address', 'County', 'Residents.Confirmed', 'Staff.Confirmed', 'Residents.Active',
                        'Staff.Active', 'Residents.Deaths', 'Staff.Deaths']
        df_prison = df_prison[kept_columns]

        better_names = ['name', 'date', 'address', 'county', 'residents_confirmed', 'staff_confirmed', 'residents_active',
                        'staff_active', 'residents_deaths', 'staff_deaths']
        df_prison.columns = better_names

        numeric_values = ['residents_confirmed', 'staff_confirmed', 'residents_active', 'staff_active', 'residents_deaths',
                          'staff_deaths']
        df_prison[numeric_values] = df_prison[numeric_values].fillna(0)
        df_prison[numeric_values] = df_prison[numeric_values].astype(int)

        df_prison.dropna(subset=['address'], inplace=True)

        df_prison['name'] = df_prison['name'].str.title()
        df_prison['address'] = df_prison['address'].str.upper()
    except Exception as e:
        log_error(exception=e,
                  err_type="Data Processing Error",
                  optional_message="Column data names likely changed")
        return RETURN_ERROR_DB_DATA_PROCESSING

    # Connect
    dbConnection = db_connect()

    # # TODO: REMOVE INTERACTIVE TESTING WHEN READY
    # console = code.InteractiveConsole(dict(globals(), **locals()))
    # console.interact('Interactive shell for %s' %
    #                  os.path.basename(sys.argv[0]))

    # Update
    try:
        df_covid.to_sql(COVID_DATA_TABLE_NAME, dbConnection, if_exists='replace', dtype=DTYPE_COVID_DATA)
        df_prison.to_sql(PRISON_DATA_TABLE_NAME, dbConnection, if_exists='replace', dtype=DTYPE_PRISON_DATA)
    except TypeError as e:  # Table not properly made
        print("Error, table creation failed, data processing likely required")
        log_error(exception=e,
                  err_type="DB_TOSQL: Table Creation Error",
                  optional_message="Fixing data processing likely required")
        return RETURN_ERROR_TOSQL
    except ValueError as e:  # Table already exists, should not happen due to if_exists='replace'
        log_error(exception=e,
                  err_type="DB_TOSQL: ValueError",
                  optional_message="Table likely being replaced without if_exists='replace'")
        return RETURN_ERROR_TOSQL
    except Exception as e:
        log_error(exception=e,
                  err_type="DB_TOSQL: Unknown Error",
                  optional_message="Unknown error received")
        print("Unknown error")
        return RETURN_ERROR_TOSQL

    # Close conn
    dbConnection.close()
    success_uuid = uuid.uuid1()
    log_message(f"Success: Successful update.")
    return 0


#######################
# Testing
def db_tests():
    pass


#######################
# If ran, do update then tests
if __name__ == '__main__':
    # Read data from site
    update()
    # Run test suite
    log_message("Starting tests")
    db_tests()
    log_message("Tests completed")
    print("Tests Completed!")
