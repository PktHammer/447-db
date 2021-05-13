import pandas as pd
from sqlalchemy import create_engine
import secrets_ignore
import datetime
import sqlalchemy
import code
import os
import sys

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
    # Used
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


def update_from_link(link: str, renames: dict, table_name: str):
    df = pd.read_csv(link)
    if renames is not False:
        df.rename(columns=renames)


def connectDB():
    engine_string = 'mysql+pymysql://' + secrets_ignore.user + ":" + secrets_ignore.password + "@" + secrets_ignore.ip_endpoint + "/" + secrets_ignore.db_name
    engine = create_engine(engine_string)
    dbConnection = engine.connect()
    return dbConnection


def update():
    main_covid_data = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv"
    main_prison_data = "https://raw.githubusercontent.com/uclalawcovid19behindbars/historical-data/main/data/CA-historical-data.csv"
    non_prison_county_hospitalization = "https://data.ca.gov/api/3/action/datastore_search?resource_id=0d9be83b-5027-41ff-97b2-6ca70238d778"

    df_covid = pd.read_csv(main_covid_data)
    df_prison = pd.read_csv(main_prison_data)

    # Data Processing
    # covid table: Drop non-CA results
    df_covid = df_covid[df_covid['state'] == "California"]
    numeric_values = ['cases', 'deaths']
    df_covid[numeric_values] = df_covid[numeric_values].astype(int)

    # Prison table:
    # Drop anything without an address
    # Keep tables for Name, Address, County, Date, Residents.Confirmed, Staff.Confirmed, Residents.Active, Staff.Deaths
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

    # Main tables
    covid_table_name = "main_covid_data"
    prison_table_name = "main_prison_data"

    # Connect
    dbConnection = connectDB()

    # # TODO: REMOVE INTERACTIVE TESTING WHEN READY
    # console = code.InteractiveConsole(dict(globals(), **locals()))
    # console.interact('Interactive shell for %s' %
    #                  os.path.basename(sys.argv[0]))

    # Deduplication
    df_covid.drop_duplicates(subset=['county', 'date'], keep='last')

    # Update
    try:
        send_frame_covid = df_covid.to_sql(covid_table_name, dbConnection, if_exists='replace', dtype=DTYPE_COVID_DATA)
        send_frame_prison = df_prison.to_sql(prison_table_name, dbConnection, if_exists='replace',
                                             dtype=DTYPE_PRISON_DATA)
    except TypeError:  # Table not properly made
        print("Error, table creation failed, data processing likely required")
    except ValueError:  # Table already exists, should not happen due to if_exists='replace'
        pass
    # Close conn
    dbConnection.close()
    return 0


if __name__ == '__main__':
    # Read data from site
    update()

    print("Update Success!")
