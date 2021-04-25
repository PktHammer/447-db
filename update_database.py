import pandas as pd
from sqlalchemy import create_engine
import secrets_ignore
import datetime
import sqlalchemy

dtype_covid_data = {
	"date": sqlalchemy.types.DATE,
        "county": sqlalchemy.types.VARCHAR(length=50),
        "state": sqlalchemy.types.VARCHAR(length=50),
        "fips": sqlalchemy.types.INT,
        "cases": sqlalchemy.types.INT,
	"death": sqlalchemy.types.INT
}

dtype_prison_data = {
	"facility_id" : sqlalchemy.types.INT,
	"jurisdiction" : sqlalchemy.types.VARCHAR(length=50),
	"prison_name" : sqlalchemy.types.VARCHAR(length=50),
	"source" : sqlalchemy.types.VARCHAR(length=500),
	"residents_confirmed" : sqlalchemy.types.INT,
	"staff_confirmed" : sqlalchemy.types.INT,
	"staff_confirmed" : sqlalchemy.types.INT,
	"residents_death" : sqlalchemy.types.INT,
    "staff_death" : sqlalchemy.types.INT,
    "residents_recovered" : sqlalchemy.types.INT,
    "staff_recovered" : sqlalchemy.types.INT,
    "residents_tadmin" : sqlalchemy.types.INT,
    "staff_tested" : sqlalchemy.types.INT,
    "residents_negative" : sqlalchemy.types.INT,
    "staff_negative" : sqlalchemy.types.INT,
    "residents_pending" : sqlalchemy.types.INT,
    "staff_pending" : sqlalchemy.types.INT,
    "residents_quarantine" : sqlalchemy.types.INT,
    "staff_quarantine" : sqlalchemy.types.INT,
    "residents_active" : sqlalchemy.types.INT,
    "population_feb20" : sqlalchemy.types.INT,
    "residents_population" : sqlalchemy.types.INT,
    "residents_tested" : sqlalchemy.types.INT,
    "residents_initiated" : sqlalchemy.types.INT,
    "residents_completed" : sqlalchemy.types.INT,
    "residents_vadmin" : sqlalchemy.types.INT,
    "staff_initiated" : sqlalchemy.types.INT,
    "staff_completed" : sqlalchemy.types.INT,
    "staff_vadmin" : sqlalchemy.types.INT,
    "address" : sqlalchemy.types.VARCHAR(length=100),
    "zipcode" : sqlalchemy.types.INT,
    "city" : sqlalchemy.types.VARCHAR(length=20),
    "county" : sqlalchemy.types.INT,
    "latitude" : sqlalchemy.types.FLOAT,
    "longitude" : sqlalchemy.types.FLOAT,
    "county_fips" : sqlalchemy.types.INT,
    "hifld_id" : sqlalchemy.types.INT,
    "jurisdiction_scraper" : sqlalchemy.types.VARCHAR(length=20),
    "description" : sqlalchemy.types.VARCHAR(length=20),
    "security" : sqlalchemy.types.VARCHAR(length=20),
    "age" : sqlalchemy.types.VARCHAR(length=10),
    "is_different_operator" : sqlalchemy.types.BOOLEAN,
    "different_operator" : sqlalchemy.types.VARCHAR(length=20),
    "capacity" : sqlalchemy.types.VARCHAR(length=50),
    "bjs_id" : sqlalchemy.types.VARCHAR(length=50),
    "source_population_feb20" : sqlalchemy.types.VARCHAR(length=50),
    "source_capacity" : sqlalchemy.types.VARCHAR(length=50),
	"website" : sqlalchemy.types.VARCHAR(length=500),
	"ice_field_office" : sqlalchemy.types.VARCHAR(length=50)
}

def update():
    main_covid_data = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv"
    main_prison_data = "https://raw.githubusercontent.com/uclalawcovid19behindbars/historical-data/main/data/CA-historical-data.csv"

    # Extra opendata tables (json backends)
    non_prison_county_hospitalization = "https://data.ca.gov/api/3/action/datastore_search?resource_id=0d9be83b-5027-41ff-97b2-6ca70238d778"

    df_covid = pd.read_csv(main_covid_data)
    df_prison = pd.read_csv(main_prison_data)
    df_full_cty_hosp = pd.read_json(non_prison_county_hospitalization)
    df_full_cty_hosp.rename(columns={"todays_date": "date"})

    # Main tables
    covid_table_name = "main_covid_data"
    prison_table_name = "main_prison_data"

    engine_string = 'mysql+pymysql://' + secrets_ignore.user + ":" + secrets_ignore.password + "@" + secrets_ignore.ip_endpoint + "/" + secrets_ignore.db_name
    engine = create_engine(engine_string)
    dbConnection = engine.connect()
    send_frame_covid = df_covid.to_sql(covid_table_name, dbConnection, if_exists='replace', dtype=dtype_covid_data)
    send_frame_prison = df_prison.to_sql(prison_table_name, dbConnection, if_exists='replace', dtype=dtype_prison_data)
    # Close conn
    dbConnection.close()
    pass

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # Read data from site
    update()
    print("Hello world")
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
