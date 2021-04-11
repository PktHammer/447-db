import pandas as pd
from sqlalchemy import create_engine
import secrets_ignore

dtype_covid_data = {
        "date": "DATE",
        "county": "CHAR(50)",
        "state": "CHAR(50)",
        "fips": int,
        "cases": int,
	"death": int
}

dtype_prison_data = {
	"facility_id" : int,
	"jurisdiction" : "CHAR(50)",
	"prison_name" : "CHAR(50)",
	"date" : "DATE",
	"source" : "TEXT(500)",
	"residents_confirmed" : int,
	"staff_confirmed" : int,
	"staff_confirmed" : int,
	"residents_death" : int,
        "staff_death" : int,
        "residents_recovered" : int,
        "staff_recovered" : int,
        "residents_tadmin" : int,
        "staff_tested" : int,
        "residents_negative" : int,
        "staff_negative" : int,
        "residents_pending" : int,
        "staff_pending" : int,
        "residents_quarantine" : int,
        "staff_quarantine" : int,
        "residents_active" : int,
        "population_feb20" : int,
        "residents_population" : int,
        "residents_tested" : int,
        "residents_initiated" : int,
        "residents_completed" : int,
        "residents_vadmin" : int,
        "staff_initiated" : int,
        "staff_completed" : int,
        "staff_vadmin" : int,
        "address" : "CHAR(200)",
        "zipcode" : int,
        "city" : "CHAR(50)",
        "county" : int,
        "latitude" : float,
        "longitude" : float,
        "county_fips" : int,
        "hifld_id" : int,
        "jurisdiction_scraper" : "CHAR(50)",
        "description" : "CHAR(50)",
        "security" : "CHAR(50)",
        "age" : "CHAR(50)",
        "is_different_operator" : int,
        "different_operator" : "CHAR(50)",
        "capacity" : "CHAR(50)",
        "bjs_id" : "CHAR(50)",
        "source_population_feb20" : "CHAR(50)",
        "source_capacity" : "CHAR(50)",
	"website" : "TEXT(500)",
	"ice_field_office" : "CHAR(50)"
}

def update():
    main_covid_data = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv"
    main_prison_data = "https://raw.githubusercontent.com/uclalawcovid19behindbars/historical-data/main/data/CA-historical-data.csv"
    df_covid = pd.read_csv(main_covid_data, dtype = dtype_covid_data)
    df_prison = pd.read_csv(main_prison_data, dtype = dtype_prison_data)

    covid_table_name = "main_covid_data"
    prison_table_name = "main_prison_data"
    engine_string = 'mysql+pymysql://' + secrets_ignore.user + ":" + secrets_ignore.password + "@" + secrets_ignore.ip_endpoint + "/" + secrets_ignore.db_name
    engine = create_engine(engine_string)
    dbConnection = engine.connect()
    send_frame_covid = df_covid.to_sql(covid_table_name, dbConnection, if_exists='replace')
    send_frame_prison = df_prison.to_sql(prison_table_name, dbConnection, if_exists='replace')

    # Close conn
    dbConnection.close()
    pass

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # Read data from site
    update()
    print("Hello world")
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
