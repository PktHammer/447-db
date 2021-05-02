from sqlalchemy import create_engine
import pymysql
import pandas as pd
import secrets_ignore

def query_state(return_type):
    engine_string = 'mysql+pymysql://' + \
                    secrets_ignore.user + ":" + \
                    secrets_ignore.password + "@" + \
                    secrets_ignore.ip_endpoint + "/" + \
                    secrets_ignore.db_name
    print(engine_string)
    engine = create_engine(engine_string)
    dbConnection = engine.connect()
    searchedDate = "'2020-01-28'"
    result = dbConnection.execute("SET @a = 'California';")
    setup = "SET @b = " + searchedDate + ";"
    print(setup)
    result = dbConnection.execute(setup)
    result = dbConnection.execute("PREPARE cov_read from 'SELECT * from main_covid_data where state=? AND date=? limit 0,10;';")
    covid_data_df = pd.read_sql("EXECUTE cov_read using @a, @b;", dbConnection)
    pd.set_option('display.expand_frame_repr', False)
    if return_type == "csv":
        return_this = covid_data_df.to_csv()
    elif return_type == "print":
        print(covid_data_df.to_csv())
        print(covid_data_df)
        return 0
    else:
        return_this = covid_data_df
    dbConnection.close()
    return return_this

def read_prison_by_date(return_type):
    engine_string = 'mysql+pymysql://' + \
                    secrets_ignore.user + ":" + \
                    secrets_ignore.password + "@" + \
                    secrets_ignore.ip_endpoint + "/" + \
                    secrets_ignore.db_name
    print(engine_string)
    engine = create_engine(engine_string)
    dbConnection = engine.connect()

    # Set variable
    searchedDate = "'2020-04-07'"
    setup = "SET @a = " + searchedDate + ";"
    print(setup)
    result = dbConnection.execute(setup)

    # Do query
    result = dbConnection.execute("PREPARE pris_read from 'SELECT * from main_prison_data where date=? limit 0,10;';")
    covid_data_df = pd.read_sql("EXECUTE pris_read using @a;", dbConnection)
    pd.set_option('display.expand_frame_repr', False)
    if return_type == "csv":
        return_this = covid_data_df.to_csv()
    elif return_type == "print":
        print(covid_data_df.to_csv())
        print(covid_data_df)
        return 0
    else:
        return_this = covid_data_df
    dbConnection.close()
    return return_this


def deduplicate():
    engine_string = 'mysql+pymysql://' + secrets_ignore.user + ":" + secrets_ignore.password + "@" + secrets_ignore.ip_endpoint + "/" + secrets_ignore.db_name
    engine = create_engine(engine_string)
    dbConnection = engine.connect()
    dbConnection.execute("")
# Setup
if __name__ == "__main__":
    # print(query_state("csv"))
    # query_state("print")
    read_prison_by_date("print")
