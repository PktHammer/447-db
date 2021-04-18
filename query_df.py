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

# Setup
if __name__ == "__main__":
    # print(query_state("csv"))
    query_state("print")
