from sqlalchemy import create_engine
import pymysql
import pandas as pd
import db_config
import db_utils
import sqlalchemy
import json

'''SELECT MAX(date), county, state, fips, cases, deaths 
from (SELECT * from main_covid_data where DATE(date)<="2021-01-28") AS A 
group by county 
order by date;'''

def get_latest_update(date: str) -> pd.DataFrame:
    dbConnection = db_utils.db_connect()
    sql = '''SELECT MAX(date), county, state, fips, cases, deaths 
    from (SELECT * from main_covid_data where DATE(date)<= %(var)s) AS A 
    group by county 
    order by date;
    '''
    return pd.read_sql(sql=sql,con=dbConnection, params={"var": date})

def prepare_one(return_type: str, prepname: str, tbl_name: str, where_clause: str, var_a: str):
    dbConnection = db_utils.db_connect()

    # Set variable :: Ex: varA = "'2020-04-07'"
    setup = "SET @a = " + str(var_a) + ";"
    result = dbConnection.execute(setup)

    # Do query
    result = dbConnection.execute(
        "PREPARE " + str(prepname) + " from 'SELECT * from " + str(tbl_name) + " " + str(where_clause) + ";';"
    )
    covid_data_df = pd.read_sql("EXECUTE " + str(prepname) + " using @a;", dbConnection)

    # Display
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


def prepare_two(return_type: str = 'df', prepname: str = 'NA', tbl_name: str = 'NA', where_clause: str = '',
                var_a: str = '', var_b=''):
    dbConnection = db_utils.db_connect()

    # Set variable :: Ex: varA = "'2020-04-07'"
    setup = "SET @a = " + str(var_a) + ";"
    # print(setup)
    result = dbConnection.execute(setup)

    # Set variable :: Ex: var_b =
    setup = "SET @b = " + str(var_b) + ";"
    # print(setup)
    result = dbConnection.execute(setup)

    # Do query
    result = dbConnection.execute(
        "PREPARE " + str(prepname) + " from 'SELECT * from " + str(tbl_name) + " " + str(where_clause) + ";';"
    )
    covid_data_df = pd.read_sql("EXECUTE " + str(prepname) + " using @a, @b;", dbConnection)

    # Display
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


# Examples below
if __name__ == "__main__":
    date = "2021-01-28"
    print(get_latest_update(date))
    # prepare_one(return_type="print",
    #             prepname="whatv",
    #             tbl_name="main_covid_data",
    #             where_clause="where DATE(date)<=? ORDER BY date desc",
    #             var_a="'2021-01-28'")
    # Return type can return a csv (csv) or print (print) or a dataframe (anything else)
    # prepname needs to be unique for the query -- TODO: actually prepare all queries and delete the prep creation lines
    # tbl_name is the name of the table in the database
    # where_clause is remaining sql to be executed -- variables will be assigned as ?s
    # var_a is the first variable (?) to be replaced
    # var_b is the second variable (?) to be replaced
    # Examples:
    # prepare_one(return_type="print",
    #             prepname="sdtest",
    #             tbl_name="main_covid_data",
    #             where_clause="where county=? limit 10",
    #             var_a='"Alameda"')
    # prepare_two(return_type="print",
    #             prepname="sdtest2",
    #             tbl_name="main_covid_data",
    #             where_clause="where county=? and date=? limit 10",
    #             var_a='"Alameda"',
    #             var_b="'2020-12-15'")
