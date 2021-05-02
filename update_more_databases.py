import pandas as pd
from sqlalchemy import create_engine
import secrets_ignore
import datetime
import sqlalchemy

vaccine_data_csv = "https://data.chhs.ca.gov/dataset/e283ee5a-cf18-4f20-a92c-ee94a2866ccd/resource/130d7ba2-b6eb-438d-a412-741bde207e1c/download/covid19vaccinesbycounty.csv"

def update_vaccine_data():
    df_vaccine_cty = pd.read_csv(vaccine_data_csv)

    vaccine_tbl_name = "main_vaccine_by_cty"

    engine_string = 'mysql+pymysql://' + secrets_ignore.user + ":" + secrets_ignore.password + "@" + secrets_ignore.ip_endpoint + "/" + secrets_ignore.db_name
    engine = create_engine(engine_string)
    dbConnection = engine.connect()
    print(df_vaccine_cty.head(10))
    #df_vaccine_cty.drop(columns="index")
    send_frame_covid = df_vaccine_cty.to_sql(vaccine_tbl_name, dbConnection, if_exists='replace')
    # print(f"ALTER TABLE {vaccine_tbl_name} DROP COLUMN index;")
    # dbConnection.execute(f"ALTER TABLE {vaccine_tbl_name} DROP COLUMN index;")
    dbConnection.close()

def join_vaccine_covid():
    engine_string = 'mysql+pymysql://' + secrets_ignore.user + ":" + secrets_ignore.password + "@" + secrets_ignore.ip_endpoint + "/" + secrets_ignore.db_name
    engine = create_engine(engine_string)
    dbConnection = engine.connect()
    dbConnection.execute("CREATE VIEW joint_vaccine_covid AS SELECT 'index' id1 FROM main_vaccine_by_cty A "
                         "LEFT JOIN main_covid_data B "
                         "ON A.county = B.county and A.administered_date = B.date;")
    dbConnection.close()

# def drop_indx():


if __name__ == "__main__":
    update_vaccine_data()

    # join_vaccine_covid()