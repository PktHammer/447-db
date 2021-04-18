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