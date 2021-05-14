import pandas as pd
from sqlalchemy import create_engine
import pymysql
import db_config

def setup_stmt():
    engine_string = 'mysql+pymysql://' + db_config.user + ":" + db_config.password + "@" + db_config.ip_endpoint + "/" + db_config.db_name
    engine = create_engine(engine_string)
    dbConnection = engine.connect()
    result = dbConnection.execute("SET @a = 'California';")
    result = dbConnection.execute("PREPARE cov_read from 'SELECT * from main_covid_data where state=? limit 0,10;';")
    result2 = dbConnection.execute("EXECUTE cov_read USING @a;")

    # ### TODO: REMOVE, Debug stuff
    for v in result2:
        for column, value in v.items():
            print(f"{column}: {value}")
    print(result)
    # ### END

    # Close conn
    dbConnection.close()
    return

# Setup
if __name__ == "__main__":
    setup_stmt()
