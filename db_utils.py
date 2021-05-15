import sqlalchemy
import db_config


# db_connect - Logs in with the main user
def db_connect() -> sqlalchemy.engine.base.Connection:
    engine_string = 'mysql+pymysql://' + db_config.user + ":" + db_config.password + "@" + db_config.ip_endpoint + "/" + db_config.db_name
    engine = sqlalchemy.create_engine(engine_string)
    dbConnection = engine.connect()
    return dbConnection

