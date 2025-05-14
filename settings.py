import os
import sys
import configparser
import psycopg2


global config
global spark_info
global dir_info
global server_connection


def init():
    global config
    global spark_info
    global dir_info
    global server_connection

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    config = configparser.ConfigParser()
    config.read("config.ini")


    db_dict = config["POSTGRESQL"]
    # print(db_dict.items())
    pg_info = {
        "url": db_dict["URL"],
        "host": F"{db_dict['HOST']}:{db_dict['PORT']}",
        "database": db_dict["DATABASE"],
        "user": db_dict["USER"],
        "password": db_dict["PASSWORD"],
    }

    spark_dict = config["SPARK"]
    spark_info = {
        "master_url": spark_dict["MASTER_URL"],
        "jdbc_jars": spark_dict["JDBC_JARS"],
    }

    db_props = {
        "user": pg_info["user"],
        # "dbtable": "words",
        "password": pg_info["password"],
        "driver": "org.postgresql.Driver",
        "stringtype": "unspecified",
    }

    dir_dict = config["DIRECTORY"]

    # basically, variables for forming filepaths.
    dir_info = {
        "data_proc": dir_dict["DATA_PROC_DIR"],
        "data_end": dir_dict["END_DATA_DIR"]
    }

    # TODO: rewrite this so it works, lol
    ## from instructions.... probably won't use for a while
    # server_connection = psycopg2.connect(f"dbname=WiktionaryWordData USER={db_props['USER']}"
    #                                      f"password={db_props['password']}")

    # TODO: Make this happen gracefully. ie: check for the existence of the
    #  directories and create them if they don't exist, account for issues with
    #  the server connection...

    return None
