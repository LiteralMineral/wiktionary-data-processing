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

    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    config = configparser.ConfigParser()
    config.read('config.ini')

    db_dict = config["postgresql"]
    pg_info = {
        'url':      db_dict['url'],
        'host':     db_dict['host'],
        'database': db_dict['database'],
        'user':     db_dict['user'],
        'password': db_dict['password']
    }

    spark_dict = config['spark']
    spark_info = {
        'master_url': spark_dict['master_url'],
        'jdbc_jars':  spark_dict['jdbc_jars']
    }

    db_props = {
        "user":       pg_info["user"],
        # "dbtable": "words",
        "password":   pg_info["password"],
        "driver":     "org.postgresql.Driver",
        "stringtype": "unspecified"
    }

    dir_dict = config['directory']

    # basically, variables for forming filepaths.
    dir_info = {
        "data_proc": dir_dict["data_proc_dir"],
        "data_end":  dir_dict["end_data_dir"]
    }


    # TODO: rewrite this so it works, lol
    ## from instructions.... probably won't use for a while
    # server_connection = psycopg2.connect(f"dbname=WiktionaryWordData user={db_props['user']}"
    #                                      f"password={db_props['password']}")


    # TODO: Make this happen gracefully. ie: check for the existence of the
    #  directories and create them if they don't exist, account for issues with
    #  the server connection...

    return None
