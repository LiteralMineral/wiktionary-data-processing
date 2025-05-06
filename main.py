# other stuff
import os
import sys
import psycopg2
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
import configparser

# project procedures...
from DataProcessing import procedures

# pyspark stuff
from pyspark.sql import SparkSession

# currently focused on the data processing side of the program...


config = configparser.ConfigParser()
config.read('config.ini')

config_dat = config.sections()

db_dict = config["postgresql"]

pg_info = {
    'url': db_dict['url'],
    'host': db_dict['host'],
    'database': db_dict['database'],
    'user': db_dict['user'],
    'password': db_dict['password']
}

spark_dict = config['spark']
spark_info = {
    'master_url': spark_dict['master_url'],
    'jdbc_jars': spark_dict['jdbc_jars']
}

db_props = {
    "user": pg_info["user"],
    # "dbtable": "words",
    "password":  pg_info["password"],
    "driver": "org.postgresql.Driver",
    "stringtype": "unspecified"
}

## from instructions.... probably won't use for a while

conn = psycopg2.connect(f"dbname=WiktionaryWordData user={db_props['user']} "
                        f"password={db_props['password']}")


# appName =
spark_master_url = spark_info['master_url']
# print(spark_info['jdbc_jars'])

spark = SparkSession.builder \
    .appName("PySpark Wiktionary Server") \
    .master("local[*]") \
    .config("spark.executor.cores", "1") \
    .config("spark.driver.memory", "4G")\
    .config('spark.sql.debug.maxToStringFields', 500) \
    .config('spark.sql.caseSensitive', True) \
    .getOrCreate()
    # .config("spark.jars", spark_info['jdbc_jars']) \ # this would have come after the master url setting...

## with database connection.... that's currently causing issues.
# spark = SparkSession.builder \
#     .appName("PostgreSQL Connection with PySpark") \
#     .master("local[*]") \
#     .config("spark.jars", spark_info['jdbc_jars']) \ # this would have come after the master url setting...
#     .config("spark.executor.cores", "1") \
#     .config("spark.driver.memory", "4G")\
#     .config('spark.sql.debug.maxToStringFields', 500) \
#     .config('spark.sql.caseSensitive', True) \
#     .getOrCreate()

lang = "Russian"
# lang = "German"
# lang = "Spanish"
# lang = "French"

## initial download
# procedures.download(lang)

## add ids
# df = procedures.assign_ids(spark, lang)

## read from parquet if possible...
df = spark.read.parquet(f"Data/{lang}/has_id_column")

## read from postgresql server if possible
# df = spark.read.jdbc(pg_info['url'], f"wiktionary_info.{lang}_json_info", properties=db_props)

## analyze the tags....
inflection_data = df.select(['entry_id', 'word', 'pos', 'senses'])
inflection_data.show(50)
tags = procedures.collect_inflection_tags(inflection_data)








# schemas_df = spark.createDataFrame()
# columns = [f.name for f in df.schema.fields
#            if (isinstance(f.dataType, ArrayType) or isinstance(f.dataType, StructType))]

# json_data = df[columns]
# col_names = json_data.columns
# schema_strs = [{'column_name': col_name,
#                 'json_schema': df.select(col_name).schema.json()} for col_name in col_names]
# schema_capture = spark.createDataFrame(schema_strs)
# schema_capture.write.jdbc(url, f"wiktionary_info.{lang}_schema_info",
#                           properties=properties)
# schema_capture.show()





# df = spark.read.jdbc(url, f"wiktionary_info.{lang}_json_info", properties=db_props)



# json_data = df.withColumn('senses_json', funcs.from_json())

# json_data.printSchema(1)
# json_data.printSchema()

# procedures.save_basic_info(spark, lang, df, url, properties )

# json_df = procedures.convert_all_to_json(df)
#
# json_df.write.mode("overwrite").jdbc(url, table= f"wiktionary_info.{lang}_json_info",
#                             properties=properties)

# json_df = spark.read.jdbc(url, f"wiktionary_info.{lang}_json_info", properties=properties)
#
# json_df_schemas = procedures.get_all_json_schema(json_df)
# json_df_schemas.printSchema()
# json_df_schemas.show(10)
# print(json_df_schemas)

# json_df = procedures.convert_all_from_json(json_df)

# get a list of the columns that are json-able

# json_df.printSchema()
# json_df.show(10)
# json_df.write.mode("overwrite").jdbc(url, table= f"wiktionary_info.{lang}_json_info_trial",
#                             properties=properties)


# json_df = procedures.convert_to_json(df)
# json_df.explain()
# json_df.show(20, truncate= 50)
# json_df.show(20)
# json_df.printSchema()




# basic_info = spark.read.jdbc(url,table='word_entries',properties= properties)
# basic_info.printSchema()
# print(basic_info.count())

# basic_info.show(20)


# df.write.jdbc(url, "schema.demo_save",
#           properties=properties)

# Specifying create table column data types on write
# jdbcDF.write \
#     .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)") \
#     .jdbc("jdbc:postgresql:dbserver", "schema.tablename",
#           properties={"user": "username", "password": "password"})




spark.stop()