# other stuff
from functools import reduce
import pyspark.sql.functions as funcs
import settings
import psycopg2
import DataProcessing.Steps
import configparser

# project procedures...
from DataProcessing.Steps import \
    utils, \
    downloads, \
    inflections  # \

settings.init()

# pyspark stuff
from pyspark.sql import SparkSession

# currently focused on the data processing side of the program...

spark = SparkSession.builder \
    .appName("PySpark Wiktionary Server") \
    .master("local[*]") \
    .config("spark.executor.cores", "1") \
    .config("spark.driver.memory", "4G") \
    .config('spark.sql.debug.maxToStringFields', 500) \
    .config('spark.sql.caseSensitive', True) \
    .getOrCreate()

## with database connection.... that's currently causing issues.
# spark = SparkSession.builder \
#     .appName("PostgreSQL Connection with PySpark") \
#     .master("local[*]") \
#     .config("spark.jars", spark_info['jdbc_jars']) \
#     .config("spark.executor.cores", "1") \
#     .config("spark.driver.memory", "4G")\
#     .config('spark.sql.debug.maxToStringFields', 500) \
#     .config('spark.sql.caseSensitive', True) \
#     .getOrCreate()

# lang = "All"
lang = "Russian"
# lang = "German"
# lang = "Spanish"
# lang = "Latin"
# lang = "French"
# lang = "Japanese"
# lang = "Arabic"
langs = [
    "Russian",
    "Korean",
    "Mandarin",
    "Chinese",
    "Catalan",
    "Portuguese",
    "Finnish",
    "Polish",
    "Swedish",
    "German",
    "Spanish",
    "Latin",
    "French",
    "Japanese",
    "Arabic"
]

# for lang in langs:
#     downloads.download(lang)
    # downloads.assign_ids(spark, lang)

# downloads.assign_ids(spark, 'Latin')


# tables = utils.load_datasets(langs, 'has_id_column', spark.read.parquet)
# inflection_tags = utils.apply_to_df(tables, inflections.analyze_inflection_tags)
# zipped_inflections = zip(inflection_tags, langs)
# utils.save_datasets(zipped_inflections, "inflectional_tags_by_pos")

# for t in inflection_tags:
#     t.show()
# for t in zipped_inflections:
#     t[0].show()


all_lang_tags = utils.join_language_data(langs, "inflectional_tags_by_pos", spark)#, "lang")
all_lang_tags.show()

all_lang_tags = reduce(lambda acc_df, col_name: acc_df.withColumn(col_name,
                                                                  funcs.concat_ws("\n", col_name)),
                       all_lang_tags.columns,
                       all_lang_tags)
all_lang_tags.show()
all_lang_tags.write.mode('overwrite')\
    .option("header", True)\
    .csv(f"{settings.dir_info['data_proc']}/All/inflection_tags_by_pos")



## initial download
# downloads.download(lang)

## add ids
# df = downloads.assign_ids(spark, lang)


## read from parquet if possible...
# df = spark.read.parquet(f"DataProcessing/Data/{lang}/has_id_column")


# df.show()

## read from postgresql server if possible
# df = spark.read.jdbc(pg_info['url'], f"wiktionary_info.{lang}_json_info", properties=db_props)

## analyze the tags....
# inflection_data = df.select(['entry_id', 'lang', 'word', 'pos', 'forms'])

# df.printSchema(1)
# inflection_data.show(20)

# tags = inflections.analyze_inflection_tags(inflection_data)

# tags.show(truncate=500)
# tags.

# tags = reduce(lambda acc_df, col_name: acc_df.withColumn(col_name, funcs.concat_ws("\n", col_name)),
#               tags.columns,
#               tags)

# tags.write.mode('overwrite')\
#     .parquet(f"DataProcessing/Data/{lang}/inflectional_tags_by_pos")
# tags.write.mode('overwrite')\
#     .option('header', True)\
#     .csv(f"DataProcessing/Data/{lang}/{lang}_inflectional_tags_by_pos.csv")


# tags.show()


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
