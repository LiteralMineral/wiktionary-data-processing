import settings
settings.init()
import DatabaseFill.dbprocedures

# other stuff
from functools import reduce
import pyspark.sql.functions as funcs
import pyarrow
from django.templatetags.i18n import language
from pyspark.sql.functions import array_contains
from pyspark.sql.types import MapType

# project procedures...
from DataProcessing.Steps import \
    utils, \
    downloads, \
    inflections, \
    database

from DatabaseFill import dbprocedures, SparkModels

import shutil


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

langs = [
    # "Arabic",
    # "Catalan",
    # "Chinese",
    "Finnish",
    # "French",
    # "German",
    # "Japanese",
    # "Korean",
    # "Latin",
    # "Mandarin",
    # "Polish",
    # "Portuguese",
    # "Russian",
    # "Spanish",
    # "Swedish",
]

# for lang in langs:
#     downloads.download(lang)
#     downloads.assign_ids(spark, lang)

# table = utils.load_dataset('Arabic','has_id_column', spark.read.parquet)
# inflections.analyze_inflection_tags(table, 'lang')


# all_langs = utils.build_collective_data(langs, 'has_id_column', spark.read.parquet)

# downloads.assign_ids(spark, 'Latin')



def testing_func(lang:str, filename):
    # load the data
    data = utils.load_dataset(lang, filename, spark.read.parquet)
    # do stuff to the data
    data = inflections.collect_inflection_tags(data)
    # data.show()
    # save the data
    utils.save_dataset(data, lang, "form_tags")
    pass

def word_forms_to_json(lang:str, filename):
    data = utils.load_dataset(lang, filename, spark.read.parquet)
    data = inflections.dataframe_to_word_forms(data)
    # data.show()
    # data.printSchema()
    utils.write_to_single_json(data, lang, "word_forms")


    # print(map.collectAsMap())
    # database.word_forms_to_database(data)
    # utils.write_to_single_csv(data, lang, "word_forms")



# utils.apply_to_data(langs, "has_id_column", testing_func, kwargs=None)

# all_data = utils.build_collective_data(langs, "form_tags", spark.read.parquet)
# all_data.show(50)
# utils.write_to_single_csv(all_data, 'All', "form_tags")
# utils.write_to_single_json(all_data, 'All', "form_tags")


# SparkModels.insert_json(f"{settings.dir_info['data_proc']}/word_forms/Russian_word_forms.json", None)

# utils.apply_to_data(langs, "has_id_column", word_forms_to_json, kwargs=None)



# tab = utils.load_dataset("French", 'form_tags', spark.read.parquet)
# tab = utils.load_dataset("French", 'has_id_column', spark.read.parquet)
# tab = inflections.extract_word_forms('Russian', spark.read.parquet)
# tab = inflections.extract_word_forms('French', spark.read.parquet)
# tab = inflections.extract_word_forms('Japanese', spark.read.parquet)
# tab = inflections.extract_word_forms('Finnish', spark.read.parquet)
# tab = inflections.extract_word_forms('Korean', spark.read.parquet)
# tab.where(funcs.array_contains(funcs.col('tags'), "Rōmaji")).show(500)
# tab.where(funcs.array_contains(funcs.col('tags'), "kyūjitai")).show(500)
# tab.where(funcs.array_contains(funcs.col('tags'), "")).show(500)
# tab.where(funcs.array_contains(funcs.col('tags'), "essive")).show(200)
# tab.where(funcs.isnotnull(funcs.col('ruby'))).show(200)
# tab.where((funcs.col('pos').contains("character"))).show(500)
# print(tab.where(funcs.isnotnull(funcs.col('word'))).count())
# print(tab.count())


# tab = tab.select(['entry_id', 'word', 'lang', 'forms'])
# forms_schema = tab.select(['entry_id', 'word', 'lang', 'forms']).schema
# flat_schema = utils.flatten_schema(forms_schema, data= tab)
# flat_schema.printSchema()

# exploded = utils.find_arrays(tab.schema)
# tab.printSchema()
# print(exploded)




# flat_tab = tab.select(flat_schema)
# flat_tab.show(truncate=500)
# tab.printSchema()
# flat_tab.printSchema()

# flat_tab = utils.apply_to_columns(flat_tab, flat_tab.columns, funcs.explode_outer)
# flat_tab = flat_tab

# col_name = 'forms'
# tab.printSchema()
# tab = tab.withColumn(col_name, funcs.explode_outer(col_name))
# col_name += ".tags"
# tab = tab.withColumn(col_name, funcs.explode_outer(col_name))
# tab.printSchema()

# flat_tab.show(truncate=500)
# flat_tab.printSchema()

# for lang in langs:
#     move(lang)

# tables = utils.load_datasets(langs, 'has_id_column', spark.read.parquet)
# for table in tables:
    # table.show()
    # table.printSchema()


# zipped_tables = zip(tables, langs)
# utils.save_datasets(zipped_tables, 'has_id_column')



# tables = utils.load_datasets(langs, 'has_id_column', spark.read.parquet)
# inflection_tags = utils.apply_to_df(tables, inflections.analyze_inflection_tags)
# zipped_inflections = zip(inflection_tags, langs)
# utils.save_datasets(zipped_inflections, "inflectional_tags_by_pos")

# for t in inflection_tags:
#     t.show()
# for t in zipped_inflections:
#     t[0].show()


# all_lang_tags = utils.join_language_data(langs, "inflection_tags_by_pos", spark)#, "lang")
# all_lang_tags.show()

# all_lang_tags = reduce(lambda acc_df, col_name: acc_df.withColumn(col_name,
#                                                                   funcs.concat_ws("\n", col_name)),
#                        all_lang_tags.columns,
#                        all_lang_tags)
# all_lang_tags.show()
# all_lang_tags.write.mode('overwrite')\
#     .option("header", True)\
#     .csv(f"{settings.dir_info['data_proc']}/inflection_tags_by_pos/All")



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

# Specifying create table column data types on write
# jdbcDF.write \
#     .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)") \
#     .jdbc("jdbc:postgresql:dbserver", "schema.tablename",
#           properties={"user": "username", "password": "password"})


# spark.stop()
