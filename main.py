# other stuff
from functools import reduce
import pyspark.sql.functions as funcs
import pyarrow
from pyspark.sql.functions import array_contains
from pyspark.sql.types import MapType

import settings
import psycopg2
import DataProcessing.Modules
import configparser

# project procedures...
from DataProcessing.Modules import \
    utils, \
    downloads, \
    inflections,\
    InOut  # \

import shutil

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

InOut.init(spark)

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
# lang = "Russian"
# lang = "German"
# lang = "Spanish"
# lang = "Latin"
# lang = "French"
# lang = "Japanese"
# lang = "Arabic"
langs = [
    "Arabic",
    "Catalan",
    "Chinese",
    "Finnish",
    "French",
    "German",
    "Japanese",
    "Korean",
    "Latin",
    "Mandarin",
    "Polish",
    "Portuguese",
    "Russian",
    "Spanish",
    "Swedish"
]

# for lang in langs:
#     downloads.download(lang)
#     downloads.assign_ids(spark, lang)

# table = utils.load_dataset('Arabic','has_id_column', spark.read.parquet)
# inflections.analyze_inflection_tags(table, 'lang')


# all_langs = utils.build_collective_data(langs, 'has_id_column', spark.read.parquet)

# downloads.assign_ids(spark, 'Latin')




# def move(lang):
    # filename = 'inflectional_tags_by_pos'
    # df = utils.load_dataset(lang, filename, spark.read.parquet)
    # utils.save_dataset(df, lang, 'inflection_tags_by_pos')
    # print(lang)

    # shutil.move(f"{settings.dir_info['data_proc']}/{lang}/{lang}_kaikki_data.jsonl",
    #             f"{settings.dir_info['data_proc']}/json_files/{lang}_kaikki_data.jsonl")
    #
    # pass



def testing_func(lang:str, filename):
    # load the data
    data = InOut.load_parquet(lang, filename)
    # do stuff to the data
    data = inflections.collect_inflection_tags(data)
    data.show()
    # save the data
    InOut.save_parquet(data, lang, "form_tags")
    pass


# utils.load_apply_save(langs,
#                       "has_id_column",
#                       "word_forms",
#                       inflections.dataframe_to_word_forms)


# utils.load_union_save(langs,
#                       "form_tags",
#                       "All")



all_form_tags = InOut.load_parquet("All", "form_tags")
all_form_tags = all_form_tags.groupBy("form_tags").pivot("lang").agg(funcs.collect_set('pos'))
# for lang in langs:
#     lang_form_tags = all_form_tags.select(["form_tags", lang]).filter(funcs.isnotnull(lang))
#     lang_form_tags.show(40, truncate=100)


array_cols = utils.select_array_cols(all_form_tags)

all_langs = utils.reduce_columns(all_form_tags, "all_langs",
                                     array_cols, funcs.array_union)

all_langs = all_langs.withColumn("array_size", funcs.array_size("all_langs"))\
    .sort("array_size", ascending=False)\
    .drop("array_size")

all_langs = all_langs.withColumn("all_langs", funcs.concat_ws("\n", "all_langs"))

all_langs = all_langs.select("form_tags", "all_langs")
# all
InOut.write_to_single_csv(all_langs, "All", "form_tags_by_pos")

# all_langs = all_langs.select("form_tags", "all_langs").show(100, truncate=50)

# all_form_tags.show()
# col_map = utils.make_map_dict(array_cols, lambda x: funcs)


# all_form_tags = all_form_tags.withColumns(utils.concat_array_map(all_form_tags, "\n"))
# all_form_tags.show(100, truncate=200)

# for lang in langs:
#     lang_form_tags = all_form_tags.select(["form_tags", lang])\
#         .filter((funcs.isnotnull(lang)) & (funcs.col(lang) != ""))\
#         .sort("form_tags")
#     InOut.save_parquet(lang_form_tags, lang, "form_tags_by_pos")
    # InOut.write_to_single_csv(lang_form_tags, lang, "form_tags_by_pos")



    # lang_form_tags.show(100, truncate=50)
# for lang in langs:
#     inflections.sample_word_forms(lang, min_samples=100)







# sample = inflections.sample_word_forms("Russian", min_samples=100)




spark.stop()
