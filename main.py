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
    # "Arabic",
    # "Catalan",
    # "Chinese",
    # "Finnish",
    # "French",
    # "German",
    # "Japanese",
    # "Korean",
    # "Latin",
    # "Mandarin",
    # "Polish",
    # "Portuguese",
    "Russian",
    # "Spanish",
    # "Swedish"
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


for lang in langs:
    # load the word_forms data

    # select the tags for each tag category column....

    # save the dataset.

    pass

# data = InOut.load_parquet("Russian", "word_forms")
# data = InOut.load_parquet("Japanese", "word_forms")
# data = InOut.load_parquet("Chinese", "word_forms")
# data = InOut.load_parquet("Mandarin", "word_forms")
# data = InOut.load_parquet("Korean", "word_forms")
data = InOut.load_parquet("Finnish", "word_forms")
# data = InOut.load_parquet("German", "word_forms")
# data.show(500)
sorted_tags = inflections.sort_by_grammatical_feature(data)
# sorted_tags = sorted_tags.filter(sorted_tags.pos == "verb")
# sorted_tags = sorted_tags.filter(funcs.array_size("ruby") > 0 )
# sorted_tags = sorted_tags.filter(funcs.array_size("gender") > 1 )
# sorted_tags = sorted_tags.filter(funcs.array_size("usage_notes") > 0 )
sorted_tags = sorted_tags.filter(funcs.array_size("case") > 0 )
# sorted_tags = sorted_tags.filter()
# sorted_tags = sorted_tags.filter(((funcs.array_size("tags") > 0)
                                  # & (sorted_tags.pos != "romanization")
                                  # & (sorted_tags.pos != "character")
                                  # & (sorted_tags.pos != "name")
                                  # & (sorted_tags.pos != "symbol")
                                  # ))
# sorted_tags = sorted_tags.filter(funcs.array_contains(????, "colloquial"))
# sorted_tags = sorted_tags.filter(funcs.array_size("") > 0 )
sorted_tags.show(500, truncate="100")
# sorted_tags.show(500)
# sorted_tags.printSchema()


spark.stop()
