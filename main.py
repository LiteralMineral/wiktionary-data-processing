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
    InOut, \
    senses

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


data = InOut.load_parquet("Japanese", "has_id_column")
# non_arrays = [item[0] for item in data.dtypes if not (item[1].startswith("array") or item[1].startswith("struct"))]
# non_arrays.append("senses")

print(data.columns)
data.printSchema(3)

items = senses.get_definitions(data)
items.show(truncate=200)
items.printSchema()
columns = items.columns

# items.select([col for col in items.columns if col.startswith("senses/related")]).show()


# for col in columns:
#     print(col)
#     items.filter(funcs.isnotnull(col)).show(truncate=200)

# items.show()

# senses.show()


spark.stop()
