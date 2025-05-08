# other stuff
import string
from functools import reduce
import psycopg2
from configparser import ConfigParser
import json
import requests
import os
import settings





# pyspark stuff
from types import *
from typing import Dict
from pyspark.sql import *
from pyspark.sql import SparkSession, \
    functions as funcs
from pyspark.sql.types import *




# Download and assign unique identifiers to the data
# TODO: Allow user to define the directory in which the file is written
# TODO: create directory if it doesn't exist!
def collect_file(lang: string):
    # download
    if lang == 'All':
        url = "https://kaikki.org/dictionary/All%20languages%20combined/kaikki.org-dictionary-all.jsonl"
        print(f"File is too large to download nicely. Download it yourself and name it {lang}_kaikki_data.jsonl")
    else:
        url = f"https://kaikki.org/dictionary/{lang}/words/kaikki.org-dictionary-{lang}-words.jsonl"

    response = requests.get(url)
    if response.status_code == 200:
        with open(f"{settings.dir_info['data_proc']}/{lang}/{lang}_kaikki_data.jsonl", 'wb') as file:
            file.write(response.content)
    else:
        print("could not get file")
        Exception("could not get file")


def download(lang: string):
    filepath=f"{settings.dir_info['data_proc']}/{lang}/{lang}_kaikki_data.jsonl"
    if os.path.exists(filepath):
        print(f"{filepath} exists already, not redownloading.")
    else:
        print(f"downloading {lang} dataset and saving to {filepath}")
        # TODO: make sure the directory exists.....
        collect_file(lang)


# This will ALWAYS be done to the original jsonl data.
# returns df in case it's wanted
def assign_ids(spark: SparkSession, lang: string):
    df = spark.read.json(f"{settings.dir_info['data_proc']}/{lang}/{lang}_kaikki_data.jsonl")
    df = df.withColumn("entry_id", funcs.monotonically_increasing_id())
    df.write.mode('overwrite').parquet(f"{settings.dir_info['data_proc']}/{lang}/has_id_column")
    return df






def save_basic_info(spark: SparkSession,lang, data: DataFrame, url, db_props):
    df = data[[f.name for f in data.schema.fields if not isinstance(f.dataType, ArrayType)]]
    # df.write.option('url', url).option('dbtable', True).saveAsTable(name=f"{lang}_demo_basic_info",
    #                      format='jdbc', mode="overwrite")
    df.write.mode("overwrite").jdbc(url, table= f"wiktionary_info.{lang}_basic_info",
              properties=db_props)


# convert the column to json
def convert_all_to_json(data: DataFrame):
    # make those columns into json
    df = (reduce(lambda memo_df, col_name: memo_df.withColumn(col_name, funcs.to_json(col_name)),
                 data.columns,
                 data))
    # learned about the functools module from this:
    # https://mrpowers.medium.com/performing-operations-on-multiple-columns-in-a-pyspark-dataframe-36e97896c378
    return df


def convert_some_to_binary(data: DataFrame):
    columns = [f.name for f in data.schema.fields
               if (isinstance(f.dataType, ArrayType) or isinstance(f.dataType, StructType))]
    print(columns)
    # make those columns into json
    df = (reduce(lambda memo_df, col_name: memo_df.withColumn(col_name, funcs.encode(col_name, "utf-8")),
                 columns,
                 data))
    # learned about the functools module from this:
    # https://mrpowers.medium.com/performing-operations-on-multiple-columns-in-a-pyspark-dataframe-36e97896c378
    return df






def convert_all_from_json(data: DataFrame):
    df = (reduce(lambda memo_df, col_name: memo_df.withColumn(col_name, funcs.from_json(col_name)),
                 data.columns,
                 data))
    return df

# infer data from samples of the target Column....
# def infer_schema(column: Column):


def get_all_json_schema(data: DataFrame):
    df = (reduce(lambda memo_df, col_name: memo_df.withColumn(col_name, funcs.schema_of_json(col_name)),
                 data.columns,
                 data))
    return df
# given: * the set of tags to target for each part of speech and a dataframe
#        * the dataframe with inflectional forms
# returns: a dataframe with the grammatical tag combos as columns
