# other stuff
import string
from functools import reduce
import psycopg2
from configparser import ConfigParser
import json
import requests
import os


# pyspark stuff
from types import *
from typing import Dict
from pyspark.sql import *
from pyspark.sql import SparkSession, \
    functions as funcs
from pyspark.sql.types import *




# Download and assign unique identifiers to the data
def collect_file(lang: string):
    # download
    response = requests.get(
        f"https://kaikki.org/dictionary/{lang}/words/kaikki.org-dictionary-{lang}-words.jsonl")
    if response.status_code == 200:
        with open(f"Data/{lang}/{lang}_kaikki_data.jsonl", 'wb') as file:
            file.write(response.content)
    else:
        print("could not get file")
        Exception("could not get file")


def download(lang: string):
    if os.path.exists(f"Data/{lang}/{lang}_kaikki_data.jsonl"):
        print('exists already, not redownloading.')
    else:
        # TODO: make sure the directory exists.....
        collect_file(lang)


# This will ALWAYS be done to the original jsonl data.
# returns df in case it's wanted
def assign_ids(spark: SparkSession, lang: string):
    df = spark.read.json(f"Data/{lang}/{lang}_kaikki_data.jsonl")
    df = df.withColumn("entry_id", funcs.monotonically_increasing_id())
    df.write.parquet(f"Data/{lang}/has_id_column")

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


def get_all_json_schema(data: DataFrame):
    df = (reduce(lambda memo_df, col_name: memo_df.withColumn(col_name, funcs.schema_of_json(col_name)),
                 data.columns,
                 data))
    return df

# infer data from samples of the target Column....
# def infer_schema(column: Column):


# given: * the set of tags to target for each part of speech and a dataframe
#        * the dataframe with inflectional forms
# returns: a dataframe with the grammatical tag combos as columns
def sort_inflection_tags(data: DataFrame):


    pass



# given: dataframe of inflection information
# returns: a set of tags associated with each part of speech.
def collect_inflection_tags(data: DataFrame):
    # explode the entries

    # group them by part of speech

    # get the union of the arrays in each column.
    # get the dataframe with the columns 'language', 'pos', and 'tags'.
    # get the dataframe to have columns that are the parts of speech.
    # somehow turn it into a dictionary?? dunno... or just return the dataframe
    pass





def filter_inflection_entries(self, dataframe, separator = "\n\n"):
    og = dataframe
    print(og.count())
    # separate out the senses, drop the original 'senses' column
    in_progress = dataframe.withColumn('separate_senses',
                                       funcs.explode_outer('senses')) \
        .drop('senses')
    # detect if the sense is tagged as "form-of"
    in_progress = in_progress.withColumn('not_dict_form',
                                         funcs.array_contains(
                                             'separate_senses.tags',
                                             'form-of'))
    # Turn individual senses into their own strings
    in_progress = in_progress.withColumn('separate_senses_glosses',
                                         funcs.array_join(
                                             'separate_senses.glosses',
                                             '###'))
    # TODO: detect if the 'form-of' entries are diminutives, make sure they get included
    # if the sense was not tagged, it was not tagged with "form-of"
    in_progress = in_progress.na.fill({'not_dict_form': False})
    # select the senses that are not an inflection or other form...
    in_progress = in_progress.where(
        in_progress.not_dict_form == False).drop('not_dict_form')

    grouped_by_id = in_progress.groupBy('entry_id') \
        .agg(funcs.concat_ws(separator,
                             funcs.collect_list(
                                 in_progress.separate_senses_glosses)).alias(
        'filtered_glosses'))
    print(grouped_by_id.count())
    grouped_by_id = grouped_by_id.withColumnRenamed('entry_id', 'entry_id2')
    # grouped_by_id.show(100, truncate=False)

    in_progress = og \
        .join(grouped_by_id,
              self.editable_dataset.entry_id == grouped_by_id.entry_id2,
              'right')
    # print(grouped_by_id.count())

    in_progress = in_progress.drop('entry_id2')

    return in_progress