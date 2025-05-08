# other stuff
import array
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
from pyspark.sql.functions import lit
# from pyspark.sql.connect.functions import lit
from pyspark.sql.types import *


# given: dataframe of inflection information
# returns: a set of tags associated with each part of speech.
def analyze_inflection_tags(data: DataFrame):
    # if (spark != None):
    #     spark.read.parquet(f"{data}")
    # explode the entries
    exploded = data.withColumn('form_tags', funcs.explode('forms.tags')) \
        .select('lang', 'pos', funcs.explode('form_tags').alias('form_tags'))
    # exploded.explain()

    # get the union of the arrays in each column.
    # get the dataframe with the columns 'language', 'pos', and 'tags'.
    # get the dataframe to have columns that are the parts of speech.
    grouped_by = exploded.groupBy('lang') \
        .pivot('pos') \
        .agg(funcs.collect_set(exploded['form_tags']))
    # somehow turn it into a dictionary?? dunno... or just return the dataframe
    return grouped_by


def sort_inflection_tags(data: DataFrame):

    pass


def get_definition_entries(self, dataframe, separator="\n\n"):
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


def filter_inflection_entries(self, dataframe, separator="\n\n"):
    pass
