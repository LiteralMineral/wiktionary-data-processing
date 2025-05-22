# other stuff
import array
import string
import types
import typing
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
from pyspark.sql import SparkSession, functions as funcs
from pyspark.sql.functions import lit

# from pyspark.sql.connect.functions import lit
from pyspark.sql.types import *

import settings
from DataProcessing.Modules import utils, InOut


# from DataProcessing.Modules.utils import load

# given: dataframe of inflection information
# returns: a set of tags associated with each part of speech.
# TODO: make it so this function
def collect_inflection_tags(data: DataFrame, group: str = "lang", explode_tags=False):

    # if (spark != None):
    #     spark.read.parquet(f"{data}")
    # explode the entries
    exploded = data.withColumn("form_tags", funcs.explode_outer("forms.tags")).select(
        "lang", "pos", funcs.explode("form_tags").alias("form_tags")
    )
    # exploded.explain()

    # exploded.show()

    # get the union of the arrays in each column.
    # get the dataframe with the columns 'language', 'pos', and 'tags'.
    # get the dataframe to have columns that are the parts of speech.
    grouped_by = (
        exploded.groupBy(group, "pos")
        .agg(funcs.collect_set(exploded["form_tags"]).alias("form_tags"))
        .withColumn("form_tags", funcs.explode_outer("form_tags"))
        .distinct()
    )
    # .pivot('pos') \
    # grouped_by.show()
    # print([item[0] for item in grouped_by.dtypes if item[1].startswith("array")])
    # print(grouped_by.columns)
    # somehow turn it into a dictionary?? dunno... or just return the dataframe
    return grouped_by


def analyze_lang_forms(lang: str,
                       spark: SparkSession) -> DataFrame:
    data = InOut.load_parquet("lang", "has_id_column", spark)
    data = collect_inflection_tags(data, "lang")
    return data


def file_to_word_forms(lang: str, spark: SparkSession) -> DataFrame:
    data = InOut.load_parquet(lang, "has_id_column")
    data = data.select("entry_id", "word", "pos", "forms") # select the basic information as needed...
    data = utils.apply_to_columns(data,  # explode the things that need exploding
                                  [item[0] for item in data.dtypes
                                   if item[1].startswith("array")],
                                  funcs.explode_outer)
    flat_schema = utils.flatten_schema(data.schema)
    flat_data = data.select(flat_schema)
    # print([item[0] for item in flat_data.dtypes
    #                                if item[1].startswith("array")])
    # flat_data = utils.apply_to_columns(flat_data,         # explode the things that need exploding
    #                               [item[0] for item in flat_data.dtypes
    #                                if item[1].startswith("array")],
    #                               funcs.explode_outer)
    # flat_data.show()
    return flat_data

def dataframe_to_word_forms(data: DataFrame) -> DataFrame:
    # data = load_dataset(lang, "has_id_column", spark_read_func)
    data = data.select("entry_id", "word", "lang", "pos", "forms")
    data = utils.apply_to_columns(
        data,  # explode the things that need exploding
        [item[0] for item in data.dtypes if item[1].startswith("array")],
        funcs.explode_outer,
    )
    flat_schema = utils.flatten_schema(data.schema)
    flat_data = data.select(flat_schema)
    # print([item[0] for item in flat_data.dtypes
    #                                if item[1].startswith("array")])
    # flat_data = utils.apply_to_columns(flat_data,         # explode the things that need exploding
    #                               [item[0] for item in flat_data.dtypes
    #                                if item[1].startswith("array")],
    #                               funcs.explode_outer)

    # flat_data.show()
    return flat_data

# takes in the dataframe of inflectional information to be sorted and a
# dictionary defining which tags fall under which word properties
# expects the dataframe to have columns entry_id, word, forms, etc.
# returns the dataframe with the tags exploded
def extract_inflection_tags(data: DataFrame):
    # explode the forms tags
    df = data.withColumn("exploded_forms", funcs.explode_outer("forms")).withColumn(
        "form_tags", funcs.explode_outer("exploded_forms.tags")
    )  # .withColumn("word_forms.")


def sort_inflection_tags(data: DataFrame, category_tags: typing.Dict):
    return utils.sort_tags_column(data, "inflection_tags", category_tags)




def sample_word_forms(lang: str, min_samples = 30):
    # load the word_forms
    word_forms = InOut.load_parquet(lang, "word_forms").drop("ruby")
    # establish array concat....
    col_map = utils.concat_array_map(word_forms)
    # establish the sample_tag_name
    sample_tag_name = "lang/pos/form_tag"
    # add the explode column...
    col_map[sample_tag_name] = funcs.explode_outer("tags")

    # apply the map and then make the sample_tag_name column.
    sample_tags = word_forms.withColumns(col_map)\
        .withColumn(sample_tag_name, funcs.concat_ws("/", "lang","pos",sample_tag_name))
    # sample_tags.explain()

    # establish how to sample them....
    fractions = sample_tags.groupBy(sample_tag_name).count().collect()
    frac_map = dict()
    for mapping in fractions:
        frac_map[mapping[sample_tag_name]] = min(1.0, (min_samples /mapping["count"]))
    # print(frac_map)

    # actually sample it
    samples = sample_tags.sampleBy(sample_tag_name, frac_map, seed=1).sort(sample_tag_name)
    InOut.save_parquet(samples, lang, "sample_word_forms")
    InOut.write_to_single_csv(samples, lang, "sample_word_forms")