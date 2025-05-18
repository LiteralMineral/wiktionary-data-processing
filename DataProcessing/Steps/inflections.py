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
from DataProcessing.Steps import utils
from DataProcessing.Steps.utils import load_dataset


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


def analyze_lang_forms(lang: str, spark_read_func) -> DataFrame:
    data = load_dataset("lang", "has_id_column", spark_read_func)
    data = collect_inflection_tags(data, "lang")
    return data


def file_to_word_forms(lang: str, spark_read_func) -> DataFrame:
    data = load_dataset(lang, "has_id_column", spark_read_func)
    data = data.select("entry_id", "word", "pos", "forms")
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

def dataframe_to_word_forms(data: DataFrame) -> DataFrame:
    # data = load_dataset(lang, "has_id_column", spark_read_func)
    data = data.select("entry_id", "word", "pos", "forms")
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


def get_definition_entries(self, dataframe, separator="\n\n"):
    og = dataframe
    print(og.count())
    # separate out the senses, drop the original 'senses' column
    in_progress = dataframe.withColumn(
        "separate_senses", funcs.explode_outer("senses")
    ).drop("senses")
    # detect if the sense is tagged as "form-of"
    in_progress = in_progress.withColumn(
        "not_dict_form", funcs.array_contains("separate_senses.tags", "form-of")
    )
    # Turn individual senses into their own strings
    in_progress = in_progress.withColumn(
        "separate_senses_glosses", funcs.array_join("separate_senses.glosses", "###")
    )
    # TODO: detect if the 'form-of' entries are diminutives, make sure they get included
    # if the sense was not tagged, it was not tagged with "form-of"
    in_progress = in_progress.na.fill({"not_dict_form": False})
    # select the senses that are not an inflection or other form...
    in_progress = in_progress.where(in_progress.not_dict_form == False).drop(
        "not_dict_form"
    )

    grouped_by_id = in_progress.groupBy("entry_id").agg(
        funcs.concat_ws(
            separator, funcs.collect_list(in_progress.separate_senses_glosses)
        ).alias("filtered_glosses")
    )
    print(grouped_by_id.count())
    grouped_by_id = grouped_by_id.withColumnRenamed("entry_id", "entry_id2")
    # grouped_by_id.show(100, truncate=False)

    in_progress = og.join(
        grouped_by_id,
        self.editable_dataset.entry_id == grouped_by_id.entry_id2,
        "right",
    )
    # print(grouped_by_id.count())

    in_progress = in_progress.drop("entry_id2")

    return in_progress


def filter_inflection_entries(self, dataframe, separator="\n\n"):
    pass
