# for making other code easier.
import glob
import csv
import os
import string
from os import system
import shutil
import types
import typing
from types import NoneType

import pyspark.sql.types as T
from functools import reduce
from typing import List, Iterable

from pyspark.sql import (
    SparkSession,
    DataFrame,
    functions as funcs,
    Column,
    DataFrameReader,
)
from pyspark.sql.functions import lit
from pyspark.sql.types import MapType

import settings
from DataProcessing.Modules import InOut

def outer_join_dataframes(data1: DataFrame, data2: DataFrame, on=None):
    return data1.join(data2, on=on, how="outer")


def concat_dataframes(*args):
    return reduce(lambda acc_df, new_df: acc_df.union(new_df),
                  args[1:],
                  args[0])




def load_and_join_list(langs: List[str], foldername: str):
    """
    given:
        * list of strings `langs`
        * string `data` (representing the folder in which the data will be found)
        * SparkSession spark
    does: recursively loads and combines the files and combines their columns where they match
    """
    # helper function
    def join_dataframes(data1: DataFrame, data2: DataFrame) -> DataFrame:
        return data1.join(data2,
                          list(set(data1.columns) & set(data2.columns)),
                          # join them on the intersection of their columns
                          how="outer")  # join them outer.

    result = reduce(lambda acc_df, lang:
                    join_dataframes(acc_df,
                                    InOut.load_parquet(lang, foldername)),
        langs[1:],  # iterate over the languages other than the first one.
        InOut.load_parquet(langs[0], foldername) # get the first one loaded
    )
    return result


def load_and_union_data(langs: List[str], foldername: str) -> DataFrame:
    result = reduce(lambda acc_df, lang: acc_df.union(InOut.load_parquet(lang, foldername)),
                    langs[1:],
                    InOut.load_parquet(langs[0], foldername))
    return result

def load_union_save(langs: List[str],foldername: str,  union_name: str = None) -> None:
    if isinstance(union_name, NoneType):
        union_name = "_".join(langs)
    result = load_and_union_data(langs, foldername)
    InOut.save_parquet(result, f"{union_name}", foldername)

# given: str language, SparkSession spark, FunctionType func
# does: applies procedure to the data specified by lang and filename
# func must take in lang, filename, and save a dataframe
def load_apply_save(langs: List[str],
                    in_folder: str, out_folder: str,
                    function: typing.Callable[[DataFrame], DataFrame]) -> None:
    """load the data, apply the function, save the dataframe. The function must
    only take in a dataframe. Any need for an additional argument can be handled
    by constructing a lambda function with explicitly filled values."""
    for lang in langs:
        data = InOut.load_parquet(lang, in_folder)
        data = function(data)
        # data.show()
        InOut.save_parquet(data, lang, out_folder)

    pass



def apply_to_columns(data: DataFrame, col_names: List[str], func) -> DataFrame:
    return reduce(
        lambda acc_df, col_name: acc_df.withColumn(
            col_name, func(col_name).alias(col_name)
        ),
        col_names,
        data,
    )

def reduce_columns(data:DataFrame, new_name: str, col_names: List[str], func) -> DataFrame:
    new_col = reduce(lambda col1, col2: func(col1, col2),
                     col_names
                     )
    return data.withColumn(new_name, new_col)



def select_array_cols(data: DataFrame) -> List[str]:
    return [item[0] for item in data.dtypes if item[1].startswith("array")]

def make_map_dict(cols: List[str], map_function):
    map_dict = dict()
    for c in cols:
        map_dict[c] = map_function(c)

    return map_dict




def concat_array_map(data: DataFrame, sep = "_") -> dict:
    map = dict()
    for col_name in [item[0] for item in data.dtypes if item[1].startswith("array")]:
        map[col_name] = funcs.concat_ws(sep, col_name)

    return map

# input: dataframe: dataframe to do the operation on
#        string: name of column to filter
#        dictionary: with keywords of columns and values of filtering tag lists
# returns: dataframe with columns added, as described by the dictionary.
def sort_tags_column(data: DataFrame, column_name, filter_tags: typing.Dict):
    return reduce(
        lambda acc_df, cat_name: acc_df.withColumn(
            cat_name,  # given the category name
            funcs.array_intersect(
                data[column_name],  # and the tag column to filter
                lit(filter_tags[cat_name]),
            ),
        ),  # filtering tags
        filter_tags.keys(),  # use the tagsets for these columns
        data,  # dataframe to start with
    )


# def recursively_flatten_column(data: DataFrame, col_name: str) -> DataFrame:

def flatten_schema(schema: T.StructType, prefix=None, level = -1, iter_count = 0) -> typing.List[str]:
    fields = []

    for field in schema.fields:
        name = f"{prefix}.{field.name}" if prefix else field.name
        # rename = f"{prefix}_{field.name}" if prefix else field.name
        dtype = field.dataType
        if isinstance(dtype, T.ArrayType):
            # if type(data) != NoneType:
            # data = data.withColumn(name, funcs.explode_outer(name).alias(name))
            dtype = dtype.elementType
            # print(dtype)
        if isinstance(dtype, T.StructType):
            if level != iter_count:
                fields += flatten_schema(dtype, prefix=name,
                                         level = level, iter_count= iter_count + 1)
        else:
            fields.append(name)

    return fields


# def find_arrays(schema: T.StructType, prefix=None) -> typing.Iterable[str]:
#     fields = []
#     for field in schema.fields:
#         name = f"{prefix}.{field.name}" if prefix else field.name
#         dtype = field.dataType
#         # print(f"{name}\t{dtype}\n\n")
#         if isinstance(dtype, T.ArrayType):
#             fields.append(name)
#             # print(f"node {name}\t{isinstance(dtype, T.ArrayType)}")
#
#             dtype = dtype.elementType
#             # print(f"node {name}\t{isinstance(dtype, T.ArrayType)}")
#
#             # print(dtype)
#         if isinstance(dtype, T.StructType):
#             # print(f"{name}\t{isinstance(dtype, T.ArrayType)}")
#             fields += flatten_schema(dtype, prefix=name)
#         else:
#             print(f"leaf node {name}\t{isinstance(dtype, T.ArrayType)}")
#             # fields.append(name)
#     return fields


def explode_array_cols(data: DataFrame, array_cols: List[str]) -> DataFrame:
    # reduce those array columns with explode
    data = reduce(lambda acc_df, col_name: acc_df.withColumn(col_name,
                                                             funcs.explode_outer(col_name)),
                  array_cols,
                  data)
    # flatten the schema
    # data.printSchema()
    # print(flatten_schema(data.schema))
    data = data.select([funcs.col(col_name)\
                       .alias(str.replace(col_name,".","/"))
                        for col_name in flatten_schema(data.schema)])
    data.printSchema()
    return data

def explode_all_arrays(data: DataFrame, level = -1) -> DataFrame:
    # get the array columns
    iter_count = 0
    array_cols = [item[0] for item in data.dtypes if item[1].startswith("array")]
    while len(array_cols) > 0 and level != iter_count:
        data = explode_array_cols(data, array_cols)
        array_cols = [item[0] for item in data.dtypes if item[1].startswith("array")]
        iter_count+= 1

    return data

