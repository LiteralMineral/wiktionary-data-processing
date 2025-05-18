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
from scripts.h2py import filedict

import settings


def outer_join_dataframes(data1: DataFrame, data2: DataFrame, on=None):
    return data1.join(data2, on=on, how="outer")


def concat_dataframes(*args):


    return reduce(lambda acc_df, new_df: acc_df.union(new_df),
                  args[1:],
                  args[0])


# given: list of strings `langs`,
#        string `data` (representing the folder in which the data will be found), and
#        SparkSession spark
# does:  loads the files and iteratively combines their columns.
def join_language_data(langs, filename, spark: SparkSession):
    tables = [
        spark.read.parquet(f"{settings.dir_info['data_proc']}/{filename}/{lang}")
        for lang in langs
    ]
    for tab in tables:
        print(tab.columns)
        print(type(tab.columns))

    result = reduce(
        lambda acc_df, df: outer_join_dataframes(
            acc_df, df, list(set(acc_df.columns) & set(df.columns))
        ),
        tables[1:],  # iterate over the tables other than the first one.
        tables[0],
    )
    return result
    # pass


# expects the data to be loaded for each one....
def build_collective_data(
    langs: typing.List[str], filename: str, spark_read_func=types.FunctionType
) -> DataFrame:
    result = reduce(
        lambda acc_df, lang: acc_df.union(
            load_dataset(lang, filename, spark_read_func)
        ),
        langs[1:],  # iterate over the languages.
        load_dataset(langs[0], filename, spark_read_func),  # the initial result
    )
    return result




def load_datasets(
    langs, filename, spark_read_func: types.FunctionType
) -> typing.List[DataFrame]:
    tables = [
        spark_read_func(f"{settings.dir_info['data_proc']}/{filename}/{lang}")
        for lang in langs
    ]
    return tables


def load_dataset(lang: str, filename: str, spark_read_func) -> DataFrame:
    table = spark_read_func(f"{settings.dir_info['data_proc']}/{filename}/{lang}")
    return table


def save_dataset(
    table: DataFrame, lang: str, filename: str, write_to_parquet: bool = True
):
    filename = f"{settings.dir_info['data_proc']}/{filename}/{lang}"
    if write_to_parquet:
        table.write.mode("overwrite").parquet(filename)
    else:
        table.write.mode("overwrite").csv(filename)


# tables: tuple(Dataframe, string)
# filename: what to save it as
def save_datasets(tables, filename: str):
    for tup in tables:
        save_dataset(tup[0], tup[1], filename)


#
def write_to_single_csv(data: DataFrame, lang, filename):
    spark = data.sparkSession
    tmp_output_dir = f"{settings.dir_info['data_proc']}/{filename}/{lang}/my_temp"
    output_file = (
        f"{settings.dir_info['data_proc']}/{filename}/{lang}/{lang}_{filename}.csv"
    )
    # https: // engineeringfordatascience.com / posts / how_to_save_pyspark_dataframe_to_single_output_file /
    # headers = spark.createDataFrame([[c.name for c in data.schema.fields]],
    #                                 schema=T.StructType(
    #                                     [T.StructField(c.name, T.StringType(),False)
    #                                      for c in data.schema.fields]))
    # headers.write.mode('overwrite').option("encoding", "utf-8").csv(tmp_output_dir, header)

    data.write.mode("overwrite").option("encoding", "utf-8").csv(
        tmp_output_dir, header=True
    )
    # os.system(f"cat {tmp_output_dir}/*.csv > {output_file}")

    files = glob.glob(tmp_output_dir + f"/*.csv")
    # for f in files:
    #     print(f)

    with open(output_file, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile)

        # get the header in from the first file.
        with open(files[0], "r", encoding="utf-8") as input_file:
            reader = csv.reader(input_file)
            for row in reader:
                # print(row)
                print(reader.line_num)
                writer.writerow(row)

        # skip the header in these files.
        for source in files[1:]:
            with open(source, "r", encoding="utf-8") as input_file:
                reader = csv.reader(input_file)
                #
                for row in reader:
                    if reader.line_num > 1:
                        print(row)
                        writer.writerow(row)

    pass


# given a part of speech and a tag, finds example words that
# are of that part of speech and have that tag
# This is to prepare data for language experts to look at
# in order to give me their opinions.
def sample(data: DataFrame, pos: str, tag: str) -> DataFrame:
    # sample from the dataframe. and return it.
    pass


# takes in the language to sample tags from.
# samples word_form data for each pos and tag combo.
def sample_known_tags(
    lang: str,
    pos: str,
) -> DataFrame:
    pass


# given: str language, SparkSession spark, FunctionType func
# does: applies procedure to the data specified by lang and filename
# func must take in lang, filename, and save a dataframe
def apply_to_data(langs: Iterable[str], filename: str,procedure: typing.Callable[[str, str], None],
    kwargs,
) -> None:
    for lang in langs:
        procedure(lang, filename)


# def apply_to_columns(data: DataFrame, col_names: Iterable[str], func, extra_col: Column = None):
#     col_dict = {}
#     if type(extra_col) == NoneType:
#         for col in col_names:
#             col_dict[col] = func(data[col])
#     else:
#         for col in col_names:
#             col_dict[col] = func(data[col], extra_col)
#
#     print(col_dict)
#     data = data.withColumns(col_dict)
# data.explain()
#
# return data


def apply_to_columns(data: DataFrame, col_names: Iterable[str], func):
    return reduce(
        lambda acc_df, col_name: acc_df.withColumn(
            col_name, func(col_name).alias(col_name)
        ),
        col_names,
        data,
    )


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


def flatten_schema(schema: T.StructType, prefix=None) -> typing.Iterable[str]:
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
            fields += flatten_schema(dtype, prefix=name)
        else:
            fields.append(name)

    return fields


def find_arrays(schema: T.StructType, prefix=None) -> typing.Iterable[str]:
    fields = []
    for field in schema.fields:
        name = f"{prefix}.{field.name}" if prefix else field.name
        dtype = field.dataType
        # print(f"{name}\t{dtype}\n\n")
        if isinstance(dtype, T.ArrayType):
            fields.append(name)
            # print(f"node {name}\t{isinstance(dtype, T.ArrayType)}")

            dtype = dtype.elementType
            # print(f"node {name}\t{isinstance(dtype, T.ArrayType)}")

            # print(dtype)
        if isinstance(dtype, T.StructType):
            # print(f"{name}\t{isinstance(dtype, T.ArrayType)}")
            fields += flatten_schema(dtype, prefix=name)
        else:
            print(f"leaf node {name}\t{isinstance(dtype, T.ArrayType)}")
            # fields.append(name)
    return fields
