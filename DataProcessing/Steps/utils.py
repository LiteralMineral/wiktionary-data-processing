# for making other code easier.
import string
import types
from functools import reduce
from typing import List, Iterable

from pyspark.sql import \
    SparkSession, \
    DataFrame, \
    functions as funcs

import settings


def outer_join_dataframes(data1: DataFrame, data2: DataFrame, on=None):
    return data1.join(data2, on=on, how='outer')


# given: list of strings `langs`,
#        string `data` (representing the folder in which the data will be found), and
#        SparkSession spark
# does:  loads the files and iteratively combines their columns.
def join_language_data(langs, filename, spark: SparkSession):
    tables = [spark.read.parquet(f"{settings.dir_info['data_proc']}/{lang}/{filename}")
              for lang in langs]
    for tab in tables:
        print(tab.columns)
        print(type(tab.columns))

    result = reduce(lambda acc_df, df: outer_join_dataframes(acc_df, df,
                                                             list(set(acc_df.columns)
                                                                  & set(df.columns))),
                    tables[1:],  # iterate over the tables other than the first one.
                    tables[0])
    return result
    # pass


def load_datasets(langs, filename, spark_read_func):
    tables = [spark_read_func(f"{settings.dir_info['data_proc']}/{lang}/{filename}")
              for lang in langs]

    return tables


# tables: tuple(Dataframe, string)
# filename: what to save it as

def save_datasets(tables, filename):
    for tup in tables:
        tup[0].write.mode("overwrite").parquet(
            f"{settings.dir_info['data_proc']}/{tup[1]}/{filename}")


# given: str language, SparkSession spark, FunctionType func
# does: performs func on lang using spark
def apply_to_df(frames: Iterable[DataFrame],
                func, extra_args=None):
    return [func(frame) for frame in frames]
