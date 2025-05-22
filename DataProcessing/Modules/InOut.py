"""
This module handles input and output of the files.

Contains:
    * __init__()
    * load(lang: str, foldername: str, spark: SparkSession)
        * loads the identified file. the programmers are responsible for checking that
         the loaded file is appropriate for the procedure they are loading it into.
    * save_parquet(data: DataFrame, lang: str, foldername: str)
        * saves the dataframe to its appropriate location.
    * merge_files(.....)
        * merges files from a temporary directory to the temp directory's parent and
        then deletes the temporary directory.
    * write_to_single_csv(data: DataFrame, lang: str, filename: str)
        * Writes the dataset to a single csv
"""
import settings
settings.init()

import shutil
import glob
from pyspark.sql import SparkSession, DataFrame
import sys

"""
trying to make a module that keeps information about the spark session...
https://stackoverflow.com/questions/1977362/how-to-create-module-wide-variables-in-python
"""

this = sys.modules[__name__]

def init(spark_session:SparkSession):
    this.spark = spark_session
    this.data_address = settings.dir_info["data_proc"]

def load_parquet(lang: str, foldername: str) -> DataFrame:
    """given the language and the foldername, load the parquet file."""
    data = this.spark.read.parquet(f"{this.data_address}/{foldername}/{lang}")
    return data

def save_parquet(data: DataFrame, lang: str, foldername: str) -> None:
    """save the dataframe to a parquet file"""
    data.write.mode("overwrite").parquet(f"{this.data_address}/{foldername}/{lang}")

def save_csv(data: DataFrame, lang: str, foldername: str) -> None:
    """save the dataframe to a csv file"""
    data.write.mode("overwrite")\
        .csv(f"{this.data_address}/{foldername}/{lang}", header=True)

def merge_files(tmp_output_dir, file_name, file_extension, has_header=False):
    """Merge the partitioned files from the temporary output directory
    into its parent directory, and then delete the temporary directory."""
    files = glob.glob(tmp_output_dir + "/*" + file_extension)
    output_file_name = f"{tmp_output_dir}/../../{file_name}.{file_extension}"

    with open(output_file_name, "w", newline="", encoding="utf-8") as outfile:

        # handle the file that might have the header regardless.
        with open(files[0], "r", newline="", encoding="utf-8") as input_file:
            for row in input_file.readlines():
                outfile.write(row)

        # handle files where you may have to skip the header
        for source in files[1:]:
            with open(source, "r", newline="", encoding="utf-8") as input_file:
                src = input_file.readlines()
                lines = src[1:] if has_header else src
                for row in lines:
                    outfile.write(row)
    # delete tmp_output_dir
    shutil.rmtree(tmp_output_dir)


def write_to_single_csv(data: DataFrame, lang, foldername):
    """Write the files to a temporary directory. They will be partitioned
    into separate files. Then merge those files into the temporary directory's
    parent directory and delete the temporary directory. This approach was
    influenced by the following webpage:
    https://engineeringfordatascience.com/posts/how_to_save_pyspark_dataframe_to_single_output_file"""

    tmp_output_dir = f"{this.data_address}/{foldername}/{lang}/my_temp"

    data.write.mode("overwrite").option("encoding", "utf-8").csv(
        tmp_output_dir, header=True)
    merge_files(tmp_output_dir, f"{lang}_{foldername}",
                "csv", has_header=True)
    pass


def write_to_single_json(data: DataFrame, lang, foldername):
    """Write the json files to a temporary directory. They will be partitioned
    into separate files. Then merge those files into the temporary directory's
    parent directory and delete the temporary directory. This approach was
    influenced by the following webpage:
    https://engineeringfordatascience.com/posts/how_to_save_pyspark_dataframe_to_single_output_file"""

    tmp_output_dir = f"{this.data_address}/{foldername}/{lang}/my_temp"

    data.write.mode("overwrite").option("encoding", "utf-8").json(
        tmp_output_dir
    )
    # merge files. there is NO header, because spark saving to json does not involve any header.
    merge_files(tmp_output_dir, f"{lang}_{foldername}",
                "json", has_header=False)

    pass

