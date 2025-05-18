"""
This module handles input and output of the files.

Contains:
    * __init__()
    * load(lang: str, foldername: str, spark: SparkSession)
        * loads the identified file. the programmers are responsible for checking that
         the loaded file is appropriate for the procedure they are loading it into.
    * save_parquet(data: DataFrame, lang: str, foldername: str)
        * saves the dataframe to its appropriate location.
    # * write_to_single_csv(lang: str, )


"""
import settings
from pyspark.sql import SparkSession, DataFrame



def __init__():
    """Makes sure that the required directories exist,
    and establishes variables for the directories within this module"""

    # make sure directories exist. if not, create them

    proc_data = settings.dir_info["data_proc"]
    pass


data_address = settings.dir_info['data_proc']

def load_parquet(lang: str, foldername: str, spark: SparkSession) -> DataFrame:
    """given the language and the """
    table = spark.read.parquet(f"{data_address}/{foldername}/{lang}")
    return table

def save_parquet(data: DataFrame, lang: str, foldername: str) -> None:
    data.write.mode("overwrite").parquet(f"{data_address}/{foldername}/{lang}")

def save_csv(data: DataFrame, lang: str, foldername: str) -> None:
    data.write.mode("overwrite").csv(f"{data_address}/{foldername}/{lang}", header=True)


