# this is the file for writing code relating to updating the database based on new data
from datetime import datetime
from typing import Iterable
#import project settings
# from settings import db_dict
import settings
import pyspark.sql.types
# to start with, I'll just. overwrite the datatables. I'll figure out how to negotiate updates later.

########## section for transferring data from spark RDD Rows to database.
# from pyspark.sql import DataFrame, Row





# def word_form_partition_to_database(partition):
#     # make the session
#     session = database_manager.Session()
#     for row in partition:
#         session.add(WordForm(**(row.asDict())))
#         session.commit()
#     # pass
#
# def word_form_row_to_database(row, session):
#     # make the session
#     # session = database_manager.Session()
#     d = row.asDict()
#     session.add(WordForm(row.asDict()))
#     session.commit()
#     # pass
# def word_forms_to_database(data: DataFrame):
#     # for each partition...
#     # data.foreachPartition(word_form_partition_to_database)
#     session = database_manager.Session()
#     data.foreach(lambda r: word_form_row_to_database(r, session))

    # pass










########## section for getting data from database and using it on a website

