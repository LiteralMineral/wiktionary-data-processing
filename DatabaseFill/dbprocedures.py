import json

#### define the Database manager. Maybe make it global?

import settings
from sqlalchemy import create_engine, engine
from sqlalchemy.orm import declarative_base, sessionmaker, backref, relationship

from DatabaseFill import SparkModels
from DatabaseFill.SparkModels import WordForm

# from sqlalchemy.engine import URL
# from sqlalchemy import Column, Integer, \
#     BigInteger, String, DateTime, Text

Base = declarative_base()

