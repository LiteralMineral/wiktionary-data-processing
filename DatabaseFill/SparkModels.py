import json
from datetime import datetime
import settings
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy import create_engine, engine
from sqlalchemy.engine import URL
from sqlalchemy.orm import declarative_base, sessionmaker, backref, relationship
from sqlalchemy import Column, Integer, \
    BigInteger, String, DateTime, Text

Base = declarative_base()


class WordForm(Base):
    __tablename__ = 'word_forms'
    # __mapper__ =
    # id = Column()

    # primary keys
    entry_id = Column("entry_id", BigInteger(), primary_key=True)
    word = Column( "word", String(), primary_key=True)
    pos = Column("pos", String(), primary_key=True)

    form = Column("form", String(), primary_key=True)
    head_nr = Column("head_nr", String(), nullable=True)
    ipa = Column("ipa", String(), nullable=True)
    roman = Column("roman", String(), nullable=True)
    source = Column("source", String(), nullable=True)
    tags = Column("tags", ARRAY(String(50)), nullable=True)
    # raw_tags = Column("raw_tags", ARRAY(String(50)), nullable=True)
    date_added = Column("date_added", DateTime(), default=datetime.now,
                        nullable=False)
    date_edited = Column("date_edited", DateTime(), default=datetime.now,
                         onupdate=datetime.now, nullable=False)
    # pass


class BasicInfo(Base):
    __tablename__ = 'word_entries'

    entry_id = Column("entry_id", BigInteger(), primary_key=True)
    word = Column( "word", String(), nullable=True)
    lang = Column("lang", String(), nullable=True)
    lang_code = Column("lang_code", String(), nullable=True)
    pos = Column("pos", String(), nullable=True)
    original_title = Column("original_title", String(), nullable=True)
    source = Column("source", String(), nullable=True)
    etymology_number = Column("etymology_number", String(), nullable=True)
    etymology_text = Column("etymology_text", String(), nullable=True)
    date_added = Column("date_added", DateTime(),
                        default=datetime.now, nullable=False)
    date_edited = Column("date_edited", DateTime(), default=datetime.now,
                         onupdate=datetime.now, nullable=False)

    # relations
    # word_forms = relationship("WordForm", backref="word_entry")
    # senses = TODO: write the outline for the senses.
    # pass



class DatabaseManager:
    # url: URL = URL.create(
    #     drivername="postgresql",
    #     username=f"{settings.db_dict["USER"]}",
    #     password=f"{settings.db_dict["PASSWORD"]}",
    #     host=f"{settings.db_dict["HOST"]}:{settings.db_dict["PORT"]}",
    #     database=f"{settings.db_dict["DATABASE"]}"
    # )

    drivername="postgresql"
    username=f"{settings.db_dict["USER"]}"
    password=f"{settings.db_dict["PASSWORD"]}"
    host=f"{settings.db_dict["HOST"]}:{settings.db_dict["PORT"]}"
    database=f"{settings.db_dict["DATABASE"]}"

    url = f"{drivername}://{username}:{password}@{host}/{database}"
    #
    print(url)
    engine: engine = create_engine(url)
    base = Base

    def __init__(self, initialize_db=False):
        # make the session using the engine.
        self.engine.begin()
        self.Session = sessionmaker(bind=self.engine)
        # instantiate base
        if initialize_db:
            self.base.metadata.create_all(self.engine)
            # pass
        # pass
    # pass

database_manager = DatabaseManager(initialize_db=True)

def insert_json(json_source, base):
    # grab the right file from Data.
    with open(json_source, mode="r", encoding="utf-8") as source:
        lines = source.readlines()
        session = database_manager.Session()
        # for each line in the json file:
        #       * read it into a dict
        counter = 1
        for entry in lines:
            # print(json.loads(entry))
            # print(type(json.loads(entry)))
            entry = WordForm(**(json.loads(entry)))
            session.add(entry)
            if counter == 0:
                session.commit()

            counter = (counter + 1) % 5



        session.commit()
        session.close()


    pass