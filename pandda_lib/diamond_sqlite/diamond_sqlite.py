from sqlalchemy import Column, Integer, String, ForeignKey, Table
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base

DATASET_SQL_TABLE = "dataset"
SYSTEM_DATA_DIR_SQL_TABLE = "system_data_dir"


Base = declarative_base()


class DatasetSQL(Base):
    __tablename__ = DATASET_SQL_TABLE

    id = Column(Integer, primary_key=True)
    system_id = Column(Integer, ForeignKey(f"{SYSTEM_DATA_DIR_SQL_TABLE}.id"))
    dtag = Column(String)
    path = Column(String)
    model_path = Column(String)


class SystemDataDirSQL(Base):
    __tablename__ = SYSTEM_DATA_DIR_SQL_TABLE

    id = Column(Integer, primary_key=True)
    system_name = Column(String)
    path = Column(String)
    datasets = relationship("DatasetSQL")


