from sqlalchemy import Column, Integer, String, ForeignKey, Table
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base

SYSTEMDATADIRSQLTABLE = "system_data_dir"


Base = declarative_base()


class SystemDataDirSQL(Base):
    __tablename__ = SYSTEMDATADIRSQLTABLE

    id = Column(Integer, primary_key=True)
    path = Column(String)


