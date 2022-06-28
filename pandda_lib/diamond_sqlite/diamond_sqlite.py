from sqlalchemy import Column, Integer, String, ForeignKey, Table
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class SystemDataDir(Base):
    __tablename__ = SYSTEMDATADIRTABLE

    id = Column(Integer, primary_key=True)
    path = Column(String)
