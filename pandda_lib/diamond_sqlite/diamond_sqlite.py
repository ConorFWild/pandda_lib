from sqlalchemy import Column, Integer, String, ForeignKey, Table, Float
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base

DATASET_SQL_TABLE = "dataset"
PROJECT_DIR_SQL_TABLE = "project_dir"
SYSTEM_SQL_TABLE = "system"
PANDDA_DIR_SQL_TABLE = "pandda_dir"
PANDDA_DATASET_SQL_TABLE = "pandda_dataset"
PANDDA_EVENT_SQL_TABLE = "pandda_event"
PANDDA_BUILD_SQL_TABLE = "pandda_build"
PANDDA_1_DIR_SQL_TABLE = "pandda_1_dir"
REFERENCE_STRUCTURE_SQL_TABLE = "reference_structure"
BOUND_STATE_MODEL_SQL_TABLE = "bound_state_model"
SYSTEM_EVENT_MAP_SQL_TABLE = "system_event_map"
EVENT_MAP_QUANTILES_SQL_TABLE = "event_map_quantiles"

Base = declarative_base()


class EventMapQualtiles(Base):
    __tablename__ = EVENT_MAP_QUANTILES_SQL_TABLE

    id = Column(Integer, primary_key=True)
    event_map_id = Column(Integer, ForeignKey(f"{SYSTEM_EVENT_MAP_SQL_TABLE}.id"))

    base_50 =  Column(Float)
    base_75 =  Column(Float)
    base_90 =  Column(Float)

    event_50 =  Column(Float)
    event_75 =  Column(Float)
    event_90 = Column(Float)


class SystemEventMapSQL(Base):
    __tablename__ = SYSTEM_EVENT_MAP_SQL_TABLE

    id = Column(Integer, primary_key=True)
    dataset_id = Column(Integer, ForeignKey(f"{DATASET_SQL_TABLE}.id"))

    path = Column(String)
    event_idx = Column(Integer)
    bdc = Column(Float)

    event_map_quantiles = relationship("EventMapQualtiles", uselist=False)


class BoundStateModelSQL(Base):
    __tablename__ = BOUND_STATE_MODEL_SQL_TABLE

    id = Column(Integer, primary_key=True)
    dataset_id = Column(Integer, ForeignKey(f"{DATASET_SQL_TABLE}.id"))
    # project_id = Column(Integer, ForeignKey(f"{PROJECT_DIR_SQL_TABLE}.id"))

    rscc = Column(Float)
    custom_score = Column(Float)


class DatasetSQL(Base):
    __tablename__ = DATASET_SQL_TABLE

    id = Column(Integer, primary_key=True)
    system_id = Column(Integer, ForeignKey(f"{SYSTEM_SQL_TABLE}.id"))
    project_id = Column(Integer, ForeignKey(f"{PROJECT_DIR_SQL_TABLE}.id"))

    dtag = Column(String)
    path = Column(String)
    model_path = Column(String)
    mtz_path= Column(String)
    pandda_model_path = Column(String)
    event_maps = relationship("SystemEventMapSQL")
    bound_state_model = relationship("BoundStateModelSQL", uselist=False)


class ProjectDirSQL(Base):
    __tablename__ = PROJECT_DIR_SQL_TABLE

    id = Column(Integer, primary_key=True)

    system_id = Column(Integer, ForeignKey(f"{SYSTEM_SQL_TABLE}.id"))

    project_name = Column(String)
    path = Column(String)
    datasets = relationship("DatasetSQL")


class SystemSQL(Base):
    __tablename__ = SYSTEM_SQL_TABLE

    id = Column(Integer, primary_key=True)
    system_name = Column(String)
    projects=relationship("ProjectDirSQL")
    datasets = relationship("DatasetSQL")


class PanDDABuildSQL(Base):
    __tablename__ = PANDDA_BUILD_SQL_TABLE

    id = Column(Integer, primary_key=True)
    event_id = Column(Integer, ForeignKey(f"{PANDDA_EVENT_SQL_TABLE}.id"))

    build_path = Column(String)


class PanDDAEventSQL(Base):
    __tablename__ = PANDDA_EVENT_SQL_TABLE

    id = Column(Integer, primary_key=True)
    dataset_id = Column(Integer, ForeignKey(f"{PANDDA_DATASET_SQL_TABLE}.id"))

    event_map_path = Column(String)

    builds = relationship("PanDDABuildSQL")


class PanDDADatasetSQL(Base):
    __tablename__ = PANDDA_DATASET_SQL_TABLE

    id = Column(Integer, primary_key=True)
    pandda_id = Column(Integer, ForeignKey(f"{PANDDA_DIR_SQL_TABLE}.id"))

    dtag = Column(String)
    path = Column(String)

    events = relationship("PanDDAEventSQL")


class PanDDADirSQL(Base):
    __tablename__ = PANDDA_DIR_SQL_TABLE

    id = Column(Integer, primary_key=True)
    # system_id = Column(Integer, ForeignKey(f"{SYSTEM_DATA_DIR_SQL_TABLE}.id"))

    path = Column(String)
    pandda_dataset_results = relationship("PanDDADatasetSQL")


class PanDDA1DirSQL(Base):
    __tablename__ = PANDDA_1_DIR_SQL_TABLE

    id = Column(Integer, primary_key=True)
    system_id = Column(Integer, ForeignKey(f"{SYSTEM_SQL_TABLE}.id"))

    path = Column(String)
    system = relationship("SystemSQL")

    # pandda_dataset_results = relationship("PanDDADatasetSQL")


class ReferenceStructureSQL(Base):
    __tablename__ = REFERENCE_STRUCTURE_SQL_TABLE

    id = Column(Integer, primary_key=True)
    system_id = Column(Integer, ForeignKey(f"{SYSTEM_SQL_TABLE}.id"))
    dataset_id = Column(Integer, ForeignKey(f"{DATASET_SQL_TABLE}.id"))

    path = Column(String)
    system = relationship("SystemSQL")
    dataset = relationship("DatasetSQL")
