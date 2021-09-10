from mongoengine import (
    Document,
    StringField,
    ListField,
    EmbeddedDocumentField,
    ReferenceField,
    IntField,
    FloatField
)


class Structure(Document):
    path = StringField()


class Reflections(Document):
    path = StringField()


class Compound(Document):
    path = StringField()


class System(Document):
    system_name = StringField()
    datasets = ListField(ReferenceField("Dataset"))


class Dataset(Document):
    dtag = StringField()
    system = ReferenceField(System)
    structure = ReferenceField(Structure)
    reflections = ReferenceField(Reflections)
    compounds = ListField(ReferenceField(Compound))


class Model(Document):
    path = StringField()
    dataset = ReferenceField(Dataset)
    compounds = ListField(ReferenceField(Compound))


class Event(Document):
    dataset = ReferenceField(Dataset)
    event_idx = IntField()
    x = FloatField()
    y = FloatField()
    z = FloatField()
    model = ReferenceField(Model)


class PanDDA(Document):
    path = StringField()
    system = ReferenceField(System)
    events = ListField(ReferenceField(Event))
    datasets = ListField(ReferenceField(Dataset))


class Ligand(Document):
    reference_model = ReferenceField("ReferenceModel")
    x = FloatField()
    y = FloatField()
    z = FloatField()


class ReferenceModel(Document):
    path = StringField()
    system = ReferenceField(System)
    dataset = ReferenceField(Dataset)
    event = ReferenceField(Event)
    dataset = ReferenceField(Dataset)
    ligands = ListField(ReferenceField(Ligand))
