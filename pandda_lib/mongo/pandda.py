from mongoengine import (
    Document,
    StringField,
    ListField,
    EmbeddedDocumentField,
    ReferenceField,
    IntField,
    FloatField
)


class Structure:
    path = StringField()


class Reflections:
    path = StringField()


class Compound:
    path = StringField()


class System:
    system_name = StringField()
    datasets = ListField(ReferenceField("Dataset"))


class Dataset(Document):
    dtag = StringField()
    system = ReferenceField(System)
    structure = ReferenceField(Structure)
    reflections = ReferenceField(Reflections)
    compounds = ListField(ReferenceField(Compound))


class Model:
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


class PanDDA:
    path = StringField()
    system = ReferenceField(System)
    events = ListField(ReferenceField(Event))
    datasets = ListField(StringField)
