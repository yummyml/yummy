from google.protobuf.duration_pb2 import Duration
from feast import Entity, Feature, FeatureView, ValueType

from feast_pyspark import DeltaDataSource
from feast.data_format import ParquetFormat

my_stats = DeltaDataSource(
    path="/home/jovyan/feast-pyspark/feature_repo/dataset/all",
    event_timestamp_column="datetime",
)
my_entity = Entity(name="entity_id", value_type=ValueType.INT64, description="entity id",)
mystats_view = FeatureView(
    name="my_statistics",
    entities=["entity_id"],
    ttl=Duration(seconds=3600*24*20),
    features=[
        Feature(name="f0", dtype=ValueType.FLOAT),
        Feature(name="f1", dtype=ValueType.FLOAT),
        Feature(name="f2", dtype=ValueType.FLOAT),
        Feature(name="f3", dtype=ValueType.FLOAT),
        Feature(name="f4", dtype=ValueType.FLOAT),
        Feature(name="f5", dtype=ValueType.FLOAT),
        Feature(name="f6", dtype=ValueType.FLOAT),
        Feature(name="f7", dtype=ValueType.FLOAT),
        Feature(name="f8", dtype=ValueType.FLOAT),
        Feature(name="f9", dtype=ValueType.FLOAT),
        Feature(name="y", dtype=ValueType.FLOAT),
    ], online=True, input=my_stats, tags={},)
