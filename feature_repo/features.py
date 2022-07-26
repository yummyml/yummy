from google.protobuf.duration_pb2 import Duration
from feast import Entity, Field, FeatureView, ValueType
from feast.types import Float32
from feast_pyspark import DeltaSource
from feast.data_format import ParquetFormat

my_stats = DeltaSource(
    path="/home/jovyan/feast-pyspark/feature_repo/dataset/all",
    timestamp_field="datetime",
)
my_entity = Entity(name="entity_id", value_type=ValueType.INT64, description="entity id",)
mystats_view = FeatureView(
    name="my_statistics",
    entities=["entity_id"],
    ttl=Duration(seconds=3600*24*20),
    features=[
        Field(name="f0", dtype=Float32),
        Field(name="f1", dtype=Float32),
        Field(name="f2", dtype=Float32),
        Field(name="f3", dtype=Float32),
        Field(name="f4", dtype=Float32),
        Field(name="f5", dtype=Float32),
        Field(name="f6", dtype=Float32),
        Field(name="f7", dtype=Float32),
        Field(name="f8", dtype=Float32),
        Field(name="f9", dtype=Float32),
        Field(name="y", dtype=Float32),
    ], online=True, input=my_stats, tags={},)
