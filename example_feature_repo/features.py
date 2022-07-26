from google.protobuf.duration_pb2 import Duration
from feast import Entity, Field, FeatureView, ValueType
from feast.types import Float32
from yummy import ParquetDataSource, CsvDataSource, DeltaDataSource

my_stats_parquet = ParquetDataSource(
    path="/home/jovyan/notebooks/ray/dataset/all_data.parquet",
    timestamp_field="datetime",
)

my_stats_delta = DeltaDataSource(
    path="dataset/all",
    timestamp_field="datetime",
    #range_join=10,
)

my_stats_csv = CsvDataSource(
    path="/home/jovyan/notebooks/ray/dataset/all_data.csv",
    timestamp_field="datetime",
)

my_entity = Entity(name="entity_id", value_type=ValueType.INT64, description="entity id",)

mystats_view_parquet = FeatureView(
    name="my_statistics_parquet",
    entities=["entity_id"],
    ttl=Duration(seconds=3600*24*20),
    features=[
        Field(name="p0", dtype=Float32),
        Field(name="p1", dtype=Float32),
        Field(name="p2", dtype=Float32),
        Field(name="p3", dtype=Float32),
        Field(name="p4", dtype=Float32),
        Field(name="p5", dtype=Float32),
        Field(name="p6", dtype=Float32),
        Field(name="p7", dtype=Float32),
        Field(name="p8", dtype=Float32),
        Field(name="p9", dtype=Float32),
        Field(name="y", dtype=Float32),
    ], online=True, input=my_stats_parquet, tags={},)

mystats_view_delta = FeatureView(
    name="my_statistics_delta",
    entities=["entity_id"],
    ttl=Duration(seconds=3600*24*20),
    features=[
        Field(name="d0", dtype=Float32),
        Field(name="d1", dtype=Float32),
        Field(name="d2", dtype=Float32),
        Field(name="d3", dtype=Float32),
        Field(name="d4", dtype=Float32),
        Field(name="d5", dtype=Float32),
        Field(name="d6", dtype=Float32),
        Field(name="d7", dtype=Float32),
        Field(name="d8", dtype=Float32),
        Field(name="d9", dtype=Float32),
    ], online=True, input=my_stats_delta, tags={},)


mystats_view_csv = FeatureView(
    name="my_statistics_csv",
    entities=["entity_id"],
    ttl=Duration(seconds=3600*24*20),
    features=[
        Field(name="c1", dtype=Float32),
        Field(name="c2", dtype=Float32),
    ], online=True, input=my_stats_csv, tags={},)
