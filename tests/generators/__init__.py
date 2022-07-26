import os
import random
from typing import Callable, List, Optional, Tuple, Union, Dict, Any
from abc import ABC, abstractmethod
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timezone, timedelta
from sklearn.datasets import make_hastie_10_2
from enum import Enum
from google.protobuf.duration_pb2 import Duration
from feast import Entity, Feature, FeatureView, ValueType, Field
from feast.types import Float32, Int32
from yummy import ParquetDataSource, CsvDataSource, DeltaDataSource, IcebergDataSource

start_date = datetime.strptime("2021-10-01", "%Y-%m-%d")
end_date = start_date + timedelta(days=100)

class DataType(str, Enum):
    csv = "csv"
    parquet = "parquet"
    delta = "delta"
    iceberg = "iceberg"

class Generator(ABC):

    @staticmethod
    def entity() -> Entity:
        return Entity(name="entity_id", value_type=ValueType.INT64, description="entity id",)

    @staticmethod
    def generate_entities(size: int):
        return np.random.choice(size, size=size, replace=False)

    @staticmethod
    def entity_df(size:int = 10):
        entities=Generator.generate_entities(size)
        entity_df = pd.DataFrame(data=entities, columns=['entity_id'])
        entity_df["event_timestamp"]=datetime(2021, 10, 1, 23, 59, 42, tzinfo=timezone.utc)
        return entity_df

    @staticmethod
    def generate_data(entities, year=2021, month=10, day=1) -> pd.DataFrame:
        n_samples=len(entities)
        X, y = make_hastie_10_2(n_samples=n_samples, random_state=0)
        df = pd.DataFrame(X, columns=["f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9"])
        df["y"]=y
        df['entity_id'] = entities
        df['datetime'] = pd.to_datetime(datetime(2021, 10, 1, 23, 59, 41, tzinfo=timezone.utc))
        df['created'] = pd.to_datetime(
                datetime.now(), #utc=True
                )

        # add rows for entity_id == 1
        df = Generator.__append_modified(
            df,
            1,
            [
                pd.DateOffset(days=-1),
                pd.DateOffset(hours=-1),
                pd.DateOffset(minutes=-1),
                pd.DateOffset(seconds=-1),
            ]
        )

        df['month_year'] = df['datetime'].dt.date
        return df

    @staticmethod
    def __append_modified(df: pd.DataFrame, entity_id: int, offsets: list[pd.DateOffset]) -> pd.DataFrame:
        adf = df[df.entity_id == 1].copy()
        i=1
        for offset in offsets:
            aadf = adf.copy()
            aadf['datetime'] = adf['datetime'] + offset
            aadf['f0'] = adf['f0'] + i
            i+=1
            df = df.append(aadf.copy())
        return df

    @property
    def data_type(self) -> DataType:
        raise NotImplementedError("Data type not defined")

    @property
    def features(self):
        return [
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
        ]

    def generate(self, path: str, size: int = 10, year: int = 2021, month: int = 10, day: int = 1) -> Tuple[FeatureView, str]:
        entities = Generator.generate_entities(size)
        df = Generator.generate_data(entities, year, month, day)
        self.write_data(df, path)
        return self.prepare_features(path)

    @abstractmethod
    def write_data(self, df: pd.DataFrame, path: str):
        ...

    @abstractmethod
    def prepare_source(self, path: str):
        ...

    def prepare_features(self, path: str) -> Tuple[FeatureView, str]:
        source = self.prepare_source(path)
        name = f"fv_{self.data_type}"
        return FeatureView(
            name=name,
            entities=[Generator.entity()],
            ttl=Duration(seconds=3600*24*20),
            schema=self.features,
            online=True,
            source=source,
            tags={},), name


class CsvGenerator(Generator):

    @property
    def data_type(self) -> DataType:
        return DataType.csv

    def write_data(self, df: pd.DataFrame, path: str):
        df.to_csv(path, date_format='%Y-%m-%d %H:%M:%S')

    def prepare_source(self, path: str):
        return CsvDataSource(
            name="csv",
            path=path,
            timestamp_field="datetime",
        )

class ParquetGenerator(Generator):

    @property
    def data_type(self) -> DataType:
        return DataType.parquet

    def write_data(self, df: pd.DataFrame, path: str):
        df.to_parquet(path)

    def prepare_source(self, path: str):
        return ParquetDataSource(
            name="parquet",
            path=path,
            timestamp_field="datetime",
        )

class DeltaGenerator(Generator):

    @property
    def data_type(self) -> DataType:
        return DataType.delta

    def write_data(self, df: pd.DataFrame, path: str):
        from pyspark.sql import SparkSession
        from pyspark import SparkConf

        spark = SparkSession.builder.config(conf=SparkConf().setAll(
            [
                ("spark.master", "local[*]"),
                ("spark.ui.enabled", "false"),
                ("spark.eventLog.enabled", "false"),
                ("spark.sql.session.timeZone", "UTC"),
                ("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"),
                ("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            ]
        )).getOrCreate()

        spark.createDataFrame(df).write.format("delta").mode("append").save(path)

    def prepare_source(self, path: str):
        return DeltaDataSource(
            name="delta",
            path=path,
            timestamp_field="datetime",
        )


class IcebergGenerator(Generator):

    @property
    def data_type(self) -> DataType:
        return DataType.delta

    def write_data(self, df: pd.DataFrame, path: str):
        from pyspark.sql import SparkSession
        from pyspark import SparkConf

        dir_name=os.path.dirname(path)
        db_name=os.path.basename(path)

        spark = SparkSession.builder.config(conf=SparkConf().setAll(
            [
                ("spark.master", "local[*]"),
                ("spark.ui.enabled", "false"),
                ("spark.eventLog.enabled", "false"),
                ("spark.sql.session.timeZone", "UTC"),
                ("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),
                ("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog"),
                ("spark.sql.catalog.spark_catalog.type","hive"),
                ("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog"),
                ("spark.sql.catalog.local.type","hadoop"),
                ("spark.sql.catalog.local.warehouse",dir_name),
            ]
        )).getOrCreate()

        spark.sql(f"CREATE TABLE {db_name} (f0 float, f1 float, f2 float, f3 float, f4 float, f5 float, f6 float, f7 float, f8 float, f9 float, y float, entity_id int, datetime date, created date, month_year date) USING iceberg")
        spark.createDataFrame(df).write.format("iceberg").mode("append").save(db_name)

    def prepare_source(self, path: str):
        return IcebergDataSource(
            name="iceberg",
            path=os.path.basename(path),
            timestamp_field="datetime",
        )


def _base(tmp_dir: str, path: str, generator: Generator):
    x_dataset = str(Path(tmp_dir) / path)
    x_fv, x_fv_name = generator.generate(x_dataset)
    return x_fv, x_fv_name


def csv(tmp_dir: str):
    return _base(tmp_dir, "data.csv", CsvGenerator())

def parquet(tmp_dir: str):
    return _base(tmp_dir, "data.parquet", ParquetGenerator())

def delta(tmp_dir: str):
    return _base(tmp_dir, "delta", DeltaGenerator())

def iceberg(tmp_dir: str):
    i = str(random.randint(0, 100))
    return _base(tmp_dir, f"local.db.table{i}", IcebergGenerator())











