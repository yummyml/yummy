# [Yummy](https://github.com/yummyml/yummy) - delicious [Feast](https://github.com/feast-dev/feast) extension

Yummy project adds possiblity to run [Feast](https://github.com/feast-dev/feast) on multiple backends:
* [polars](https://github.com/pola-rs/polars)
* [dask](https://github.com/dask/dask)
* [ray](https://github.com/ray-project/ray)
* [spark](https://github.com/apache/spark)

This gives flexibility in setting up the feature store on existing environments and using its capabilities.
Moreover using Yummy you can combine multiple and different datasources during historical fetch task.

### Install yummy:

```bash
pip install yummy
```

```bash
pip install git+https://github.com/qooba/yummy.git
```

### Create a feature repository:
```bash
feast init feature_repo
cd feature_repo
```

### Offline store:

#### Polars

To configure the offline store edit `feature_store.yaml`
```yaml
project: feature_repo
registry: data/registry.db
provider: local
online_store:
    ...
offline_store:
    type: yummy.YummyOfflineStore
    backend: polars
```

#### Dask

To configure the offline store edit `feature_store.yaml`
```yaml
project: feature_repo
registry: data/registry.db
provider: local
online_store:
    ...
offline_store:
    type: yummy.YummyOfflineStore
    backend: dask
```

#### Ray

To configure the offline store edit `feature_store.yaml`
```yaml
project: feature_repo
registry: data/registry.db
provider: local
online_store:
    ...
offline_store:
    type: yummy.YummyOfflineStore
    backend: ray
```

#### Spark

To configure the offline store edit `feature_store.yaml`
```yaml
project: feature_repo
registry: data/registry.db
provider: local
online_store:
    ...
offline_store:
    type: yummy.YummyOfflineStore
    backend: spark
    spark_conf:
        spark.master: "local[*]"
        spark.ui.enabled: "false"
        spark.eventLog.enabled: "false"
        spark.sql.session.timeZone: "UTC"
```


### Features definition

Example `features.py`:
```python
from datetime import timedelta
from feast import Entity, FeatureView, Field
from feast.types import Float32, Int32
from yummy import ParquetSource, CsvSource, DeltaSource

my_stats_parquet = ParquetSource(
    path="/home/jovyan/notebooks/ray/dataset/all_data.parquet",
    timestamp_field="datetime",
)

my_stats_delta = DeltaSource(
    path="dataset/all",
    timestamp_field="datetime",
    #range_join=10,
)

my_stats_csv = CsvSource(
    path="/home/jovyan/notebooks/ray/dataset/all_data.csv",
    timestamp_field="datetime",
)

my_entity = Entity(name="entity_id", description="entity id",)

mystats_view_parquet = FeatureView(
    name="my_statistics_parquet",
    entities=[my_entity],
    ttl=timedelta(seconds=3600*24*20),
    schema=[
        Field(name="entity_id", dtype=Float32),
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
    ], online=True, source=my_stats_parquet, tags={},)

mystats_view_delta = FeatureView(
    name="my_statistics_delta",
    entities=[my_entity],
    ttl=timedelta(seconds=3600*24*20),
    schema=[
        Field(name="entity_id", dtype=Float32),
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
    ], online=True, source=my_stats_delta, tags={},)

    
mystats_view_csv = FeatureView(
    name="my_statistics_csv",
    entities=[my_entity],
    ttl=timedelta(seconds=3600*24*20),
    schema=[
        Field(name="entity_id", dtype=Float32),
        Field(name="c1", dtype=Float32),
        Field(name="c2", dtype=Float32),
    ], online=True, source=my_stats_csv, tags={},)
```


## Historical fetch

```python
from feast import FeatureStore
import pandas as pd
import time

store = FeatureStore(repo_path=".")

start_time = time.time()
training_df = store.get_historical_features(
    entity_df=entity_df, 
    features = [
        'my_statistics_parquet:p1',
        'my_statistics_parquet:p2',
        'my_statistics_delta:d1',
        'my_statistics_delta:d2',
        'my_statistics_csv:c1',
        'my_statistics_csv:c2'
    ],
).to_df()


print("--- %s seconds ---" % (time.time() - start_time))

training_df
```


## References

This project is based on the [Feast](https://github.com/feast-dev) project.

I was also inspired by the other projects:

[feast-spark-offline-store](https://github.com/Adyen/feast-spark-offline-store/) - spark configuration and session

[feast-postgres](https://github.com/nossrannug/feast-postgres) - parts of Makefiles and github workflows

