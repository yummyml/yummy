# Feast on Spark

This repo adds possibility to run [Feast](https://github.com/feast-dev/feast) on spark.

## Get started
### Install feast:
```shell
pip install feast
```

### Install feast-postgres:
```shell
git clone https://github.com/qooba/feast-pyspark.git
cd feast-pyspark
pip install -e .
```

### Create a feature repository:
```shell
feast init feature_repo
cd feature_repo
```

### Offline store:
To configure the offline store edit `feature_store.yaml`
```yaml
project: feature_repo
registry: data/registry.db
provider: local
online_store:
    ...
offline_store:
    type: feast_pyspark.SparkOfflineStore
    spark_conf:
        spark.master: "local[*]"
        spark.ui.enabled: "false"
        spark.eventLog.enabled: "false"
        spark.sql.session.timeZone: "UTC"
```

### Example

Example `features.py`:
```python
from google.protobuf.duration_pb2 import Duration
from feast import Entity, Feature, FeatureView, ValueType

from feast_pyspark import DeltaDataSource
from feast.data_format import ParquetFormat

my_stats = DeltaDataSource(
    path="dataset/all",
    event_timestamp_column="datetime",
    #range_join=10,
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
```

## References

This project is based on the [Feast](https://github.com/feast-dev) project.

I was also inspired by the other projects:
[feast-spark-offline-store](https://github.com/Adyen/feast-spark-offline-store/) - spark configuration and session
[feast-postgres](https://github.com/nossrannug/feast-postgres) - parts of Makefiles and github workflows

