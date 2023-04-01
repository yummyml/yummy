# Yummy-[Delta](https://delta.io/)

This repository contains Yummy [Delta Lake](https://delta.io/) api written in Rust.
The api use [delta-rs](https://github.com/delta-io/delta-rs) implementation.


## Instalation

```
pip3 install yummy-delta
```

## Quick start

``` python
import yummy_delta
# yummy_delta.run({config_path}, {host}, {port}, {log_level})
yummy_delta.run("config.yaml", "0.0.0.0", 8080, "error")
```

In the configuration file we can define multiple stores using supported backends.

config.yaml
``` yaml
stores:
  - name: local
    path: "/tmp/delta-test-1/"
  - name: az
    path: "az://delta-test-1/"
```

## Delta backends

### Local file system
```
/tmp/delta-test-1
```

### AWS S3 / S3
```
s3://<bucket>/<path>
s3a://<bucket>/<path>
```

Environment variables:
  AWS_REGION="us-east-1",
  AWS_ACCESS_KEY_ID="deltalake",
  AWS_SECRET_ACCESS_KEY="weloverust",
  AWS_ENDPOINT_URL=endpoint_url,

### Azure Blob Storage / Azure Datalake Storage Gen2
```
az://<container>/<path>
adl://<container>/<path>
abfs://<container>/<path>
```

Environment variables:
  AZURE_STORAGE_ACCOUNT_NAME="devstoreaccount1",
  AZURE_STORAGE_ACCOUNT_KEY="****",
  AZURE_STORAGE_USE_EMULATOR="false",
  AZURE_STORAGE_USE_HTTP="true",

### Google cloud storage
```
gs://<bucket>/<path>
```

## Rest API

### Stores list

Request:
```
curl -X GET "http://localhost:8080/api/1.0/delta" \
-H "Content-Type: application/json"
```

Response:
```
{'stores': [{'path': '/tmp/delta-test-1/', 'store': 'local'},
            {'path': 'az://delta-test-1/', 'store': 'az'}]}
```

### Tables list

Request:
```
curl -X GET "http://localhost:8080/api/1.0/delta/{store_name}" \
-H "Content-Type: application/json"
```

Response:
```
{'path': 'az://delta-test-1/',
 'store': 'az',
 'tables': ['test_delta_1', 'test_delta_2', 'test_delta_3', 'test_delta_4']}
```

### Table details

Request:
```
curl -X GET "http://localhost:8080/api/1.0/delta/{store_name}/{table_name}/details" \
-H "Content-Type: application/json"
```

Response:
```
{'path': 'az://delta-test-1/test_delta_4',
 'schema': {'fields': [{'metadata': {},
                        'name': 'col1',
                        'nullable': false,
                        'type': 'integer'},
                       {'metadata': {},
                        'name': 'col2',
                        'nullable': false,
                        'type': 'string'}],
            'type': 'struct'},
 'store': 'az',
 'table': 'test_delta_4',
 'version': 100}
```

### Create delta table

Request:
```
curl -X POST "http://localhost:8080/api/1.0/delta/{store_name}" \
-H "Content-Type: application/json" \
-d '{
    "table": "test_delta_4",
    "schema": [
        {
            "name": "col1",
            "type": "integer",
            "nullable": false
        },
        {
            "name": "col2",
            "type": "string",
            "nullable": false
        }
    ],
    "partition_columns": ["col2"],
    "comment": "Hello from delta"
}'
```

Response:
```
{ "table": "test_delta_4" }
```

### Append data

Request:
```
curl -X POST "http://localhost:8080/api/1.0/delta/{store_name}/{table_name}" \
-H "Content-Type: application/json" \
-d '
{"record_batch_dict": {"col1": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
                       "col2": ["A", "B", "A", "B", "A", "A", "A", "B", "B", "A", "A"]}}'
```

Response:
```
{'table': 'test_delta_4', 'version': 101}
```

OR: 

Request:
```
curl -X POST "http://localhost:8080/api/1.0/delta/{store_name}/{table_name}" \
-H "Content-Type: application/json" \
-d '
{"record_batch_list": [
    {"col1": 1, "col2": "A"}, 
    {"col1": 2, "col2": "B"}, 
    {"col1": 3, "col2": "A"}, 
    {"col1": 4, "col2": "B"}, 
    {"col1": 5, "col2": "A"}, 
    {"col1": 6, "col2": "A"}, 
    {"col1": 7, "col2": "A"}, 
    {"col1": 8, "col2": "B"}, 
    {"col1": 9, "col2": "B"}, 
    {"col1": 10, "col2": "A"}, 
    {"col1": 11, "col2": "A"}
    ]
}
```

Response:
```
{'table': 'test_delta_4', 'version': 101}
```

### Overwrite data

Request:
```
curl -X PUT "http://localhost:8080/api/1.0/delta/{store_name}/{table_name}" \
-H "Content-Type: application/json" \
-d '
{"record_batch_dict": {"col1": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
                       "col2": ["A", "B", "A", "B", "A", "A", "A", "B", "B", "A", "A"]}}'
```

Response:
```
{'table': 'test_delta_4', 'version': 101}
```

OR: 

Request:
```
curl -X PUT "http://localhost:8080/api/1.0/delta/{store_name}/{table_name}" \
-H "Content-Type: application/json" \
-d '
{"record_batch_list": [
    {"col1": 1, "col2": "A"}, 
    {"col1": 2, "col2": "B"}, 
    {"col1": 3, "col2": "A"}, 
    {"col1": 4, "col2": "B"}, 
    {"col1": 5, "col2": "A"}, 
    {"col1": 6, "col2": "A"}, 
    {"col1": 7, "col2": "A"}, 
    {"col1": 8, "col2": "B"}, 
    {"col1": 9, "col2": "B"}, 
    {"col1": 10, "col2": "A"}, 
    {"col1": 11, "col2": "A"}
    ]
}
```

Response:
```
{'table': 'test_delta_4', 'version': 101}
```

### Optimize

Request:
```
curl -X POST "http://localhost:8080/api/1.0/delta/{store_name}/{table_name}/optimize" \
-H "Content-Type: application/json" \
-d '{
    "target_size": 2000000,
    "filters": [
        {
            "column": "col2",
            "operator": "=",
            "value": "B"
        }
    ]
}'
```

Response:
```
{'metrics': {'filesAdded': {'avg': 524.5,
                            'max': 541,
                            'min': 508,
                            'totalFiles': 2,
                            'totalSize': 1049},
             'filesRemoved': {'avg': 506.75,
                              'max': 538,
                              'min': 499,
                              'totalFiles': 36,
                              'totalSize': 18243},
             'numBatches': 1,
             'numFilesAdded': 2,
             'numFilesRemoved': 36,
             'partitionsOptimized': 2,
             'preserveInsertionOrder': true,
             'totalConsideredFiles': 36,
             'totalFilesSkipped': 0}}
```

### Vacuum

Request:
```
curl -X POST "http://localhost:8080/api/1.0/delta/{store_name}/{table_name}/vacuum" \
-H "Content-Type: application/json" \
-d '{
    "retention_period_seconds": 0,
    "enforce_retention_duration": false,
    "dry_run": false
}'
```

Response:
```
{'dry_run': False,
 'files_deleted': ['col2=A/part-00000-09fef82b-9208-41be-8769-30c442b36346-c000.snappy.parquet',
                   'col2=A/part-00000-16779a2f-d2f4-450c-b815-3b8fca850402-c000.snappy.parquet',
                   'col2=A/part-00000-26e01b49-a432-4d06-816d-9a2048c0194e-c000.snappy.parquet',
                   'col2=A/part-00000-3ff1f905-87ae-4b73-a3ae-3cd1056171a0-c000.snappy.parquet',
                   'col2=A/part-00000-50dbcfea-22d6-4ef3-bacf-f1371b9186ad-c000.snappy.parquet',
                   'col2=A/part-00000-511d3ceb-0959-4024-9bbe-d049bb4fb4c6-c000.snappy.parquet',
                   'col2=A/part-00000-56e5484f-1722-45eb-84bb-1475b67e9f63-c000.snappy.parquet',
                   'col2=B/part-00000-b15a1cd7-9a15-4ba6-8ba8-e3ca63737f81-c000.snappy.parquet',
                   'col2=B/part-00000-d4fadc68-46b5-4f1f-a378-e9f3dba842f0-c000.snappy.parquet',
                   'col2=B/part-00000-d6e823d1-433e-4798-a641-1e2783c8c1eb-c000.snappy.parquet',
                   'col2=B/part-00000-ded9a5b7-4754-47b7-9e2c-132263cea52e-c000.snappy.parquet',
                   'col2=B/part-00000-fb89f02c-8ea3-47d9-8794-182d8602687c-c000.snappy.parquet']}
```

