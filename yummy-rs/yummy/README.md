# Yummy rust addons

This repository contains Yummy addons written in Rust.

## Feature server

Rust feature server have been implemented using [Actix](https://actix.rs/) server.
The server is fully compatible with Feast.
Thus you can use it with the features materialized with Feast to online store.
Currently `Redis/RedisCluster` online store implementation is available.
Additionaly only http protocol is supported.

The payload is fully compatible with Feast server:

### Example request/response:

Request with features list:
```json
{          
  "features": [
                "driver_hourly_stats:conv_rate",
                "driver_hourly_stats:acc_rate",
                "driver_hourly_stats:avg_daily_trips",                      
            ],
  "entities": {"driver_id": [1001,1002,1003,1004,1005]},
  "full_feature_names": true,
}
```

Request with [feature service](https://docs.feast.dev/getting-started/concepts/feature-retrieval#feature-services):
```json
{
  "feature_service": "driver_activity_basic",
  "entities": {"driver_id": [1001,1002,1003,1004,1005]},
  "full_feature_names": true,
}
```

Example response:
```json
{"metadata": {"feature_names": ["driver_id",
   "driver_hourly_stats__conv_rate",
   "driver_hourly_stats__acc_rate",
   "driver_hourly_stats__avg_daily_trips"]},
 "results": [{"values": [1001, 1002, 1003, 1004, 1005],
   "statuses": ["PRESENT", "PRESENT", "PRESENT", "PRESENT", "PRESENT"],
   "event_timestamps": ["2022-10-04T22:46:16",
    "2022-10-04T22:46:16",
    "2022-10-04T22:46:16",
    "2022-10-04T22:46:16",
    "2022-10-04T22:46:16"]},
  {"values": [0.44467267, null, 0.775576, 0.719485, null],
   "statuses": ["PRESENT", "PRESENT", "PRESENT", "PRESENT", "PRESENT"],
   "event_timestamps": ["2022-10-04T22:46:16",
    "2022-10-04T22:46:16",
    "2022-10-04T22:46:16",
    "2022-10-04T22:46:16",
    "2022-10-04T22:46:16"]},
  {"values": [0.36920926, null, 0.8855987, 0.09924329, null],
   "statuses": ["PRESENT", "PRESENT", "PRESENT", "PRESENT", "PRESENT"],
   "event_timestamps": ["2022-10-04T22:46:16",
    "2022-10-04T22:46:16",
    "2022-10-04T22:46:16",
    "2022-10-04T22:46:16",
    "2022-10-04T22:46:16"]},
  {"values": [821, null, 381, 587, null],
   "statuses": ["PRESENT", "PRESENT", "PRESENT", "PRESENT", "PRESENT"],
   "event_timestamps": ["2022-10-04T22:46:16",
    "2022-10-04T22:46:16",
    "2022-10-04T22:46:16",
    "2022-10-04T22:46:16",
    "2022-10-04T22:46:16"]}]}
```


## Protos

[Feast](https://github.com/feast-dev/feast) protobuf definitions ([License](https://github.com/feast-dev/feast/blob/master/LICENSE)) have been used in the project.

Protobuf rust implementation have been used to generate rust code:
```bash
protoc --rust_out . $(find . -iname "*.proto")
```


