# Yummy-[MLflow](https://www.mlflow.org/) rust addons

This repository contains Yummy mlflow addon written in Rust.

## Yummy mlflow

The MLflow rust wrapper currently supports models:
[x] lightgbm
[x] catboost (only binary classification)

The implementation currently supports MLflow models kept on local path.
To run the model run:

`yummy_mlflow.model_serve(MODEL_PATH, HOST, POST, LOG_LEVEL)`

example:
```python
import yummy_mlflow

yummy_mlflow.model_serve(model_path, '0.0.0.0', 8080, 'error')
```

The `yummy-mlflow` will expose HTTP server. 
The request response is compatible with MLflow model serving API.

Example:

Request:
```
curl -X POST "http://test1:8080/invocations" \
-H "Content-Type: application/json" \
-d '{
    "columns": ["0","1","2","3","4","5","6","7","8","9","10",
               "11","12"],
    "data": [
     [ 0.913333, -0.598156, -0.425909, -0.929365,  1.281985,
       0.488531,  0.874184, -1.223610,  0.050988,  0.342557,
      -0.164303,  0.830961,  0.997086,
    ]]
}'
```

Response:
```
[[0.9849612333276241, 0.008531186707393178, 0.006507579964982725]]
```


