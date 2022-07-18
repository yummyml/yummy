#!/bin/bash

export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS="lab --notebook-dir=/home/jovyan --ip='0.0.0.0' --port=8888 --no-browser --allow-root --NotebookApp.password='' --NotebookApp.token=''"

pyspark \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:0.13.2,io.delta:delta-core_2.12:1.1.0,org.apache.hadoop:hadoop-aws:3.3.1,software.amazon.awssdk:s3:2.17.131 \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    --conf "spark.driver.memory=5g" \
    --conf "spark.executor.memory=5g" \
    --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog" \
    --conf "spark.sql.catalog.spark_catalog.type=hive" \
    --conf "spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog" \
    --conf "spark.sql.catalog.local.type=hadoop" \
    --conf "spark.sql.catalog.local.warehouse=$PWD/warehouse" \
    --conf "spark.hadoop.fs.s3a.access.key=minioadmin" \
    --conf "spark.hadoop.fs.s3a.secret.key=minioadmin" \
    --conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000"


#--packages org.apache.iceberg:iceberg-spark3-runtime:0.13.2,software.amazon.awssdk:bundle:2.17.131,software.amazon.awssdk:url-connection-client:2.17.131 \

#--packages org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:0.13.2,com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.2 \

#jupyter lab --notebook-dir=/home/jovyan --ip='0.0.0.0' --port=8888 --no-browser --allow-root --NotebookApp.password='' --NotebookApp.token=''
