#!/bin/bash

#docker build -t qooba/feast:yummy .
docker build -t qooba/yummy:v0.0.2_spark -f Dockerfile.spark .
