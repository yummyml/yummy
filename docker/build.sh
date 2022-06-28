#!/bin/bash

#docker build -t qooba/feast:yummy .
docker build -t qooba/feast:yummy_spark -f Dockerfile.spark .
