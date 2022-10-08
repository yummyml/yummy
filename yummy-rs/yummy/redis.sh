#!/bin/bash

docker run --rm -it -p 6379:6379 --name redis --network app_default redis
