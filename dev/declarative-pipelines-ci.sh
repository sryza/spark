#!/bin/bash
set -e

./build/sbt \
    "project pipelines" "testOnly *" \
    "project connect" "testOnly org.apache.spark.sql.connect.pipelines*"

dev/lint-python --compile --black --custom-pyspark-error --flake8
