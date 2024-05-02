#!/bin/bash
cd ..
export PYSPARK_DRIVER_PYTHON=python # Do not set in cluster modes.
export PYSPARK_PYTHON=./environment/bin/python
hdfs dfs -put data_files.zip
hdfs dfs -put python_files.zip
spark-submit --archives spark-env.tar.gz#environment \
             --files hdfs://nyu-dataproc-m:8020/user/gl1589_nyu_edu/data_files.zip \
             --py-files hdfs://nyu-dataproc-m:8020/user/gl1589_nyu_edu/python_files.zip \
             test_process_data.py