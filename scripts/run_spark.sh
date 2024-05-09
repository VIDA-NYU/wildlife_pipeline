#!/bin/bash
cd ..
<<<<<<< HEAD
#export PYSPARK_DRIVER_PYTHON=python # Do not set in cluster modes.
export PYSPARK_PYTHON=./spark-env/bin/python
hdfs dfs -put data_files.zip
hdfs dfs -put python_files.zip
spark-submit \
    --archives=spark-env.tar.gz#spark-env \
    --files=hdfs://nyu-dataproc-m:8020/user/gl1589_nyu_edu/data_files.zip \
    --py-files=hdfs://nyu-dataproc-m:8020/user/gl1589_nyu_edu/python_files.zip \
    --conf "spark.driver.memory=8g" \
    --conf "spark.executor.memory=8g" \
    --conf "spark.executor.cores=4" \
    --conf "spark.executor.instances=4" \
    --conf "spark.default.parallelism=8" \
    --conf "spark.executor.memoryOverhead=2g" \
    --conf "spark.dynamicAllocation.enabled=true" \
    --conf "spark.dynamicAllocation.minExecutors=1" \
    --conf "spark.dynamicAllocation.maxExecutors=20" \
    --conf "spark.network.timeout=1000s" \
    --conf "spark.executor.heartbeatInterval=60s" \
    --conf "spark.sql.shuffle.partitions=200" \
    --conf "spark.sql.autoBroadcastJoinThreshold=20971520" \
    --conf "spark.speculation=true" \
    --conf "spark.speculation.interval=5000ms" \
    --conf "spark.speculation.multiplier=2" \
    --conf "spark.speculation.quantile=0.9" \
    --conf "spark.speculation.threshold=5" \
    --conf "spark.sql.broadcastTimeout=300" \
    --conf "spark.locality.wait=0ms" \
    --conf "spark.shuffle.service.enabled=true" \
    --conf "spark.shuffle.manager=tungsten-sort" \
    --conf "spark.memory.offHeap.enabled=true" \
    --conf "spark.memory.offHeap.size=4g" \
    --conf "spark.storage.memoryFraction=0.6" \
    --conf "spark.shuffle.memoryFraction=0.2" \
    --conf "spark.sql.codegen.wholeStage=false" \
    --conf "spark.sql.execution.arrow.enabled=true" \
    --conf "spark.executorEnv.PYARROW_IGNORE_TIMEZONE=true" \
    --conf "spark.driverEnv.PYARROW_IGNORE_TIMEZONE=true" \
    test_process_data.py
=======
export PYSPARK_DRIVER_PYTHON=python # Do not set in cluster modes.
export PYSPARK_PYTHON=./environment/bin/python
hdfs dfs -put data_files.zip
hdfs dfs -put python_files.zip
spark-submit --archives spark-env.tar.gz#environment \
             --files hdfs://nyu-dataproc-m:8020/user/gl1589_nyu_edu/data_files.zip \
             --py-files hdfs://nyu-dataproc-m:8020/user/gl1589_nyu_edu/python_files.zip \
             test_process_data.py
>>>>>>> f50299e9ff93e6535a18e9241d942ab9d7257d8d
