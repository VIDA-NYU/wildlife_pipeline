#!/bin/bash
cd ..
export PYSPARK_PYTHON=/home/gl1589_nyu_edu/spark-env/bin/python
export PYTHONWARNINGS=ignore
spark-submit \
    --master local[*] \
    --deploy-mode client \
    --conf "spark.driver.memory=4g" \
    --conf "spark.executor.memory=4g" \
    --conf "spark.executor.cores=2" \
    --conf "spark.executor.instances=2" \
    --conf "spark.default.parallelism=4" \
    --conf "spark.task.cpus=1" \
    --conf "spark.executorEnv.PYARROW_IGNORE_TIMEZONE=1" \
    --conf "spark.driverEnv.PYARROW_IGNORE_TIMEZONE=1" \
    test_process_data.py