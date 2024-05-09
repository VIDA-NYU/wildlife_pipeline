#!/usr/bin/env python3
# coding: utf-8

import argparse
from etl_job import ETLJob
from clf_job import CLFJob
from etl_disk_job import ETLDiskJob
from minio_client import MinioClient
from elastic_search import ElasticSearch
from bloom_filter import BloomFilter
import os

from pyspark.sql import SparkSession
from pyspark import SparkFiles

def setup_environment():
    read_from_zip = False
    # Check if the local 'data/' directory exists
    local_data_dir = "data/"
    if os.path.exists(local_data_dir) and os.path.isdir(local_data_dir):
        read_from_zip = False
    else:
        # Initialize SparkSession (or SparkContext)
        spark = SparkSession.builder.getOrCreate()

        # Add a file to distribute to worker nodes
        spark.sparkContext.addFile("hdfs://nyu-dataproc-m:8020/user/gl1589_nyu_edu/data_files.zip")
        spark.sparkContext.addPyFile("hdfs://nyu-dataproc-m:8020/user/gl1589_nyu_edu/python_files.zip")
    
        read_from_zip = True

    # Set environment variables:
    os.environ["READ_FROM_ZIP"] = str(read_from_zip)

    os.environ["PYTHON_FILES_ZIP_PATH"] = SparkFiles.get("python_files.zip") if os.environ["READ_FROM_ZIP"] == "True" else "NOT FOUND"
    os.environ["DATA_FILES_ZIP_PATH"] = SparkFiles.get("data_files.zip") if os.environ["READ_FROM_ZIP"] == "True" else "NOT FOUND"

def main():
    setup_environment()
    parser = argparse.ArgumentParser(prog="ETL - Elasticsearch", description='Clean data on ES index and send it to Minio')

    # Common arguments for all
    parser.add_argument('-finalbucket', type=str, required=True, help="The Minio bucket to store processed data -- clf or elt")
    parser.add_argument('-model', type=str, required=False, help="Model name on Hugging Face")
    parser.add_argument('-bloom', type=str, required=False, help="The Minio bloom file name")
    parser.add_argument('-image', type=bool, required=False, help="Download image - True or False")
    parser.add_argument('-es', type=str, required=True, help="Specifies the ElasticSearch address to get data")
    parser.add_argument('-sd', type=str, required=True, help="The end date to filter on ES")
    parser.add_argument('-ed', type=str, required=True, help="The start date to filter on ES")
    parser.add_argument('-i', type=str, required=False, help="The index where the data is stored in ES")
    parser.add_argument('-task', type=str, required=True, choices=["text-classification", "zero-shot-classification"], help="Task to perform")
    parser.add_argument('-col', type=str, required=False, help="The column you wanna get perform the inference for text-classification")
    args = parser.parse_args()

    host = args.es
    index_start = args.i
    bloom_file = args.bloom
    end_date = args.ed
    start_date = args.sd
    final_bucket = args.finalbucket
    model = args.model

    column = args.col
    task=args.task
    save_image=args.image

    access_key = os.environ["MINIO_KEY"]
    secret_key = os.environ["MINIO_SECRET"]


    minio_client =  MinioClient(access_key, secret_key)
    es = ElasticSearch(host, index_start)
    bloom = BloomFilter(minio_client=minio_client, file_name=bloom_file)
    print("Starting Dump")
    
    from datetime import datetime, timedelta

    # Convert start and end dates to datetime objects
    start_date = datetime.strptime(start_date, '%Y-%m-%dT%H:%M:%S')
    end_date = datetime.strptime(end_date, '%Y-%m-%dT%H:%M:%S')

    # Iterate over the dates
    current_date = start_date
    while current_date < end_date:
        currend_end = current_date + timedelta(hours=2)
        # Process the current date
        ETL = ETLJob(
        bucket=final_bucket,
        minio_client=minio_client,
        bloom_filter=bloom, 
        es_client= es, 
        start_date=current_date, 
        end_date=currend_end,
        save_image=save_image,
        task=task,
        column=column,
        model=model)
        ETL.perform_etl()
        # Increment the current date by 1 day
        current_date = currend_end
    
print("Job Completed")

if __name__ == "__main__":
    main()


