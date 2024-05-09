#!/usr/bin/env python3
# coding: utf-8

import argparse
import os
from etl_disk_job import ETLDiskJob
from minio_client import MinioClient
from bloom_filter import BloomFilter

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
    args = create_arg_parser()
    bloom_file = args.bloom
    final_bucket = args.finalbucket
    date = args.date
    model = args.model
    filename = args.filename
    temporal = args.temporal

    column = args.col
    task = args.task
    save_image = args.image

    access_key = os.environ["MINIO_KEY"]
    secret_key = os.environ["MINIO_SECRET"]

    minio_client = MinioClient(access_key, secret_key)
    if bloom_file:
        bloom = BloomFilter(minio_client=minio_client, file_name=bloom_file)
    else:
        bloom = None

    if temporal:
        print("Starting Temporal ETL Job from Disk")
        files = os.listdir("/data")
        for file in files:
            path = f"/data/{file}/{filename}/data_pages/"
            folder_name = f"{filename}_{file}/"
            etl_job = ETLDiskJob(bucket=final_bucket, minio_client=minio_client, path=path, save_image=save_image,
                                 task=task, column=column, model=model, bloom_filter=bloom)
            etl_job.run(folder_name=folder_name, date=date)
    else:
        print("Starting regular ETL Job from Disk")
        path = f"/data/{filename}/data_pages/"
        folder_name = ""
        etl_job = ETLDiskJob(bucket=final_bucket, minio_client=minio_client, path=path, save_image=save_image,
                             task=task, column=column, model=model, bloom_filter=bloom)
        etl_job.run(folder_name=folder_name, date=date)

    print("Job Completed")


def create_arg_parser():
    # Create a command-line interface for our customizable ETL pipeline

    parser = argparse.ArgumentParser(prog="clean_data", description='Clean data on Disk and send it to Minio')

    # Common arguments for all
    parser.add_argument('-finalbucket', type=str, required=True,
                        help="The Minio bucket to store processed data -- clf or elt")
    parser.add_argument('-model', type=str, required=False, help="Model name on Hugging Face")
    parser.add_argument('-bloom', type=str, required=False, help="The Minio bloom file name")
    parser.add_argument('-task', type=str, required=False, choices=["text-classification", "zero-shot-classification", "both", "multi-model"],
                        help="Task to perform")
    parser.add_argument('-image', type=bool, required=False, help="Download image - True or False")
    parser.add_argument('-col', type=str, required=False,
                        help="The column on which you want to perform the inference for text-classification")
    parser.add_argument('-filename', type=str, required=False,
                        help="filename on disk - for temporal task, it is the crawler ID")
    parser.add_argument('-date', type=str, required=False, help="The date of image bucket")
    parser.add_argument('-temporal', type=bool, required=False,
                        help="Where you want the data from the Temporal Analysis or not")

    return parser.parse_args()


if __name__ == "__main__":
    main()
