#!/usr/bin/env python3
# coding: utf-8

import argparse
import os
from clf_job import CLFJob
from minio_client import MinioClient

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
    parser = argparse.ArgumentParser(prog="Inference Job", description='Perform Classification')
    parser.add_argument('-bucket', type=str, required=True,
                        help="The Minio bucket to get data to perform clf -- both tasks")
    parser.add_argument('-finalbucket', type=str, required=True,
                        help="The Minio bucket to store processed data -- clf or elt")
    parser.add_argument('-model', type=str, required=False, help="Model name on Hugging Face")
    parser.add_argument('-folder', type=str, required=False, help="Folder you get all files from bucket")
    parser.add_argument('-task', type=str, required=True, choices=["text-classification", "zero-shot-classification"],
                        help="Task to perform")
    parser.add_argument('-col', type=str, required=False,
                        help="The column you wanna get perform the inference for text-classification")
    # parser.add_argument('-date', type=str, required=False, help="The date you wanna get data")

    args = parser.parse_args()

    bucket = args.bucket

    final_bucket = args.finalbucket
    date_folder = args.folder
    model = args.model

    column = args.col
    task = args.task

    access_key = os.environ["MINIO_KEY"]
    secret_key = os.environ["MINIO_SECRET"]

    minio_client = MinioClient(access_key, secret_key)
    classifier = CLFJob(bucket=bucket, final_bucket=final_bucket, minio_client=minio_client, date_folder=date_folder,
                        task=task, model=model, column=column)
    classifier.perform_clf()

    print("Job Completed")


if __name__ == "__main__":
    main()
