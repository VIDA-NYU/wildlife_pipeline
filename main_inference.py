#!/usr/bin/env python
# coding: utf-8

import argparse
from pyspark import SparkFiles
import os
from pyspark.sql import SparkSession

if os.environ["READ_FROM_ZIP"] == "True":
    # Initialize SparkSession (or SparkContext)
    spark = SparkSession.builder.getOrCreate()
    # Add a file to distribute to worker nodes
    spark.sparkContext.addFile("data_files.zip")
    spark.sparkContext.addFile("python_files.zip")

os.environ["DATA_FILES_ZIP_PATH"] = SparkFiles.get("data_files.zip") if  os.environ["READ_FROM_ZIP"] == "True" else "NOT FOUND"
os.environ["PYTHON_FILES_ZIP_PATH"] = SparkFiles.get("python_files.zip") if os.environ["READ_FROM_ZIP"] == "True" else "NOT FOUND"


from clf_job import CLFJob
from minio_client import MinioClient


def main():
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
