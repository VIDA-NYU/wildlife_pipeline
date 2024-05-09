#!/usr/bin/env python3
# coding: utf-8

from typing import Any, Optional
import pandas as pd
import torch
import numpy as np
import os
from transformers import pipeline, DistilBertTokenizer
import logging
import constants

from pyspark.sql import SparkSession
<<<<<<< HEAD
from pyspark import SparkFiles
#if os.environ["READ_FROM_ZIP"] == "True":
    # Initialize SparkSession (or SparkContext)
    #spark = SparkSession.builder.getOrCreate()
    # Add a file to distribute to worker nodes
    #spark.sparkContext.addFile("hdfs://nyu-dataproc-m:8020/user/gl1589_nyu_edu/data_files.zip")
    #spark.sparkContext.addPyFile("hdfs://nyu-dataproc-m:8020/user/gl1589_nyu_edu/python_files.zip")

#os.environ["DATA_FILES_ZIP_PATH"] = "hdfs://nyu-dataproc-m:8020/user/gl1589_nyu_edu/data_files.zip" if  os.environ["READ_FROM_ZIP"] == "True" else "NOT FOUND"
#os.environ["PYTHON_FILES_ZIP_PATH"] = "hdfs://nyu-dataproc-m:8020/user/gl1589_nyu_edu/python_files.zip" if os.environ["READ_FROM_ZIP"] == "True" else "NOT FOUND"

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
        
        #os.environ["PYTHON_FILES_ZIP_PATH"] = SparkFiles.get("python_files.zip") if os.environ["READ_FROM_ZIP"] == "True" else "NOT FOUND"
        #os.environ["DATA_FILES_ZIP_PATH"] = SparkFiles.get("data_files.zip") if os.environ["READ_FROM_ZIP"] == "True" else "NOT FOUND"
    # Set environment variables:
    os.environ["READ_FROM_ZIP"] = str(read_from_zip)

    os.environ["PYTHON_FILES_ZIP_PATH"] = SparkFiles.get("python_files.zip") if os.environ["READ_FROM_ZIP"] == "True" else "NOT FOUND"
    os.environ["DATA_FILES_ZIP_PATH"] = SparkFiles.get("data_files.zip") if os.environ["READ_FROM_ZIP"] == "True" else "NOT FOUND"
=======

if os.environ["READ_FROM_ZIP"] == "True":
    # Initialize SparkSession (or SparkContext)
    spark = SparkSession.builder.getOrCreate()
    # Add a file to distribute to worker nodes
    spark.sparkContext.addFile("hdfs://nyu-dataproc-m:8020/user/gl1589_nyu_edu/data_files.zip")
    spark.sparkContext.addPyFile("hdfs://nyu-dataproc-m:8020/user/gl1589_nyu_edu/python_files.zip")

os.environ["DATA_FILES_ZIP_PATH"] = "hdfs://nyu-dataproc-m:8020/user/gl1589_nyu_edu/data_files.zip" if  os.environ["READ_FROM_ZIP"] == "True" else "NOT FOUND"
os.environ["PYTHON_FILES_ZIP_PATH"] = "hdfs://nyu-dataproc-m:8020/user/gl1589_nyu_edu/python_files.zip" if os.environ["READ_FROM_ZIP"] == "True" else "NOT FOUND"

>>>>>>> f50299e9ff93e6535a18e9241d942ab9d7257d8d
from multi_model_inference import MultiModalModel, get_inference_data, run_inference
import zipfile
import io

# Spark-related import statements:
from pyspark.sql import SparkSession
import databricks.koalas as ks

setup_environment()
class CLFJob:
    def __init__(
            self,
            bucket: str,
            final_bucket: str,
            minio_client: Any,
            date_folder: Optional[str],
            column: Optional[str],
            model: Optional[str],
            task: str):
        self.bucket = bucket
        self.final_bucket = final_bucket
        self.minio_client = minio_client
        self.column = column
        self.date = date_folder
        self.classifier = None
        self.task = task
        self.labels = constants.classifier_target_labels
        self.hypothesis_template = constants.classifier_hypothesis_template
        self.model = model
        self.spark = SparkSession.builder.getOrCreate() # Default settings
        ks.set_option('compute.default_index_type', 'distributed')  # Set index type to distributed for large datasets

    def perform_clf(self):
        files = self.minio_client.list_objects_names(self.bucket, self.date)
        for file in files:
            filename = file.split(".")[0]

            if self.final_bucket != self.bucket and self.minio_client.check_obj_exists(self.final_bucket, file):
                logging.warning(f"File {file} ignored as it already present in destination")
                continue
            logging.info(f"Starting inference for {file}")
            df = self.minio_client.read_df_parquet(bucket=self.bucket, file_name=file)
            if df.empty:
                logging.warning(f"File {file} is empty")
                continue
            if self.task == "zero-shot-classification":
                df = self.get_inference(df=df)
            elif self.task == "text-classification":
                df = self.get_inference_for_column(df=df)
            elif self.task == "both":
                df = self.get_inference(df=df)
                df = self.get_inference_for_column(df=df)
            else:
                raise ValueError(f"Task \"{self.task}\" is not available")

            self.minio_client.save_df_parquet(self.final_bucket, filename, df)

    def get_inference(self, df: ks.DataFrame) -> ks.DataFrame:
        classifier = self.maybe_load_classifier(task="zero-shot-classification")
        print("classifier loaded")
        
        if not self.column:
            self.column = "product"
            df[self.column] = df['title'].fillna(df['description']).fillna(df['name'])
        
        df[self.column] = df[self.column].fillna("")
        not_empty_filter = df[self.column] != ""
        filtered_df = df[not_empty_filter]
        
        # Processing inputs and collecting results
        if not filtered_df.empty:
            inputs = filtered_df[self.column].to_numpy().tolist()  # Using to_numpy() instead of to_list() for Koalas
            results = classifier(inputs, self.labels, hypothesis_template=self.hypothesis_template)
            
            # Setting the results in the DataFrame
            labels = [result['labels'][0] if result is not None else np.nan for result in results]
            scores = [result['scores'][0] if result is not None else np.nan for result in results]
            filtered_df = filtered_df.assign(label_product=labels, score_product=scores)
        
        # Merge the results back to the original DataFrame
        df = df.assign(label_product=np.nan, score_product=np.nan)
        df.update(filtered_df[['label_product', 'score_product']], join='left')

        return df

    def get_inference_for_mmm(self, df: ks.DataFrame, bucket_name) -> ks.DataFrame:
        device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
        classifier = self.maybe_load_classifier(task="zero-shot-classification")
        
        # Filtering rows where 'image_path' is not null
        indices = df["image_path"].notnull()
        filtered_df = df[indices]
        
        if not filtered_df.empty:
            # Assuming get_inference_data and run_inference are adapted to work with Koalas or return compatible outputs
            inference_data = get_inference_data(filtered_df, self.minio_client, bucket_name)
            predictions = run_inference(classifier, inference_data, device)
            
            # Creating a new column for predictions in the filtered DataFrame
            filtered_df = filtered_df.assign(predicted_label=predictions)

            # Update the original DataFrame with the new predictions
            df = df.assign(predicted_label=np.nan)  # Initialize the column with NaNs
            df.update(filtered_df[['predicted_label']], join='left')

        return df

    def get_inference_for_column(self, df: ks.DataFrame) -> ks.DataFrame:
        labels_map = {"LABEL_0": 0, "LABEL_1": 1}
        classifier = self.maybe_load_classifier(task="text-classification")
        print("classifier loaded")
        
        # Ensure the Product column is set
        if not self.column:
            self.column = "product"
            df[self.column] = df['title'].fillna(df['description']).fillna(df['name'])
        df[self.column] = df[self.column].fillna("")
        
        # Filter the dataframe to process non-empty entries only
        not_empty_df = df[df[self.column] != ""]
        
        if not not_empty_df.empty:
            # Convert column to list and perform classification
            inputs = not_empty_df[self.column].to_numpy().tolist()
            results = classifier(inputs)

            # Process results and map them back to the original dataframe
            labels = [labels_map[result['label']] if result is not None and result['label'] in labels_map else np.nan for result in results]
            scores = [result['score'] if result is not None else np.nan for result in results]

            not_empty_df = not_empty_df.assign(label=labels, score=scores)

            # Update the original dataframe with the results
            df = df.assign(label=np.nan, score=np.nan)  # Initialize label and score columns with NaNs
            df.update(not_empty_df[['label', 'score']], join='left')

        print(f"Inference completed for {len(not_empty_df)} rows")
        
        return df

    def maybe_load_classifier(self, task: Optional[str]):
        device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
        if self.task == "text-classification":
            model = self.model or 'julesbarbosa/wildlife-classification'
            if not self.classifier:
                self.classifier = pipeline(self.task,
                                        model=model,
                                        device=device,
                                        use_auth_token=os.environ["HUGGINGFACE_API_KEY"])
            return self.classifier
        elif self.task == "zero-shot-classification":
            model = self.model or 'facebook/bart-large-mnli'
            if not self.classifier:
                self.classifier = pipeline(self.task,
                                        model=model,
                                        device=device,
                                        use_auth_token=os.environ["HUGGINGFACE_API_KEY"])
            return self.classifier
        elif self.task == "multi-model":
            # Initialize an empty model
            loaded_model = MultiModalModel(num_labels=2)
            # Load the state dictionary
            if self.minio_client:
                model_load_path = './model.pth'
                self.minio_client.get_model("multimodal", "model.pth", model_load_path)

            # Check device
            device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
            
            # Load the model weights
            if device == torch.device('cpu'):
                if os.environ["READ_FROM_ZIP"] == "True":
                    with zipfile.ZipFile(os.environ["DATA_FILES_ZIP_PATH"], 'r') as zip_ref:
                        if 'model/' in zip_ref.namelist():
                            with zip_ref.open('model/model.pth', 'r') as model_file:
                                model_content = model_file.read()
                                loaded_model.load_state_dict(torch.load(io.BytesIO(model_content), map_location=device), strict=False)
                                print("Model state dictionary loaded successfully.")
                else:
                    loaded_model.load_state_dict(torch.load(model_load_path, map_location=device), strict=False)
            else:
                if os.environ["READ_FROM_ZIP"] == "True":
                    with zipfile.ZipFile(os.environ["DATA_FILES_ZIP_PATH"], 'r') as zip_ref:
                        if 'model/' in zip_ref.namelist():
                            with zip_ref.open('model/model.pth', 'r') as model_file:
                                model_content = model_file.read()
                                loaded_model.load_state_dict(torch.load(io.BytesIO(model_content)), strict=False)
                                print("Model state dictionary loaded successfully.")
                else:
                    loaded_model.load_state_dict(torch.load(model_load_path), strict=False)

            # Move model to evaluation mode and to the device
            loaded_model.eval()
            loaded_model = loaded_model.to(device)
            self.classifier = loaded_model
            return self.classifier
        elif self.task == "both":
            if task == "text-classification":
                model = 'julesbarbosa/wildlife-classification'
            else:
                model = 'facebook/bart-large-mnli'
            return pipeline(task,
                                model=model,
                                device=device,
                                use_auth_token=os.environ["HUGGINGFACE_API_KEY"])


    @staticmethod
    def get_label(x):
        # Improved logic for determining labels based on available data
        if x["label_product"] and x["label_description"]:
            if x["score_description"] > x["score_product"]:
                return x["label_description"]
            else:
                return x["label_product"]
        return x.get("label_product") or x.get("label_description")
