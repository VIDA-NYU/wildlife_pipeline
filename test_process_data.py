#!/usr/bin/env python
# coding: utf-8

import os
from pyspark import SparkFiles

read_from_zip = False
# Check if the local 'data/' directory exists
local_data_dir = "data/"
if os.path.exists(local_data_dir) and os.path.isdir(local_data_dir):
    read_from_zip = False
else:
    read_from_zip = True    

# Set environment variables:
os.environ["READ_FROM_ZIP"] = str(read_from_zip)
# Access the path of the zip file distributed with --files
os.environ["DATA_FILES_ZIP_PATH"] = SparkFiles.get("data_files.zip") if read_from_zip else None
os.environ["PYTHON_FILES_ZIP_PATH"] = SparkFiles.get("python_files.zip") if read_from_zip else None

# Now imports should be fine
import unittest
from etl_disk_job import ETLDiskJob
import json
import time
import pandas as pd
import chardet
import pybase64
import zipfile
import io


class TestProcessData(unittest.TestCase):

    # Helper functions to process data
    def process_data_helper(self, job, file, job_type):
        decompressed_file = job.get_decompressed_file(file)
        cached = []
        for line in decompressed_file.splitlines():
            json_doc = json.loads(line)
            cached.append(json_doc)
        match job_type:
            case "EXTRACT": job.extract(cached)
            case "TRANSFORM": df = job.create_df(cached)

    def classification_helper(self, df):
        df = df[0:100]
        job = ETLDiskJob("local", None, os.environ["DATA_FILES_ZIP_PATH"] if os.environ["READ_FROM_ZIP"] == "True" else 'data/', None, "multi-model", None, None, None)

        df = df[df["title"].notnull()]
        df = job.perform_classification(df, None)
        df.to_csv("test_pred.csv", index=False)
        assert "predicted_label" in df.columns
    
    def integration_helper(self, file):
        filename = file.split(".")[0]
        df = pd.read_csv(filename+".csv")
        assert not df.empty
        assert "predicted_label" in df.columns

    # Test 1 (Run Extract)
    def test_extract(self):
        if os.environ["READ_FROM_ZIP"] == "True":
            job = ETLDiskJob("local", None, os.environ["DATA_FILES_ZIP_PATH"], True, None, None, None, None)
            with zipfile.ZipFile(os.environ["DATA_FILES_ZIP_PATH"], "r") as zip_ref:
                data_files = [file for file in zip_ref.namelist() if file.startswith('data/')]                
                for file_name in data_files:
                    if(file_name.endswith('.deflate')): # Superfluous check... 
                        with zip_ref.open(file_name) as file:
                            self.process_data_helper(job, file, "EXTRACT")
        else:
            job = ETLDiskJob("local", None, "data/", True, None, None, None, None)
            files = os.listdir("data/") 
            for file in files:
                if file.endswith('.deflate'):
                    self.process_data_helper(job, file, "EXTRACT")


    # Test 2 (Run Transform)
    '''
    def test_transform(self):
        if os.environ["READ_FROM_ZIP"] == "True":
            job = ETLDiskJob("local", None, os.environ["DATA_FILES_ZIP_PATH"], True, None, None, None, None)
            with zipfile.ZipFile(os.environ["DATA_FILES_ZIP_PATH"], "r") as zip_ref:
                data_files = [file for file in zip_ref.namelist() if file.startswith('data/')]                
                for file_name in data_files:
                    if(file_name.endswith('.deflate')):
                        with zip_ref.open(file_name) as file:
                            self.process_data_helper(job, file, "TRANSFORM")
        else:
            job = ETLDiskJob("local", None, "data/", True, None, None, None, None)
            files = os.listdir("data/") 
            for file in files:
                if file.endswith('.deflate'):
                    self.process_data_helper(job, file, "TRANSFORM")    
    '''
    
    # Test 3 (Run the classifier)
    '''
    def test_perform_classification(self):
        if os.environ["READ_FROM_ZIP"] == "True":
            with zipfile.ZipFile(os.environ["DATA_FILES_ZIP_PATH"], "r") as zip_ref:
                if 'test_2.csv' in zip_ref.namelist():
                    with zip_ref.open('test_2.csv') as file:
                        file_content_bytes = file.read()
                        file_content_io = io.BytesIO(file_content_bytes)
                        df = pd.read_csv(file_content_io)
                        self.classification_helper(df)
        else:
            df = pd.read_csv("test_2.csv")
            self.classification_helper(df)
    '''

    # Test 4 (Run entire pipeline)
    '''
    def test_integration(self):
        if os.environ["READ_FROM_ZIP"] == "True":
            job = ETLDiskJob("local", None, os.environ["DATA_FILES_ZIP_PATH"], True, None, None, None, None)
            job.run("", "image_data")
            with zipfile.ZipFile(os.environ["DATA_FILES_ZIP_PATH"], "r") as zip_ref:
                data_files = [file for file in zip_ref.namelist() if file.startswith('data/')]                   
                for file_name in data_files:
                    if(file_name.endswith('.deflate')):
                        self.integration_helper(file_name)
        else:
            job = ETLDiskJob("local", None, "data/", None, "multi-model", None, None, None)
            job.run("", "image_data")
            files = os.listdir("data/")
            for file in files:
                if(file.endswith('.deflate')):
                    self.integration_helper(file)
    '''
    
    # Test single integration:
    '''
    def test_single_integration(self):
        if os.environ["READ_FROM_ZIP"] == "True":
            job = ETLDiskJob("local", None, os.environ["DATA_FILES_ZIP_PATH"], True, None, None, None, None)
            job.run("", "image_data")
            with zipfile.ZipFile(os.environ["DATA_FILES_ZIP_PATH"], "r") as zip_ref:
                data_files = [file for file in zip_ref.namelist() if file.startswith('data2/')]                   
                for file_name in data_files:
                    if(file_name.endswith('.deflate')):
                        self.integration_helper(file_name)
        else:    
            job = ETLDiskJob("local", None, "data/", None, "multi-model", None, None, None)
            job.run("", "image_data")
            files = os.listdir("data2/")
            for file in files:
                if(file.endswith('.deflate')):
                    self.integration_helper(file)
    ''' 

if __name__ == '__main__':
    unittest.main()
