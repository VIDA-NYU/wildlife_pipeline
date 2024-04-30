#!/usr/bin/env python
# coding: utf-8

import unittest
from etl_disk_job import ETLDiskJob
import json
import time
import pandas as pd
import chardet
import pybase64
import os


class TestProcessData(unittest.TestCase):

    # Test 1 (Run Extract)
    '''
    def test_extract(self):
        job = ETLDiskJob("local", None, "data/", True, None, None, None, None)
        files = os.listdir("data/") 
        for file in files:
            if file.endswith('.deflate'):
                decompressed_data = job.get_decompressed_file(file)
                cached = []
                for line in decompressed_data.splitlines():
                    json_doc = json.loads(line)
                    cached.append(json_doc)
                job.extract(cached)
    '''

    # Test 2 (Run Transform)
    '''
    def test_transform(self):
        job = ETLDiskJob("local", None, "data/", True, None, None, None, None)
        files = os.listdir("data/") 
        for file in files:
            if file.endswith('.deflate'):
                decompressed_data = job.get_decompressed_file(file)
                cached = []
                for line in decompressed_data.splitlines():
                    json_doc = json.loads(line)
                    cached.append(json_doc)
                df = job.create_df(cached)
    '''
    
    # Test 3 (Run the classifier)
    '''
    def test_perform_classification(self):
        df = pd.read_csv("test_2.csv")
        df = df[0:100]
        job = ETLDiskJob("local", None, "data/", None, "multi-model", None, None, None)

        df = df[df["title"].notnull()]
        df = job.perform_classification(df, None)
        df.to_csv("test_pred.csv", index=False)
        assert "predicted_label" in df.columns
    '''
    
    # Test 4 (Run entire pipeline)
    '''
    def test_integration(self):
        job = ETLDiskJob("local", None, "data/", None, "multi-model", None, None, None)
        job.run("", "image_data")
        files = os.listdir("data/")
        for file in files:
            filename = file.split(".")[0]
            df = pd.read_csv(filename+".csv")
            assert not df.empty
            assert "predicted_label" in df.columns
    '''
    def test_single_integration(self):
        job = ETLDiskJob("local", None, "data/", None, "multi-model", None, None, None)
        job.run("", "image_data")
        files = os.listdir("data2/")
        for file in files:
            filename = file.split(".")[0]
            df = pd.read_csv(filename+".csv")
            assert not df.empty
            assert "predicted_label" in df.columns
    

if __name__ == '__main__':
    unittest.main()
