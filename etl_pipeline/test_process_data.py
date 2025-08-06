import unittest
# from extraction import ExtractionDisk
from etl_disk_job import ETLDiskJob
import json
import time
import pandas as pd
import chardet
import pybase64
import os


class TestProcessData(unittest.TestCase):


    # def test_open_deflate(self):
    #     job = ETLDiskJob("local", None, "data/", True, None, None, None, None)
    #     files = os.listdir("./data/")
    #     files= [file for file in files if file.endswith(".deflate")]
    #     for file in files:
    #         print(f"opening {file}")
    #         decompressed_data = job.get_decompressed_file(file)
    #         cached = []
    #         for line in decompressed_data.splitlines():
    #             json_doc = json.loads(line)
    #             cached.append(json_doc)
    #         print("File completed")


    #Test1

    # def test_create_df(self):
    #     job = ETLDiskJob("local", None, "data/", True, None, None, None, None)
    #     decompressed_data = job.get_decompressed_file("crawl_data-1699796707793-0.deflate")

    #     cached = []
    #     for line in decompressed_data.splitlines():
    #         json_doc = json.loads(line)
    #         cached.append(json_doc)

    #     df = job.create_df(cached)
    #     df_origin = pd.read_csv("test_extraction.csv")
    #     assert df["seller"] == df_origin["seller"]
    #     assert df["location"] == df_origin["location"]

     #Test2

    # def test_perform_classification(self):
    #     df = pd.read_csv("test_3.csv")
    #     df = df[0:100]
    #     job = ETLDiskJob("local", None, "data/", None, "multi-model", None, None, None)

    #     df = df[df["title"].notnull()]
    #     df = job.perform_classification(df, None)
    #     df.to_csv("test_pred.csv", index=False)
    #     assert "predicted_label" in df.columns

    #  #Test3

    def test_integration(self):
        job = ETLDiskJob("local", None, "/Users/julesbarbosa/work/wildlife_pipeline/data/", None, None, None, None, None, "processed_files" )
        job.run("", "image_data")
        # files = os.listdir("./data/")
        # for file in files:
        #     filename = file.split(".")[0]
        #     df = pd.read_csv(filename+".csv")
        #     assert not df.empty
        #     assert "predicted_label" in df.columns


    # def test_create_df(self):
    #     job = ExtractionDisk("local", None, "data/", True, None, None, None, None)
    #     decompressed_data = job.get_decompressed_file("crawl_data-1699796707793-0.deflate")

    #     cached = []
    #     for line in decompressed_data.splitlines():
    #         json_doc = json.loads(line)
    #         cached.append(json_doc)

    #     df = job.create_df(cached)
    #     df_origin = pd.read_csv("test_extraction.csv")



if __name__ == '__main__':
    unittest.main()
