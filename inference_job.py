#!/usr/bin/env python3
# coding: utf-8

from typing import Any, Optional
import pandas as pd
import torch
import numpy as np
from transformers import pipeline
import databricks.koalas as ks

class InferenceJob():
    def __init__(self, bucket: str, final_bucket: str, minio_client: Any, column: str, task: str):
        self.bucket = bucket
        self.final_bucket = final_bucket
        self.minio_client=minio_client
        self.column = column
        self.classifier = None
        self.date = None
        self.task = task
        self.labels = ["a real animal",
          "a toy",
          "a print of an animal",
          "an object",
          "a faux animal",
          "an animal body part",
          "an faux animal body part"]
        self.hypothesis_template = 'This product advertisement is about {}.'


    def perform_clf(self):
        files = self.minio_client.list_objects_names(self.bucket, None)
        for file in files:
            filename =  file.split(".")[0]
            df = self.minio_client.read_df_parquet(bucket= self.bucket, file_name=file)
            if not df.empty:
                df = self.get_inference(df=df)
                self.minio_client.save_df_parquet(self.final_bucket, filename, df)
            else:
                continue

    def get_inference(self, df: ks.DataFrame) -> ks.DataFrame:
        classifier = self.maybe_load_classifier(task=None)
        ## Product column
        df[self.column] = df[self.column].fillna("")

        # Filter out non-empty entries
        not_empty_df = df[df[self.column] != ""]

        # Apply classifier to non-empty entries
        def classify_text(text):
            if text:
                result = classifier([text], self.labels, hypothesis_template=self.hypothesis_template)
                if result:
                    return result['labels'][0], result['scores'][0]
            return np.nan, np.nan

        # Using 'apply' to create new columns for labels and scores
        not_empty_df[['label', 'score']] = not_empty_df[self.column].apply(lambda x: classify_text(x)).apply(list).to_list()

        # Combine the results back into the original DataFrame
        df = df.assign(label=np.nan, score=np.nan)  # Initialize the columns
        df.update(not_empty_df[['label', 'score']], join='left')  # Update from not_empty_df

        return df

    def maybe_load_classifier(self, model: Optional[str] = 'facebook/bart-large-mnli'):
        if not self.classifier:
            device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
            self.classifier = pipeline(self.task,
                model=model,
                device=device)
            print(type(self.classifier))
        return self.classifier 
            