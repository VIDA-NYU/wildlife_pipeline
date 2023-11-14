from typing import Any, Optional
import pandas as pd
import torch
import numpy as np
from transformers import pipeline


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

    def get_inference(self, df: pd.DataFrame) -> pd.DataFrame:
        classifier = self.maybe_load_classifier(task=None)
        ## Product column
        df[self.column] = df[self.column].fillna("")
        not_empty_filter = (df[self.column] != "")
        inputs = df[not_empty_filter][self.column].to_list()
        results = classifier(inputs, self.labels, hypothesis_template=self.hypothesis_template)

        # Set the results to a new column in the dataframe
        labels = [result['labels'][0] if result is not None else np.nan for result in results]
        scores = [result['scores'][0] if result is not None else np.nan for result in results]
        df.loc[not_empty_filter, "label"] = labels
        df.loc[not_empty_filter, "score"] = scores

        return df

    def maybe_load_classifier(self, model: Optional[str] = 'facebook/bart-large-mnli'):
        if not self.classifier:
            device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
            self.classifier = pipeline(self.task,
                model=model,
                device=device)
            print(type(self.classifier))
        return self.classifier 
            