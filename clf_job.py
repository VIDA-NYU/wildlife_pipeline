from typing import Any, Optional
import pandas as pd
import torch
import numpy as np
import os
from transformers import pipeline, DistilBertTokenizer
import logging
import constants
from multi_model_inference import MultiModalModel, get_inference_data, run_inference


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

    def get_inference(self, df: pd.DataFrame) -> pd.DataFrame:
        classifier = self.maybe_load_classifier(task="zero-shot-classification")
        print("classifier loaded")
        if not self.column:
            self.column = "product"
            df[self.column] = df['title'].fillna(df['description']).fillna(df['name'])
        df[self.column] = df[self.column].fillna("")
        not_empty_filter = (df[self.column] != "")
        inputs = df[not_empty_filter][self.column].to_list()
        results = classifier(inputs, self.labels, hypothesis_template=self.hypothesis_template)

        # Set the results to a new column in the dataframe
        labels = [result['labels'][0] if result is not None else np.nan for result in results]
        scores = [result['scores'][0] if result is not None else np.nan for result in results]
        df.loc[not_empty_filter, "label_product"] = labels
        df.loc[not_empty_filter, "score_product"] = scores
        return df

    def get_inference_for_mmm(self, df: pd.DataFrame, bucket_name) -> pd.DataFrame:
        device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
        classifier = self.maybe_load_classifier(task="zero-shot-classification")
        indices = ~df["image_path"].isnull()

        inference_data = get_inference_data(df[indices], self.minio_client, bucket_name)
        predictions = run_inference(classifier, inference_data, device)

        df.loc[indices, 'predicted_label'] = predictions

        return df

    def get_inference_for_column(self, df: pd.DataFrame) -> pd.DataFrame:
        labels_map = {"LABEL_0": 0, "LABEL_1": 1}
        # this case cannot be zero-shot
        classifier = self.maybe_load_classifier(task="text-classification")
        print("classifier loaded")
        # Product column
        if not self.column:
            self.column = "product"
            df[self.column] = df['title'].fillna(df['description']).fillna(df['name'])
        df[self.column] = df[self.column].fillna("")
        not_empty_filter = (df[self.column] != "")
        inputs = df[not_empty_filter][self.column].to_list()
        results = classifier(inputs)

        # Set the results to a new column in the dataframe
        labels = [labels_map[result['label']] if result is not None and result['label'] in labels_map else np.nan for
                  result in results]
        scores = [result['score'] if result is not None else np.nan for result in results]
        df.loc[not_empty_filter, "label"] = labels
        df.loc[not_empty_filter, "score"] = scores
        print(f"Inference completed for {len(labels)} rows")

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
            else:
                model_load_path = './model/model.pth'

            # Check device
            device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

            # Load the model weights
            if device == torch.device('cpu'):
                loaded_model.load_state_dict(torch.load(model_load_path, map_location=device), strict=False)
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
        if x["label_product"] and x["label_description"]:
            return None
        elif x["label_product"]:
            return x["label_description"]
        elif x["label_description"]:
            return x["label_product"]
        else:
            if x["score_description"] > x["score_product"]:
                return x["label_description"]
            else:
                return x["label_product"]
