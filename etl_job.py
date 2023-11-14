import logging
from typing import Any
from process_data import ProcessData
from datetime import datetime
import json
import os


class ETLJob(ProcessData):
    def __init__(
            self,
            bucket: str,
            minio_client: Any,
            bloom_filter: Any,
            es_client: Any,
            start_date: str,
            end_date: str,
            save_image: bool,
            task: str,
            column: str):
        super().__init__(bloom_filter=bloom_filter, minio_client=minio_client, bucket=bucket, task=task, column=column)
        self.es_client = es_client
        self.start_date = start_date
        self.end_date = end_date
        self.save_image = save_image

    def perform_etl(self):
        path = self.es_client.get_docs(start_date=self.start_date, end_date=self.end_date)
        jsonl_iterator = JSONLBatchIterator(path, 5000)
        date = datetime.strptime(str(self.start_date), '%Y-%m-%d %H:%M:%S').strftime("%b_%d")
        month = date.split('_')[0].lower()
        image_folder = f"data_{date}/"
        image_bucket = f"images-{month}"
        for docs in jsonl_iterator:
            processed_df = self.extract_information_from_docs(docs)
            if not processed_df.empty:
                ct = datetime.now()
                file = f"data_{date}/data_" + ct.strftime("%m-%d-%YT%H:%M:%S")
                if self.task:
                    processed_df = self.run_classification(df=processed_df)
                self.minio_client.save_df_parquet(self.bucket, file, processed_df)
                logging.info(f"Documents from {file} successfully indexed on minio")
                self.bloom_filter.save()
                if self.save_image:
                    self.send_image(processed_df, image_folder, image_bucket)
        logging.info("All documents processed")
        os.remove(path)


class JSONLBatchIterator:
    def __init__(self, file_path, batch_size):
        self.file_path = file_path
        self.batch_size = batch_size

    def __iter__(self):
        batch = []
        with open(self.file_path, "r") as f:
            for line in f:
                doc = json.loads(line)
                batch.append(doc)

                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []

            if batch:
                yield batch
