from typing import Any, Optional
import zlib
import json
import pandas as pd
import os

from bloom_filter import BloomFilter
from process_data import ProcessData
import base64
from urllib.parse import urlparse
import arrow
from bs4 import BeautifulSoup
import extruct
from mlscraper.html import Page
import chardet
import logging
import constants
import pybase64


from create_metadata import (
    get_sintax_opengraph,
    get_sintax_dublincore,
    get_dict_json_ld,
    get_dict_microdata
)

class ETLDiskJob(ProcessData):
    def __init__(self, bucket: str, minio_client: Any, path: str, save_image: Optional[bool], task: Optional[str], column: str,
                 model: str, bloom_filter: Optional[BloomFilter]):
        super().__init__(bloom_filter=bloom_filter, minio_client=minio_client, bucket=bucket, task=task, column=column,
                         model=model)
        self.bucket = bucket
        self.path = path
        self.save_image = save_image
        self.task = task

    def get_files(self):
        try:
            files = os.listdir(self.path)
            files= [file for file in files if file.endswith(".deflate")]
            logging.info(f"{len(files)} files to be processed")
        except FileNotFoundError:
            logging.error(f"No files on {self.path}")
            return None
        return files

    def run(self, folder_name: str, date: Optional[str]) -> None:
        # Run ETL for all the files on the given path on disk
        files = self.get_files()

        if files:
            for file in files:
                logging.info(f"Starting processing file {file}")
                final_filename = file.split(".")[0]
                final_filename = f"{folder_name}{final_filename}"
                if self.minio_client:
                    checked_obj = self.minio_client.check_obj_exists(self.bucket, final_filename + ".parquet")
                else:
                    checked_obj = False
                if not checked_obj:
                    cached = []
                    processed = []
                    count = 0
                    decompressed_data = self.get_decompressed_file(file)

                    # Processing decompressed data in batch size of 5000 records
                    for line in decompressed_data.splitlines():
                        json_doc = json.loads(line)
                        cached.append(json_doc)
                        count += 1
                        if count % 5000 == 0:
                            processed_df = self.extract_information_from_docs(cached)
                            processed.append(processed_df)
                            count = 0
                            cached = []
                    if len(cached) > 0:
                        processed_df = self.extract_information_from_docs(cached)
                        processed.append(processed_df)
                    if len(processed) > 0:
                        processed_df = pd.concat(processed).reset_index(drop=True)
                        processed_df = processed_df[processed_df["title"].notnull()]
                        if self.minio_client:
                            self.load_file_to_minio(final_filename, processed_df)
                        else:
                            final_filename = final_filename+".csv"
                            processed_df.to_csv(final_filename, index=False)
                        if self.minio_client:
                            image_bucket = f"images-{date}"
                        else:
                            image_bucket = None
                        df_w_image_path = self.save_image_if_applicable(processed_df, date)
                        processed_df = self.perform_classification(df_w_image_path, image_bucket)
                        if not processed_df.empty:
                            if self.minio_client:
                                self.load_file_to_minio(final_filename, processed_df)
                            else:
                                processed_df.to_csv(final_filename, index=False)
                else:
                    df = self.minio_client.read_df_parquet(self.bucket, final_filename + ".parquet")
                    if "image_path" not in df.columns:
                        image_bucket = f"images-{date}"
                        df_w_image_path = self.save_image_if_applicable(df, date)
                        processed_df = self.perform_classification(df_w_image_path, image_bucket)
                        if not processed_df.empty:
                            self.load_file_to_minio(final_filename, processed_df)
                    else:
                        logging.info(f"file {final_filename} already indexed")
            logging.info("ETL Job run completed")

    # Run classification and load data to MinIO
    def perform_classification(self, processed_df, bucket_name = Optional[str]):
        if self.task:
            processed_df = self.run_classification(df=processed_df, bucket_name=bucket_name)
        return processed_df

    def save_image_if_applicable(self, processed_df, date):
        if self.save_image:
            if self.minio_client:
                image_bucket = f"images-{date}"
                return self.send_image(processed_df, None, image_bucket, self.task)
            else:
                return ETLDiskJob.save_image_local(processed_df, date)
        return processed_df

    def load_file_to_minio(self, file_name, df):
        self.minio_client.save_df_parquet(self.bucket, file_name, df)
        self.bloom_filter.save()
        logging.info("Document successfully indexed on minio")

    def maybe_check_bloom(self, text):
        if self.bloom_filter:
            self.bloom_filter.check_bloom_filter(text)
        else:
            return False

    def create_df(self, ads: list) -> pd.DataFrame:
        final_dict = []
        for ad in ads:
            html_content = ETLDiskJob.get_decoded_html_from_bytes(ad["content"])
            if html_content:
                content_type = ad["content_type"]
                parser = ProcessData.get_parser(content_type)
                soup = BeautifulSoup(html_content, parser)
                text, title = ETLDiskJob.get_text_title(soup=soup)
                if not ProcessData.remove_text(text) and not self.maybe_check_bloom(text):
                    domain = ETLDiskJob.get_domain(ad["url"])
                    dict_df = {
                        "url": ad["url"],
                        "title": title,
                        "text": text,
                        "domain": domain,
                        "retrieved": ETLDiskJob.get_time(ad["fetch_time"]),
                        "name": None,
                        "description": None,
                        "image": None,
                        "production_data": None,
                        "category": None,
                        "price": None,
                        "currency": None,
                        "seller": None,
                        "seller_type": None,
                        "seller_url": None,
                        "location": None,
                        "ships to": None,
                    }
                    final_dict.append(dict_df)
                    domain = domain.split(".")[0]
                    if "ebay" in domain:
                        extract_dict = dict_df.copy()
                        self.add_seller_information_to_metadata(domain, extract_dict, soup)
                        final_dict.append(extract_dict)
                    try:
                        if self.minio_client and domain in constants.DOMAIN_SCRAPERS:
                            extract_dict = dict_df.copy()
                            scraper = self.open_scrap(self.minio_client, domain)
                            extract_dict.update(scraper.get(Page(html_content)))
                            if extract_dict.get("product"):
                                extract_dict["name"] = extract_dict.pop("product")
                            final_dict.append(extract_dict)
                    except Exception as e:
                        logging.error(e)
                    try:
                        metadata = None
                        metadata = extruct.extract(html_content,
                                                   base_url=ad["url"],
                                                   uniform=True,
                                                   syntaxes=['json-ld',
                                                             'microdata',
                                                             'opengraph',
                                                             'dublincore'])
                    except Exception as e:
                        logging.error(f"Exception on extruct: {e}")
                    if metadata:
                        if metadata.get("microdata"):
                            for product in metadata.get("microdata"):
                                micro = get_dict_microdata(product)
                                if micro:
                                    extract_dict = dict_df.copy()
                                    extract_dict.update(micro)
                                    final_dict.append(extract_dict)
                        if metadata.get("opengraph"):
                            open_ = get_sintax_opengraph(metadata.get("opengraph")[0])
                            if open_:
                                extract_dict = dict_df.copy()
                                extract_dict.update(open_)
                                final_dict.append(extract_dict)
                        if metadata.get("dublincore"):
                            dublin = get_sintax_dublincore(metadata.get("dublincore")[0])
                            if dublin:
                                extract_dict = dict_df.copy()
                                extract_dict.update(dublin)
                                final_dict.append(extract_dict)
                        if metadata.get("json-ld"):
                            for meta in metadata.get("json-ld"):
                                if meta.get("@type") == 'Product':
                                    json_ld = get_dict_json_ld(meta)
                                    if json_ld:
                                        extract_dict = dict_df.copy()
                                        extract_dict.update(json_ld)
                                        final_dict.append(extract_dict)
                                        extract_dict = None
                    metadata = None
        df_metas = pd.DataFrame()
        if len(final_dict) > 0:
            df_metas = pd.DataFrame(final_dict)
            df_metas["price"] = df_metas["price"].apply(lambda x: ProcessData.fix_price_str(x))
            df_metas["currency"] = df_metas["currency"].apply(lambda x: ProcessData.fix_currency(x))
            df_metas = df_metas.groupby('url').agg({
                "title": 'first',
                "text": 'first',
                "domain": 'first',
                "name": 'first',
                "description": 'first',
                "image": 'first',
                "retrieved": 'first',
                "production_data": 'first',
                "category": 'first',
                "price": 'first',
                "currency": 'first',
                "seller": 'first',
                "seller_type": 'first',
                "seller_url": 'first',
                "location": 'first',
                "ships to": 'first'}).reset_index()
            df_metas = ProcessData.assert_types(df_metas)
            columns_to_fix = ["title", "text", "name", "description"]
            df_metas[columns_to_fix] = df_metas[columns_to_fix].applymap(ProcessData.maybe_fix_text)

        return df_metas

    def get_decompressed_file(self, file):
        print(f"FILE PATH {os.path.abspath(file)}")
        with open(f"{self.path}{file}", "rb") as f:
            decompressor = zlib.decompressobj()
            decompressed_data = decompressor.decompress(f.read())
            logging.info(f"file {file} decompressed")
            file_size = len(decompressed_data)
            logging.info(f"The size of the decompressed file is {file_size} bytes")
        return decompressed_data

    @staticmethod
    def get_decoded_html_from_bytes(content):
        try:
            # Attempt decoding with utf-8 encoding
            decoded_bytes = pybase64.b64decode(content, validate=True)
            html_content = decoded_bytes.decode('utf-8')

        except UnicodeDecodeError:
            try:
                # Attempt decoding with us-ascii encoding
                html_content = decoded_bytes.decode('ascii')
                print("us-ascii worked")
            except UnicodeDecodeError:
                # If both utf-8 and us-ascii decoding fail, use chardet for detection
                detection = chardet.detect(decoded_bytes)
                try:
                    html_content = decoded_bytes.decode(detection["encoding"])
                except UnicodeDecodeError as e:
                    logging.error("Error while decoding HTML from bytes due to " + str(e))
                    html_content = None

        except Exception as e:
            html_content = None
            logging.error("Error while decoding HTML from bytes due to " + str(e))

        return html_content

    @staticmethod
    def get_domain(url):
        parsed_url = urlparse(url)
        host = parsed_url.netloc.replace("www.", "")
        return host

    @staticmethod
    def get_time(time):
        # Example epoch timestamp - 1676048703245
        timestamp = arrow.get(time / 1000).format('YYYY-MM-DDTHH:mm:ss.SSSZ')
        return timestamp

    @staticmethod
    def get_text_title(soup):
        if soup:
            try:
                title = soup.title.string if soup.title else None
                text = soup.get_text()
            except Exception as e:
                text = ""
                title = ""
                logging.warning(e)
                logging.warning("Neither title or text")
            return text, title
        else:
            return None, None
