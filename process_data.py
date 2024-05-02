#!/usr/bin/env python3
# coding: utf-8

import logging
from typing import Dict, List, Optional

import os
from pyspark.sql import SparkSession

if os.environ["READ_FROM_ZIP"] == "True":
    # Initialize SparkSession (or SparkContext)
    spark = SparkSession.builder.getOrCreate()
    # Add a file to distribute to worker nodes
    spark.sparkContext.addFile("hdfs://nyu-dataproc-m:8020/user/gl1589_nyu_edu/data_files.zip")
    spark.sparkContext.addPyFile("hdfs://nyu-dataproc-m:8020/user/gl1589_nyu_edu/python_files.zip")
    
os.environ["DATA_FILES_ZIP_PATH"] = "hdfs://nyu-dataproc-m:8020/user/gl1589_nyu_edu/data_files.zip" if  os.environ["READ_FROM_ZIP"] == "True" else "NOT FOUND"
os.environ["PYTHON_FILES_ZIP_PATH"] = "hdfs://nyu-dataproc-m:8020/user/gl1589_nyu_edu/python_files.zip" if os.environ["READ_FROM_ZIP"] == "True" else "NOT FOUND"

import datamart_geo
from io import BytesIO

from bs4 import BeautifulSoup
from mlscraper import Page
import numpy as np
from numpy import asarray
import pandas as pd
from PIL import Image
import requests
from typing import Any
import re
import pickle
import uuid
import shutil
import tempfile
import zipfile

# Spark-related import statements:
from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StringType, FloatType, StructField
# from pyspark.sql.functions import udf, when, col
# import pyspark.pandas as ppd # Only supported in Spark v3.2 and above
import databricks.koalas as ks

from create_metadata import (
    open_scrap,
    get_sintax_opengraph,
    get_sintax_dublincore,
    get_dict_json_ld,
    get_dict_microdata
)
from clf_job import CLFJob
import extruct
import constants
import ftfy

geo_data = datamart_geo.GeoData.download(update=False)

'''
# UDF Helper Functions:
def generate_uuid():
    return str(uuid.uuid4())

def resolve_location(name):
    if name:
        parts = name.split(", ")  # Split the location string by comma
        for part in parts:
            result = geo_data.resolve_name(part)  # Remove leading/trailing spaces and resolve each part
            if result:
                return result
    return None

# UDF register statements:
uuid_udf = udf(generate_uuid, StringType)
resolve_location_udf = udf(resolve_location, StringType())
'''


class ProcessData:
    def __init__(self, bloom_filter, minio_client, bucket, task, column, model):
        '''
        self.spark = SparkSession.builder\
            .config("spark.executor.instances", "10") \
            .config("spark.executor.cores", "4") \
            .config("spark.executor.memory", "4g") \
            .config("spark.dynamicAllocation.enabled", "true") \
            .config("spark.shuffle.service.enabled", "true").getOrCreate()
        '''
        self.spark = SparkSession.builder.getOrCreate() # Default settings
        ks.set_option('compute.default_index_type', 'distributed')  # Set index type to distributed for large datasets
        # ppd.enable_pandas_on_spark_session(self.spark) # Enable pyspark.pandas support
        # ppd.set_option("compute.default_index_type", "distributed")  # Using a distributed index for scalability
        self.minio_client = minio_client
        self.bloom_filter = bloom_filter
        self.domains = {}
        self.bucket = bucket
        self.task = task
        self.column = column
        self.model = model

    def open_scrap(self, minio_client: Any, domain: str):
        if domain not in self.domains.keys():
            if self.minio_client:
                obj = minio_client.get_obj("scrapers", "scraper_" + domain)
                scraper = pickle.load(obj)
                self.domains[domain] = scraper
                logging.info(f"{domain} MLscraper loaded")
            elif os.environ["READ_FROM_ZIP"] == "True":
                with zipfile.ZipFile(os.environ["DATA_FILES_ZIP_PATH"], 'r') as zip_ref:
                    if 'scrapers/' in zip_ref.namelist():
                        with zip_ref.open('scrapers/scraper_' + domain, 'r') as pickle_file:
                            pickle_content = pickle_file.read()
                            scraper = pickle.loads(pickle_content)
                            self.domains[domain] = scraper
                            logging.info(f"{domain} MLscraper loaded")
            else: # os.environ["READ_FROM_ZIP"] == "False"
                if os.path.exists("scrapers/"):
                    scraper = pickle.load("scrapers/scraper_" + domain)
                    self.domains[domain] = scraper
                    logging.info(f"{domain} MLscraper loaded")
        return self.domains[domain]

    @staticmethod
    def remove_text(text: Optional[str]):
        if text:
            return any(phrase in text for phrase in constants.phrases_to_filter)
        return True

    def extract_information_from_docs(self, result: List[Dict]) -> ks.DataFrame:

        def log_processed(
                raw_count: int,
                processed_count: int) -> None:
            logging.info(f"{pd.Timestamp.now()}: received {raw_count} articles, total: "
                         f"{processed_count} unique processed")

        cache = []
        count = 0
        hits = len(result)
        # print(hits)
        for val in result:
            # print(val)
            processed = val.get("_source")
            if processed:
                if not ProcessData.remove_text(processed["text"]) and not self.bloom_filter.check_bloom_filter(
                        processed["text"]):
                    count += 1
                    cache.append(processed)
            elif val["content"]:
                count += 1
                cache.append(val)
        log_processed(hits, count)
        df = ks.DataFrame()
        if count > 0:
            """ 
            Using rdds:
            sc = self.spark.sparkContext
            rdd = sc.parallelize(cache)
            df = self.create_df(rdd)
            if not df.rdd.isEmpty():
                df = df.withColumn("id", uuid_udf())
                df = self.get_location_info(df)
            """
            df = ks.create_df(cache)
            if not df.empty:
                df["id"] = df.apply(lambda _: str(uuid.uuid4()))
                df = self.get_location_info(df)
        return df

    def create_df(self, ads):
        final_dict = []
        for ad in ads:
            dict_df = self.create_dictionary_for_dataframe_extraction(ad)
            final_dict.append(dict_df)
            domain = ad["domain"].split(".")[0]
            if "ebay" in domain:
                extract_dict = dict_df.copy()
                self.add_seller_information_to_metadata(ad["html"], domain, extract_dict, ad["content_type"])
                final_dict.append(extract_dict)
            if self.minio_client and domain in constants.DOMAIN_SCRAPERS:
                try:
                    extract_dict = dict_df.copy()
                    scraper = open_scrap(self.minio_client, domain)
                    extract_dict.update(scraper.get(Page(ad["html"])))
                    if extract_dict.get("product"):
                        extract_dict["name"] = extract_dict.pop("product")
                    final_dict.append(extract_dict)
                except Exception as e:
                    logging.error(f"MLscraper error: {e}")
            
            # Handling metadata extraction and processing
            try:
                metadata = extruct.extract(ad["html"],
                                           base_url=ad["url"],
                                           uniform=True,
                                           syntaxes=['json-ld', 'microdata', 'opengraph', 'dublincore'])
                if metadata:
                    self.process_metadata(metadata, dict_df, final_dict)
            except Exception as e:
                logging.error(f"Exception on extruct: {e}")

        # Convert final_dict to a Koalas DataFrame
        kdf_metas = ks.DataFrame(final_dict)
        
        # Applying transformations using Koalas operations
        kdf_metas["price"] = kdf_metas["price"].apply(lambda x: self.fix_price_str(x), dtype='float')
        kdf_metas["currency"] = kdf_metas["currency"].apply(lambda x: self.fix_currency(x))

        # Grouping and aggregation in Koalas
        kdf_metas = kdf_metas.groupby('url').agg({
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
            "ships_to": 'first'
        }).reset_index()

        # Perform any necessary data type conversions or text fixing
        kdf_metas = self.assert_types(kdf_metas)
        columns_to_fix = ["title", "text", "name", "description"]
        kdf_metas[columns_to_fix] = kdf_metas[columns_to_fix].apply(lambda x: self.maybe_fix_text(x), dtype='str')

        print("kdf_metas created")
        return kdf_metas

    @staticmethod
    def fix_price_str(price):
        if isinstance(price, tuple):
            price = price[1]

        if isinstance(price, str):
            # Remove non-digit characters from the string, except commas and periods
            cleaned_price = re.sub(r"[^\d,.]", "", price)
            cleaned_price = cleaned_price.replace(",", "")
            if cleaned_price:
                extracted_value = float(cleaned_price.replace(",", ""))
                # Check if the extracted value has no fractional part
                if extracted_value.is_integer():
                    extracted_value = int(extracted_value)

                return extracted_value
            return None

        elif isinstance(price, list):
            extracted_prices = []
            for item in price:
                # Remove non-digit characters from each element in the list, except commas and periods
                try:
                    cleaned_item = re.sub(r"[^\d,.]", "", item)
                    cleaned_item = cleaned_item.replace(",", "")
                    if cleaned_item:
                        extracted_value = float(cleaned_item.replace(",", ""))
                        # Check if the extracted value has no fractional part
                        if extracted_value.is_integer():
                            extracted_value = int(extracted_value)

                        extracted_prices.append(extracted_value)
                except TypeError:
                    continue
            if extracted_prices:
                return sum(extracted_prices) / len(extracted_prices)
            else:
                return None
        return price

    @staticmethod
    def fix_currency(x):
        if x is not None and isinstance(x, list):
            x = ", ".join(x)
        return x

    def send_image(self, df: ks.DataFrame, image_folder: Optional[str], bucket_name: str, task: Optional[str],
                   timeout_sec: Optional[int] = 30):
        def send_image_to_minio(image_url, img_id):
            try:
                response = requests.get(image_url, timeout=timeout_sec)
                img = Image.open(BytesIO(response.content))
                image_array = np.asarray(img)
                return save_image_to_minio(image_array, img_id)
            except Exception as e:
                print(f"image error: {e}")
                return None

        def save_image_to_minio(image_array, img_id):
            pil_image = Image.fromarray(image_array)
            in_mem_file = BytesIO()
            pil_image.save(in_mem_file, format='png')
            in_mem_file.seek(0)
            length = len(in_mem_file.read())
            in_mem_file.seek(0)
            file_name = f"{image_folder}/{img_id}.png" if image_folder else f"{img_id}.png"
            self.minio_client.store_image(image=in_mem_file, file_name=file_name, length=length, bucket_name=bucket_name)
            return bucket_name + "/" + file_name

        # Pandas UDFs require conversion back to Koalas, so handle this carefully:
        if isinstance(df, ks.DataFrame):
            # Converting to pandas for the image processing part since it involves I/O operations
            pdf = df.to_pandas()
            pdf['image_path'] = [send_image_to_minio(row['image'], row['id']) for index, row in pdf.iterrows()]
            result_df = ks.from_pandas(pdf)
        else:
            # This block is for safety, in case df is already a pandas DataFrame
            df['image_path'] = [send_image_to_minio(row['image'], row['id']) for index, row in df.iterrows()]
            result_df = df

        return result_df

    @staticmethod
    def save_image_local(df, image_folder):
        def save_image(image_url, img_id):
            try:
                response = requests.get(image_url, timeout=30)
                img = Image.open(BytesIO(response.content))
                image_path = os.path.join(image_folder, f"{img_id}.png")
                img.save(image_path)
                return image_path
            except Exception as e:
                print(f"image error: {e}")
                return None

        # Similar approach as send_image, handle df conversion if necessary
        if isinstance(df, ks.DataFrame):
            pdf = df.to_pandas()
            pdf['image_path'] = [save_image(row['image'], row['id']) for index, row in pdf.iterrows()]
            result_df = ks.from_pandas(pdf)
        else:
            df['image_path'] = [save_image(row['image'], row['id']) for index, row in df.iterrows()]
            result_df = df

        return result_df


    @staticmethod
    def get_location_info(df):
        """
        Using pyspark.pandas:
        def resolve_location(name):
            if name:
                parts = name.split(", ")  # Split the location string by comma
                for part in parts:
                    result = geo_data.resolve_name(part)  # Remove leading/trailing spaces and resolve each part
                    if result:
                        return result
            return None
        # With pyspark.pandas
        df["location"] = ppd.when(df["location"] == "None", None).otherwise(df["location"])
        df["location"] = ppd.when(df["location"] == "US", "USA").otherwise(df["location"])
        df["location"] = ppd.when(df["location"] == "GB", "Great Britain").otherwise(df["location"])
        df["loc"] = df["location"].apply(lambda x: resolve_location(x) if x else None)
        df['loc_name'] = df["loc"].apply(lambda loc: loc.name if loc else None)
        df['lat'] = df["loc"].apply(lambda loc: loc.latitude if loc else None)
        df['lon'] = df["loc"].apply(lambda loc: loc.longitude if loc else None)
        df['country'] = df["loc"].apply(lambda loc: loc.get_parent_area(level=0).name if loc else None)
        df = df.drop(columns=["loc"])
        return df
        """
        
        def resolve_location(name):
            if name:
                parts = name.split(", ")  # Split the location string by comma
                for part in parts:
                    result = geo_data.resolve_name(part.strip())  # Attempt to resolve each part
                    if result:
                        return result
            return None

        # Koalas does not support `when` directly, so use `apply` for conditional changes
        df["location"] = df["location"].apply(lambda x: None if x == "None" else x)
        df["location"] = df["location"].apply(lambda x: "USA" if x == "US" else x)
        df["location"] = df["location"].apply(lambda x: "Great Britain" if x == "GB" else x)

        # Resolve locations
        df["loc"] = df["location"].apply(lambda x: resolve_location(x) if x else None)
        
        # Extract information from resolved locations
        df['loc_name'] = df["loc"].apply(lambda loc: loc.name if loc else None)
        df['lat'] = df["loc"].apply(lambda loc: loc.latitude if loc else None)
        df['lon'] = df["loc"].apply(lambda loc: loc.longitude if loc else None)
        df['country'] = df["loc"].apply(lambda loc: loc.get_parent_area(level=0).name if loc else None)
        
        # Drop the temporary 'loc' column
        df = df.drop(columns=["loc"])

        return df
    
        """
        With pyspark.sql
        df = df.withColumn("location",  when(col("location") == "None", None)
                                        .when(col("location") == "US", "USA")
                                        .when(col("location") == "GB", "Great Britain")
                                        .otherwise(col("location")))
        df = df.withColumn("loc",   when(col("location").isNull(), None)
                                    .otherwise(resolve_location_udf(col("location"))))
        df = df.withColumn("loc_name",  when(col("loc").isNull(), None)
                                        .otherwise(col("loc").getField("name")))
        df = df.withColumn("lat",   when(col("loc").isNull(), None)
                                    .otherwise(col("loc").getField("latitude")))
        df = df.withColumn("lon",   when(col("loc").isNull(), None)
                                    .otherwise(col("loc").getField("longitude")))
        df = df.withColumn("country", when(col("loc").isNull(), None).otherwise(col("loc").getField("get_parent_area").getItem(0).getField("name")))
        df = df.drop("loc")
        return df
        """
        
    @staticmethod
    def assert_types(df):
        expected_dtypes = {
            "title": str,
            "text": str,
            "domain": str,
            "name": str,
            "description": str,
            "image": str,
            "retrieved": str,
            "production_data": str,
            "category": str,
            "price": float,
            "currency": str,
            "seller": str,
            "seller_type": str,
            "seller_url": str,
            "location": str,
            "ships to": str,
        }

        # Convert each column to the expected data type
        for column, expected_dtype in expected_dtypes.items():
            if expected_dtype == str:
                df[column] = df[column].astype(expected_dtype)
                try:
                    df[column] = df[column].apply(
                        lambda x: x.encode('utf-8', 'surrogateescape').decode('iso-8859-15') if isinstance(x,
                                                                                                           str) else x)
                except UnicodeEncodeError as e:
                    print(column)
                    print(e)
                    df[column] = ""
            else:
                df[column] = df[column].astype(expected_dtype)
        return df

    def run_classification(self, df: ks.DataFrame, bucket_name: Optional[str]) -> ks.DataFrame:
        logging.info(self.task)
        classifier_job = CLFJob(bucket=self.bucket, final_bucket=self.bucket, minio_client=self.minio_client,
                                date_folder=None, task=self.task, model=self.model, column=self.column)
        if self.task == "zero-shot-classification":
            df = classifier_job.get_inference(df)
        elif self.task == "text-classification":
            df = classifier_job.get_inference_for_column(df)
        elif self.task == "multi-model":
            if "image_path" not in df.columns:
                folder_path= "./image_data"
                os.makedirs(folder_path, exist_ok=True)
                temp_dir = tempfile.mkdtemp(dir=folder_path)
                # Assuming save_image_local converts Koalas DF to pandas DF for image processing
                pdf = df.to_pandas()  # Converting to pandas DF for image processing
                pdf = ProcessData.save_image_local(pdf, temp_dir)
                df = ks.from_pandas(pdf)  # Convert back to Koalas DF
                df = classifier_job.get_inference_for_mmm(df, bucket_name)
                shutil.rmtree(temp_dir)
            else:
                df = classifier_job.get_inference_for_mmm(df, bucket_name)
        elif self.task == "both":
            df = classifier_job.get_inference(df)
            df = classifier_job.get_inference_for_column(df)
        else:
            raise ValueError("Task is either text-classification, zero-shot-classification, multi-model, or both")
        return df

    @staticmethod
    def create_dictionary_for_dataframe_extraction(self, ad):
        dict_df = {
            "url": ad["url"],
            "title": ad["title"],
            "text": ad["text"],
            "domain": ad["domain"],
            "retrieved": ad["retrieved"],
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
        sdf = ks.DataFrame(dict_df)
        return sdf

    @staticmethod
    def maybe_fix_text(x):
        # fixes Unicode that’s broken in various ways
        return ftfy.fix_text(x) if isinstance(x, str) else x

    #self.add_seller_information_to_metadata(ad["html"], domain, extract_dict, ad["content_type"])    
    def add_seller_information_to_metadata(self, domain, metadata, soup):
        if 'ebay' in domain:
            seller_username, location = self.extract_seller_info_for_ebay(soup)
            
            # In Koalas, use the `assign` method to add new columns or modify existing ones
            # This method is akin to the pandas' way and is generally used for Koalas DataFrames
            metadata = metadata.assign(seller=seller_username, location=location)

        return metadata

    @staticmethod
    def extract_seller_info_for_ebay(soup):

        # Parse the HTML content
        # soup = BeautifulSoup(page_html, 'html.parser')
        # Find the div element with the specified class
        seller_info_div = soup.find('div', class_='ux-seller-section__item--seller')

        if seller_info_div:
            # Extract the seller's username
            seller_username = seller_info_div.a.span.get_text(strip=True)
            # print(f"Seller Username: {seller_username}")
        else:
            logging.error("Seller username not found.")
            seller_username = ""

        shipping_location_element = soup.find('div', {
            'class': 'ux-labels-values col-12 ux-labels-values--itemLocation'})

        if shipping_location_element:
            # Extract the shipping location text
            shipping_location_text = shipping_location_element.get_text()
            shipping_location = shipping_location_text.split(':')[-1].strip()
            return seller_username, shipping_location
        else:
            logging.error("Shipping location not found.")
            return seller_username, ""


    @staticmethod
    def get_parser(content_type):
        if 'text/xml' in content_type:
            parser = 'xml'
        elif 'text/html' in content_type:
            parser = 'html.parser'
        elif 'x-asp' in content_type or 'xhtml+xml' in content_type:
            parser = 'lxml'
        elif 'text/plain' in content_type:
            parser = 'plain'
        else:
            return None

    """
    def get_schema():
        schema = StructType([
            StructField("url", StringType()),
            StructField("title", StringType()),
            StructField("text", StringType()),
            StructField("domain", StringType()),
            StructField("retrieved", StringType()),
            StructField("name", StringType()),
            StructField("description", StringType()),
            StructField("image", StringType()),
            StructField("production_data", StringType()),
            StructField("category", StringType()),
            StructField("price", FloatType()),
            StructField("currency", StringType()),
            StructField("seller", StringType()),
            StructField("seller_type", StringType()),
            StructField("seller_url", StringType()),
            StructField("location", StringType()),
            StructField("ships_to", StringType())
        ])
        return schema
    """    
    