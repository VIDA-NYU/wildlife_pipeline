import logging
from typing import Dict, List, Optional

import datamart_geo
from io import BytesIO

import numpy as np
from numpy import asarray
import pandas as pd
from PIL import Image
import requests
from typing import Any
import re
import pickle
import uuid
import os
from bs4 import BeautifulSoup


import constants
import ftfy

geo_data = datamart_geo.GeoData.download(update=False)


class ProcessData:
    def __init__(self, bloom_filter, minio_client, bucket, task, column, model):
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
            elif os.path.exists("scrapers/"):
                scraper = pickle.load("scrapers/scraper_" + domain)
                self.domains[domain] = scraper
                logging.info(f"{domain} MLscraper loaded")
        return self.domains[domain]

    @staticmethod
    def remove_text(text: Optional[str]):
        if text:
            return any(phrase in text for phrase in constants.phrases_to_filter)
        return True

    def extract_information_from_docs(self, result: List[Dict]) -> pd.DataFrame:

        def log_processed(
                raw_count: int,
                processed_count: int) -> None:
            logging.info(f"{pd.Timestamp.now()}: received {raw_count} articles, total: "
                         f"{processed_count} unique processed")

        cache = []
        count = 0
        hits = len(result)
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
        df = pd.DataFrame()
        if count > 0:
            df = self.create_df(cache)
            if not df.empty:
                df["id"] = df.apply(lambda _: str(uuid.uuid4()), axis=1)
                df = self.get_location_info(df)
        return df

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

    def send_image(self, df: pd.DataFrame, image_folder: Optional[str], bucket_name: str, task: Optional[str],
                   timeout_sec: Optional[int] = 30):
        def send_image_to_minio(row):
            try:
                response = requests.get(row["image"], timeout=timeout_sec)
                img = Image.open(BytesIO(response.content))
                image_array = asarray(img)
                image_path = send(image_array, row["id"])
                image_path = bucket_name + "/" + image_path
                return image_path
            except Exception as e:
                print(f"image error: {e}")
                return None
        def send(image_array, img_id):
            pil_image = Image.fromarray(image_array)
            # Save the image to an in-memory file
            in_mem_file = BytesIO()
            pil_image.save(in_mem_file, format='png')
            in_mem_file.seek(0)
            length = len(in_mem_file.read())
            in_mem_file.seek(0)

            if image_folder:
                file_name = f"{image_folder}/{img_id}.png"
            else:
                file_name = f"{img_id}.png"

            self.minio_client.store_image(image=in_mem_file, file_name=file_name, length=length, bucket_name=bucket_name)
            return file_name

        df["image_path"] = df.apply(lambda x: send_image_to_minio(x), axis=1)

        return df

    @staticmethod
    def save_image_local(df, image_folder):
        def save_image(row):
            try:
                response = requests.get(row["image"], timeout=30)
                img = Image.open(BytesIO(response.content))
                image_path = os.path.join(image_folder, f"{row['id']}.png")
                img.save(image_path)  # Save the image locally
                return image_path
            except Exception as e:
                print(f"image error: {e}")
                return None
        df["image_path"] = df.apply(lambda x: save_image(x), axis=1)

        return df


    @staticmethod
    def get_location_info(df):
        def resolve_location(name):
            if name:
                parts = name.split(", ")  # Split the location string by comma
                for part in parts:
                    result = geo_data.resolve_name(part)  # Remove leading/trailing spaces and resolve each part
                    if result:
                        return result
            return None
        df["location"] = np.where(df["location"] == "None", None, df["location"])
        df["location"] = np.where(df["location"] == "US", "USA", df["location"])
        df["location"] = np.where(df["location"] == "GB", "Great Britain", df["location"])
        df["loc"] = df["location"].apply(lambda x: resolve_location(x) if x else None)
        df['loc_name'] = df["loc"].apply(lambda loc: loc.name if loc else None)
        df['lat'] = df["loc"].apply(lambda loc: loc.latitude if loc else None)
        df['lon'] = df["loc"].apply(lambda loc: loc.longitude if loc else None)
        df['country'] = df["loc"].apply(lambda loc: loc.get_parent_area(level=0).name if loc else None)
        df = df.drop(columns=["loc"])
        return df

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

    @staticmethod
    def create_dictionary_for_dataframe_extraction(ad):
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
        return dict_df

    @staticmethod
    def maybe_fix_text(x):
        # fixes Unicode that’s broken in various ways
        return ftfy.fix_text(x) if isinstance(x, str) else x

    def add_information_to_metadata(self, domain: str, metadata: dict, soup):
        if 'ebay' in domain:
            seller_username, location,  info, url = self.extract_seller_info_for_ebay(soup)
            item_description = self.extract_description_for_ebay(soup)
            metadata["seller"] = seller_username
            metadata["location"] = location
            metadata["seller_info"] = info
            metadata["seller_url"] = url
            if item_description:
                metadata["description"] = item_description

    @staticmethod
    def extract_description_for_ebay(soup):
        def clean_text(text):
            # Remove \n characters
            text = text.replace("\n", " ")

            # Remove \xa0 characters
            text = text.replace("\xa0", " ")

            text = text.replace("eBay", "")

            # Remove non-alphanumeric characters except spaces
            # text = re.sub(r'[^\w\s]', '', text)

            # Remove multiple spaces
            text = re.sub(r'\s+', ' ', text).strip()

            return text
        try:
            item_description_url = soup.find(id="desc_ifr")["src"]
            item_id = item_description_url[item_description_url.rindex('/')+1:]
            soup2 = BeautifulSoup(requests.get(item_description_url.format(item_id=item_id)).content, 'html.parser')

            item_description = soup2.get_text(strip=True, separator='\n')

            item_description = clean_text(item_description)

            return item_description
        except Exception:
            return None

    @staticmethod
    def extract_seller_info_for_ebay(soup):

        seller_info_div = soup.find('div', class_="d-stores-info-categories__container__info__section")

        if seller_info_div:
            # Extract the seller's username
            seller_username = seller_info_div.a.span.get_text(strip=True)
            print(f"Seller Username: {seller_username}")
            span_elements = seller_info_div.find_all('span', class_='ux-textspans ux-textspans--BOLD')

            seller_info = []
            for span in span_elements:
                seller_info.append(span.text)

            seller_url = re.search(r'(?<=href=")[^"]+', str(seller_info_div)).group()

        else:
            logging.error("Seller username not found.")
            seller_username = None
            seller_info = None
            seller_url = None

        shipping_location_element = soup.find('div', {
            'class': 'ux-labels-values col-12 ux-labels-values--itemLocation'})

        if shipping_location_element:
            # Extract the shipping location text
            shipping_location_text = shipping_location_element.get_text()
            shipping_location = shipping_location_text.split(':')[-1].strip()
        else:
            shipping_location = None


        return seller_username, shipping_location, seller_info, seller_url

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
