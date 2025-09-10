# Wildlife ETL Pipeline
## Table of Contents

- [Overview](#wildlife-etl-pipeline)
- [ACHE Crawler](#ache-crawler)
- [Extract](#extract)
- [Transform](#transform)
- [Load](#load)
- [Text Classification](#text-classification)
- [How to Run the Pipeline](#how-to-run-the-pipeline)
      - [1. Prepare Your Environment](#1-prepare-your-environment)
      - [2. Set MinIO Credentials](#2-set-minio-credentials)
      - [3. Run the Pipeline](#3-run-the-pipeline)
      - [4. Output](#4-output)
      - [5. Command-line Arguments](#5-command-line-arguments)
      - [6. Example Usage](#6-example-usage)

---

## How to Run the Pipeline

### 1. Prepare Your Environment

Install dependencies:
```bash
pip install -r requirements.txt
```

### 2. Set MinIO Credentials

Set your MinIO credentials as environment variables:
```bash
export MINIO_KEY=<your-minio-access-key>
export MINIO_SECRET=<your-minio-secret-key>
```

### 3. Run the Pipeline

Execute the pipeline:
```bash
python main_disk.py \
      -finalbucket <minio-bucket-name> \
      -filename <crawl-folder-name> \
      -bloom <bloom-filter-file> \
      -image <True/False> \
      -date <date-string> \
      -temporal <True/False>
```
- For temporal analysis, set `-temporal True` and provide the appropriate folder structure.

### 4. Output

- Processed CSV files and images are saved to your MinIO bucket or local disk, depending on configuration.
- Logs indicate progress and errors.

**Tip:**

Customize the pipeline by editing arguments in `main_disk.py` or modifying the `ETLDiskJob` class for advanced workflows.

### 5. Command-line Arguments

| Argument         | Type    | Required | Description                                                                 |
|------------------|---------|----------|-----------------------------------------------------------------------------|
| `-finalbucket`   | str     | Yes      | MinIO bucket to store processed data (e.g., `clf`, `elt`)                   |
| `-model`         | str     | No       | Model name on Hugging Face for classification tasks                         |
| `-bloom`         | str     | No       | MinIO bloom filter file name for deduplication                              |
| `-task`          | str     | No       | Task to perform: `text-classification`, `zero-shot-classification`, `both`, `multi-model` |
| `-image`         | bool    | No       | Whether to download images (`True` or `False`)                              |
| `-filename`      | str     | No       | Filename on disk. For temporal tasks, this is the crawler ID                |
| `-date`          | str     | No       | Date string for the image bucket                                            |
| `-temporal`      | bool    | No       | Run pipeline in temporal analysis mode (`True` or `False`)                  |

### 6. Example Usage

```bash
python main_disk.py \
      -finalbucket wildlife-data \
      -filename crawl_data-1699796707793-0 \
      -model xlm-roberta-large-xnli \
      -task True \
      -image True \
      -date 2024-09-10 \
      -bloom bloom_filter \
      -temporal False
```
This repository provides a complete pipeline for collecting, extracting, transforming, classifying, and storing wildlife-related data from online sources. The pipeline is modular and supports scalable deployment using MinIO, Docker, and Kubernetes.

---

## ACHE Crawler

We use the [ACHE crawler](http://ache.readthedocs.io/en/latest/) to collect data from selected web page seeds and customized paths. ACHE supports configurable link filters and outputs data in `.deflate` format.
See [ACHE data format documentation](https://ache.readthedocs.io/en/latest/data-formats.html#dataformat-files) for details.

---

## Extract

Extraction scripts retrieve crawled data from disk (`.deflate` files) or Elasticsearch.
- Disk data can be organized as a single folder or as temporal collections (multiple folders by date).
- Data is deduplicated using a [Bloom Filter](https://github.com/VIDA-NYU/wildlife_pipeline/blob/main/bloom_filter.py) stored in MinIO.

**MinIO Setup:**
To start a local MinIO server for storage:
```bash
docker run -p 9000:9000 -p 39889:39889 -v /data_minio:/data \
  -e "MINIO_ROOT_USER=minioadmin" -e "MINIO_ROOT_PASSWORD=KMIkJhjA5yMeOPzD" \
  minio/minio server /data --console-address :39889
```

We exclude certain pages, such as those without any text or pages that have identical content but different URLs. All data is saved, and we check whether it has already been indexed using a [Bloom Filter](https://github.com/VIDA-NYU/wildlife_pipeline/blob/main/bloom_filter.py).
Make sure to have a bucket for bloom filter at your Minio.


## Transform

The next step is to process the data. We employ two different methods to extract information about the listed products. The first method utilizes [MLscraper](https://github.com/lorey/mlscraper). For this approach, we have a dedicated model for each of the following domains:
```
DOMAIN_SCRAPERS = ['gumtree', 'auctionzip', '1stdibs', 'canadianlisted', 'stsy', 'ukclassifieds', 'taxidermy', 'skullsunlimited', 'glacierwear', 'picclick', 'preloved', 'ebay']
```
These models extract details such as the product name, description, price, and seller information. However, please note that this model may not achieve 100% accuracy for all pages. Therefore, we employ a second layer of extraction.

With the help of [extruct](https://github.com/scrapinghub/extruct), we are able to extract metadata using four different syntaxes:
```
metadata = extruct.extract(
                ad["html"],
                base_url=ad["url"],
                uniform=True,
                syntaxes=['json-ld', 'microdata', 'opengraph', 'dublincore'])
```
The resulting dataframe, obtained by combining these two methods, should include the following columns:
```
['url', 'title', 'text', 'domain', 'name', 'description', 'image', 'retrieved', 'production_data', 'category', 'price', 'currency', 'seller', 'seller_type', 'seller_url', 'location', 'ships to']
```
Please be aware that it may not always be possible to collect every piece of information for every page, resulting in some NaN values in the dataframe.


| Field               | Description                                    |
|---------------------|------------------------------------------------|
| url                 | The ad URL                                      |
| title               | Product Advisement title                       |
| text                | The whole html page text |
| product             | Name of the product - can be same as title |
| description         | Description of the product - can be same as title|
| domain              | Website where the product is posted             |
| image               | URL of the image                                |
| retrieved           | Time when the page was downloaded               |
| category            | The category listed for that product            |
| production date     | Production date of the product                  |
| price               | Price of the product                            |
| currency            | Currency of the price                           |
| seller              | Seller name                                    |
| seller_type         | The category the seller is listed               |
| location            | Location of product                             |
| label_product     | Zero shot classifier result label                  |
| score_product      | Zero shot label probability                    |
| label               | text classification model result (1 - animal product 0 -  Not a animal product) |
| score              | text classification model probablity for result|
| id                  | UUID used as filename for images               |

In the final data processed by the pipeline the columns available are:
``` ['url', 'title', 'text', 'domain', 'name', 'description', 'image',
       'retrieved', 'production_data', 'category', 'price', 'currency',
       'seller', 'seller_type', 'seller_url', 'location', 'ships to', 'id',
       'loc_name', 'lat', 'lon', 'country', 'product', 'label_product',
       'score_product', 'label', 'score']
 ```
Some of those collumns are working in progress, since the results are not always consistent.


With the location column, we leverage [datamart_geo](https://gitlab.com/ViDA-NYU/auctus/datamart-geo) to extract the latitude, longitude, and country name associated with each entry. Furthermore, the image column is used to download the corresponding product images using their respective URLs.

## Load

Finally, in the load step, we store the processed dataframe and images in a [MinIO](https://miniodisn.hsrn.nyu.edu/browser) bucket.


### Text Classification
The attritbutes `label and score` are releted to another model used to classify the ads in "animal product - 1" and "not an animal product - 0".
The model is not yet available and its on development phase.

## How to Run the Pipeline

### 1. Prepare Your Environment

- Install dependencies:
  ```bash
  pip install -r requirements.txt
  ```

export MINIO_KEY=<your-minio-access-key>
export MINIO_SECRET=<your-minio-secret-key>
### 2. Set MinIO Credentials

Set your MinIO credentials as environment variables:
```bash
export MINIO_KEY=<your-minio-access-key>
export MINIO_SECRET=<your-minio-secret-key>
```

### 3. Run the Pipeline

Execute the pipeline using the following command:
```bash
python main_disk.py -finalbucket <minio-bucket-name> \
      -filename <crawl-folder-name> \
      -bloom <bloom filter name for this data> \
      -image <True/False> \
      -task <text-classification> \
      -date <date-string> \
      -bloom <bloom-filter-file> \
```
- For temporal analysis, set `-temporal True` and provide the appropriate folder structure.

### 4. Output

- Processed CSV files and images will be saved to your MinIO bucket or local disk, depending on your configuration.
- Logs will indicate progress and any errors encountered.

**Tip:**
You can customize the pipeline by editing arguments in `main_disk.py` or by modifying the `ETLDiskJob` class for advanced workflows.

### Command-line Arguments

The ETL pipeline can be customized using the following command-line arguments:

- `-finalbucket` (str, required):
  The MinIO bucket to store processed data (e.g., `clf` or `elt`).

- `-model` (str, optional):
  Model name on Hugging Face for classification tasks.

- `-bloom` (str, optional):
  The MinIO bloom filter file name for deduplication.

- `-task` (str, optional):
  Task to perform. Choices: `text-classification`, `zero-shot-classification`.

- `-image` (bool, optional):
  Whether to download images (`True` or `False`).

- `-filename` (str, optional):
  Filename on disk or crawler ID for the crawled data repository

- `-date` (str, optional):
  The date string for the image bucket.

- `-temporal` (bool, optional):
  Whether to run the pipeline in temporal analysis mode (`True` or `False`).

**Example usage:**
```bash
python main_disk.py -finalbucket wildlife-data \
    -model vida-nyu/animal-product-detector \
    -task text-classification \
    -image True \
    -date April \
    -filename crawler_animal \
    -bloom animal_filter \
```

