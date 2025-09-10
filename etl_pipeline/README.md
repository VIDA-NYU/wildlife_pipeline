## ACHE

The ACHE crawler was used to collect data from selected web page seeds as well as other customized paths found on the pages. To understand how ACHE works and the concept of link filters, please refer to the project's [documentation](http://ache.readthedocs.io/en/latest/).

The output data is a .deflate file. The data format and more information can also be found [here](https://ache.readthedocs.io/en/latest/data-formats.html#dataformat-files).

## Extract

This script is used to extract the data crawled by ACHE. The first step involves retrieving all the data stored on [Disk](https://github.com/VIDA-NYU/wildlife_pipeline/blob/main/main_disk.py) or [Elasticsearch](https://github.com/VIDA-NYU/wildlife_pipeline/blob/main/main_elasticsearch.py). The data coming from disk will be a .deflate file.



The data stored on Disk can be: Regular data folder when there is only one data collection provided by one crawler and Temporal data - Where we have a collection of folders for different dates.

To start the pipeline we need to have a S3-like storage to send the data - in your case we use a open source [Minio](https://hub.docker.com/r/minio/minio) storage.
To start a Minio app on docker
```docker run -p 9000:9000 -p 39889:39889 -v /data_minio:/data -e "MINIO_ROOT_USER=minioadmin" -e "MINIO_ROOT_PASSWORD=KMIkJhjA5yMeOPzD" minio/minio server /data --console-address :39889 ```


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


## Classification

### Zero-shot Classification
A Zero-Shot model is a machine learning model that can perform a task without being explicitly trained on examples specific to that task. It relies on pretrained language models that have learned general language representations from large datasets. We use a fine-tuned model of XLM-RoBERTa. The model xlm-roberta-large-xnli is fine-tuned on a combination of data in 15 languages, but can also be effective in other languages since RoBERTa was trained in 100 different languages.
We provide the model one hypothesis: `This product advertisement is about {}:`
Then, as input for the inference, we can use any text column from the dataset.

and the labels we try to predict is:

```
labels = ["a real animal",
          "a toy",
          "a print of an animal",
          "an object",
          "a faux animal",
          "an animal body part",
          "an faux animal body part"]
```
We select the final label from the maximum score between the labels.
`label_product and score_product`.

### Text Classification
The attritbutes `label and score` are releted to another model used to classify the ads in "animal product - 1" and "not an animal product - 0".
The model is not yet available and its on development phase.


