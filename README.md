# ACHE

The ACHE crawler was used to collect data from selected [web page seeds](https://drive.google.com/file/d/17EU4HyCy5D88yCa1FGo8EhU4C137chgI/view?usp=drive_link) as well as other customized paths found on the pages. This was achieved by configuring [link filters](https://drive.google.com/file/d/1SfqHUoKgjARxSr_bSkw5jSFTpKLOn3_G/view?usp=drive_link) using wildcard patterns. To understand how ACHE works and the concept of link filters, please refer to the project's [documentation](http://ache.readthedocs.io/en/latest/).

Initially, the data is downloaded and stored in an ElasticSearch index. You can utilize ElasticSearch to search the data and obtain information about the running crawler. More information about all wildlife-crawlers can be found [here](https://docs.google.com/spreadsheets/d/1K7TuQbxaEz_IOqPW5oxDst6EM_GNQjUPO7DQQoQ0cn4/edit#gid=0).

A sample of the output data is founded [here](https://github.com/VIDA-NYU/DISN-Wildlife-Internal/blob/main/data/labeled_ads.csv)

## Extract

This script is used to extract the data crawled by ACHE. The first step involves retrieving all the data stored in ElasticSearch for a specified date range. We exclude certain pages, such as those without any text or pages that have identical content but different URLs. All data is saved, and we check whether it has already been indexed using a [Bloom Filter](https://github.com/VIDA-NYU/DISN-Wildlife-Internal/blob/ETL_s3/ETL_pipeline/bloom_filter.py).

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

With the location column, we leverage [datamart_geo](https://gitlab.com/ViDA-NYU/auctus/datamart-geo) to extract the latitude, longitude, and country name associated with each entry. Furthermore, the image column is used to download the corresponding product images using their respective URLs.

## Load

Finally, in the load step, we store the processed dataframe and images in a [MinIO](https://miniodisn.hsrn.nyu.edu/browser) bucket. Each bucket is named according to the month from which the data was retrieved by the crawler.

For more details, please refer to the [following link](https://docs.google.com/presentation/d/14Azy2_fdiq7it8s9sV5XxxfC7x8Hwdip49D_Q8SQshA/edit#slide=id.p).


## Zero-shot Classification

With the information collected in the previous steps we select two columns to perform Zero-shot classification task: PRODUCT (title column or name column in case title is not present) and DESCRIPTION. A zero-shot model is a machine learning model that can perform a task without being explicitly trained on examples specific to that task. It relies on pretrained language models that have learned general language representations from large datasets. The model used right now is the [facebook/bart-large-mnli](https://huggingface.co/facebook/bart-large-mnli).

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

The final dataframe will contain the labels with the highest score and the score for each of the two columns used for classification (product and description). After that we select the final label from the maximum score between the two predictions.

In a separted bucket [zeros-shot-{month}] the results os the zero-shot clf is storaged. You can find the exactly parquet file name of the original dataframe. 