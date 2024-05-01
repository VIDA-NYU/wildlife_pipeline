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


## Documentation
We follow the [PEP 257 - Docstring Convention for Python Docstrings](https://peps.python.org/pep-0257/) to allow for better readibility for functions.

We use the `databricks-koalas` package for the Spark migration, as it provides seemless integration of the Pandas API for Spark. As the version of Spark on Dataproc is 3.2, we cannot use the up-to-date `pyspark-pandas` package.

## Spark Installation Steps:
Note: Please create the virtual environment outside of the project folder.
- This also assumes that `pipenv` is installed. (run `pip install pipenv` if not)
1. Invoke `python -m venv spark-env` to create a virtual environment called `spark-env`.
2. Activate the virtual environment with the following command: `source spark-env/bin/activate`
- For Windows users, activate using the following command: `call call spark-env/Scripts/activate.bat`
- To deactivate the virtual environment on Windows, simply run `deactivate`
3. Install the dependencies using the Pipfile: `pipenv install`
4. Zip the virtual environment using `venv-pack`: `venv-pack -o spark-env.tar.gz`
- Windows currently not supported (use WSL as a quick fix).
5. Execute the `generate_archives_script` to allow the data files and python files to be visible by all executors:
```
cd scripts
./generate_archives.sh
```
- After making changes to the script, ensure it is an executable by running `chmod +x generate_archives.sh`
6. Upload the zip file to HDFS using the following steps:
- Compress the `wildlife_pipeline` folder using the following command:
`zip -r wildlife_pipeline.zip wildlife_pipeline -x "wildlife_pipeline/.git*" "wildlife_pipeline/data*" "wildlife_pipeline/model*" "wildlife_pipeline/scrapers*"`
- Upload the `wildlife_pipeline` folder to Greene:
`gsutil cp wildlife_pipeline.zip  gs://nyu-dataproc-hdfs-ingest`
- Run the following from within Dataproc to ingest the dataset into your HDFS home directory:
`hadoop distcp gs://nyu-dataproc-hdfs-ingest/wildlife_pipeline.zip /user/<your_net_id>_nyu_edu`
- Copy the files from the Hadoop cluster store in the HDFS directory to Dataproc:
`hdfs dfs -get wildlife_pipeline.zip`
- Unzip the `wildlife_pipeline.zip`:
`unzip wildlife_pipeline.zip`
7. Run the pipeline by using the following commands:
```
export PYSPARK_DRIVER_PYTHON=python # Do not set in cluster modes.
export PYSPARK_PYTHON=./environment/bin/python
spark-submit --archives spark-env.tar.gz#environment test_process_data.py --files data_files.zip --py-files python_files.zip
```

## Testing:
1. We add a small folder called `data2` which contains a subset of the .deflate files to test the functionality on a subset of the test files.

## Debugging Installation Errors:
1. `building wheel for pillow (pyproject.toml) did not run successfully`
this means that the versions of `pip`, `setuptools`, and `wheel` are likely the default Ubuntu 18 apt-managed versions, and require an update.
In the virtual environment, run the following command:
```
python -m pip install --upgrade pip setuptools wheel
```
2. `python setup.py bdist_wheel did not run successfully`
this occurs as there are some dependencies that contain native extensions that need to be compiled and linked correctly. These are likely written in C, C++ or some other languages. 
Thus the fix is the following:
```
pipenv install Cmake
```
- If the issue still persists, you are likely on a Mac with an M1 chip. The fix is to run the following commands:
```
pip uninstall pillow
brew install libjpeg
export LDFLAGS="..."
export CPPFLAGS="..."
pip install pillow
```
3. `building wheel for tokenizers (pyproject.toml) did not run successfully ... error: can't find Rust compiler`
install the Rust compiler with default settings:
`curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
## Point of Contact (PoC):
For any issues with running the pipeline in Spark, installation issues and code issues, please open an issue or send an email to: gl1589@nyu.edu