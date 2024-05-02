#!/bin/bash
cd ..
zip -r data_files.zip data data2 model scrapers test_2.csv config.yml
zip python_files.zip $(ls *.py | grep -v 'test_process_data.py')
venv-pack -o spark-env.tar.gz
cd ..
zip -r wildlife_pipeline.zip wildlife_pipeline -x "wildlife_pipeline/.git*" "wildlife_pipeline/data" "wildlife_pipeline/data2" "wildlife_pipeline/model*" "wildlife_pipeline/scrapers*"