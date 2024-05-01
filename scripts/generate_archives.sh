#!/bin/bash
cd ..
zip -r data_files.zip data data2 model scrapers test_2.csv config.yml
zip python_files.zip $(ls *.py | grep -v 'test_process_data.py')