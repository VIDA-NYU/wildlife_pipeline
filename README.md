# Wildlife Pipeline

This repository contains three projects for wildlife data processing and analysis:

## Projects

- **Features:**
    - Uses the [ACHE crawler](https://github.com/ViDA-NYU/ache) for web data collection
    - Configurable crawling parameters
    - Outputs raw data for ETL processing

### 1. `ETL`
Handles the preprocessing (Extract, Transform, Load) of raw wildlife data from various sources collected using ACHE crawler.

- **Features:**
    - Data fetching from APIs and local files
    - Data cleaning and normalization
    - Output in standardized formats

### 2. `Automated Classifier Generator (LTS)`
This module implements the Learn to Sample (LTS) - automated classifier generation pipeline for wildlife trafficking detection.

- **Features:**
    - Implements cost-effective sampling and labeling strategies using LLMs (see [LTS](https://github.com/VIDA-NYU/LTS))
    - Fine-tuning and evaluation of text classifiers (BERT, GPT, LLaMA, etc.)
    - Topic modeling and clustering for data triage
    - Integration with validation and use-case datasets
    - Reproducible experiments for classifier training and evaluation

**See the [`LTS`](https://github.com/VIDA-NYU/LTS) folder for code, documentation, and use-case data.**

### 3. `HILTS`
A web-based labeling and search platform for wildlife data, built with Svelte, TypeScript, and Python.
Located in the [`hilts`](https://github.com/VIDA-NYU/HILTS) folder.

- **Features:**
    - Interactive CSV data loading and labeling interface
    - Keyword, random, and image-based search for wildlife datasets
    - Label management and export functionality
    - Integrated backend API for data processing and model inference
    - Dockerized deployment and Poetry-based Python environment
    - Example client built with Svelte + Vite

**See the [`hilts`](https://github.com/VIDA-NYU/HILTS) folder for code, frontend, backend, and deployment instructions.**

## Getting Started

1. Clone the repository:
     ```bash
     git clone https://github.com/yourusername/wildlife_pipeline.git
     ```
2. Install dependencies for each project (see individual project folders for instructions).
3. Follow the usage guides in each project's README for details.
