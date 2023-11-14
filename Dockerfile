FROM python:3.8 AS geo-data

RUN mkdir /usr/src/app
RUN apt-get dist-upgrade && \
    apt-get install -y --no-install-recommends git
RUN git clone https://gitlab.com/ViDA-NYU/auctus/datamart-geo.git /usr/src/app/lib_geo
RUN pip --disable-pip-version-check --no-cache-dir install /usr/src/app/lib_geo
ENV DATAMART_GEO_DATA /usr/src/app/geo_data
RUN python -m datamart_geo --update /usr/src/app/geo_data && \
    ls -l /usr/src/app/geo_data

# Specify the parent image from which we build
FROM python:3.9 as data_dump
COPY --from=geo-data /usr/src/app/geo_data /usr/src/app/geo_data

# Set the working directory
WORKDIR /app

# Copy files from your host to your current working directory
COPY requirements.txt requirements.txt

# Build the application with cmake
RUN apt-get dist-upgrade && \
    apt-get install -y git && \
    pip --no-cache-dir install --upgrade pip && \
    pip --no-cache-dir install install -r requirements.txt && \
    apt-get remove -y git && \
    rm -rf /var/lib/apt/lists/*

# Install huggingface-cli
RUN pip install --upgrade huggingface-hub && pip install git-lfs

# Login to Hugging Face Hub (you'll need to provide your API key)
# ARG HUGGINGFACE_TOKEN
# RUN huggingface-cli login --token $HUGGINGFACE_TOKEN --add-to-git-credential


ENV PYTHONFAULTHANDLER=1
ENV DATAMART_GEO_DATA /usr/src/app/geo_data

COPY . .

ENTRYPOINT [ "python3" ]

