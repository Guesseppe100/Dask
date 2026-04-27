FROM jupyter/scipy-notebook:python-3.11

USER root

RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

USER ${NB_UID}

RUN pip install --no-cache-dir \
    "dask[distributed]==2024.5.1" \
    pandas==2.2.2 \
    numpy==1.26.4 \
    pyarrow==16.1.0 \
    fastparquet==2024.5.0 \
    matplotlib==3.8.4 \
    openpyxl==3.1.2 \
    unidecode==1.3.8