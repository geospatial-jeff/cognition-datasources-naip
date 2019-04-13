FROM geospatialjeff/cognition-datasources-db:latest

COPY requirements*.txt ./

# Paths to things
ENV \
    PROD_LIBS=/build/prod \
    DEV_LIBS=/build/dev \
    AWS_DEFAULT_REGION=us-east-1 \
    LAMBDA_DB_PATH=/home/cognition-datasources/spatial-db/lambda_db/database.fs

# Add libraries to python path
ENV \
    PYTHONPATH=$PYTHONPATH:/$PROD_LIBS/lib/python3.6/site-packages:/$DEV_LIBS/lib/python3.6/site-packages

# Install requirements into seperate folders
RUN \
    mkdir $PROD_LIBS; \
    mkdir $DEV_LIBS; \
    pip install -r requirements.txt --install-option="--prefix=$PROD_LIBS" --ignore-installed; \
    pip install -r requirements-dev.txt --install-option="--prefix=$DEV_LIBS" --ignore-installed

COPY bin/* /usr/local/bin/