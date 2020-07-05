FROM python:3.7

COPY requirements.txt .
RUN pip install -r requirements.txt

RUN  apt-get update -y && apt-get install postgresql -y

RUN mkdir classes
RUN mkdir config
RUN mkdir sql
RUN mkdir staging
run mkdir temp_data

COPY classes/ classes/
COPY config/ config/
COPY sql/ sql/
COPY seed_db.py .
COPY run_config.py .


