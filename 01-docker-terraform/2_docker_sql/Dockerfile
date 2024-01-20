FROM python:3.9.1

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY ingest_data.py ingest_data.python
COPY hw/ingest_hw_data.py ingest_hw_data.py

ENTRYPOINT [ "python", "ingest_data.py" ]