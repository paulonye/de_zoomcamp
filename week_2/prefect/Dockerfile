FROM prefecthq/prefect:2.7.7-python3.9

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY etl_gcs_bq.py /opt/prefect/flows/

WORKDIR /opt/prefect/flows/